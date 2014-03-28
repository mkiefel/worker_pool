#include "tags.h"

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>
#include <0mq/poller.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <iostream>
#include <cstddef>
#include <chrono>
#include <thread>
#include <functional>
#include <string>
#include <sstream>
#include <cassert>

typedef std::function<zmq::Message (const zmq::Message&)> jobfunction_type;
typedef std::chrono::time_point<std::chrono::steady_clock> timepoint_type;

class Job {
  public:
    Job(const jobfunction_type& jobFunction, zmq::Context& context)
      : context_(context), jobFunction_(jobFunction), doWork_(true)
    {
    }

    ~Job() {
      doWork_ = false;

      thread_->join();
    }

    void init(const std::string& bindStr) {
      using namespace std::placeholders;
      // create a thread that waits on the job socket for work
      thread_ = std::unique_ptr<std::thread>(new
          std::thread(std::bind(&Job::go, this, bindStr)));
    }

  private:
    void go(const std::string& bindStr) {
      zmq::Socket jobSocket = context_.createSocket(ZMQ_PAIR);
      jobSocket.connect(bindStr.c_str());

      while (doWork_) {
        zmq::Socket::messages_type request = jobSocket.receive();

        if (request.size() != 1) {
          throw std::runtime_error("Job::go: empty data");
        }

        zmq::Message replyMessage = jobFunction_(request.front());

        zmq::Socket::messages_type reply {replyMessage};
        jobSocket.send(reply);
      }
    }

    // do not copy
    Job(const Job&);

    zmq::Context context_;
    const jobfunction_type jobFunction_;
    bool doWork_;
    std::unique_ptr<std::thread> thread_;
};

class WorkerApplication {
  public:
    WorkerApplication(const jobfunction_type& jobFunction)
    : context_(), workerSocket_(), jobSocket_(), job_(jobFunction, context_),
      isBusy_(false), client_(), jobID_()
    {
    }

    ~WorkerApplication() {
    }

    void init() {
      boost::uuids::uuid uuid;

      std::ostringstream bindStr;
      bindStr << "inproc://job_" << uuid;

      jobSocket_ = context_.createSocket(ZMQ_PAIR);
      jobSocket_.bind(bindStr.str().c_str());
      job_.init(bindStr.str());

      connect();
    }

    void go() {
      timepoint_type nextQueueBeat;
      nextQueueBeat = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(queuebeatInterval_);
      std::size_t interval = intervalInit_;

      timepoint_type nextHeartBeat;
      nextHeartBeat = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(heartbeatInterval_);

      while (true) {
        std::vector<zmq::Socket> items = { workerSocket_ };
        if (isBusy_) {
          items.push_back(jobSocket_);
        }

        std::vector<short> state = zmq::poll(items, heartbeatInterval_);

        assert(!isBusy_ || state.size() > 1);

        if (state[0] & ZMQ_POLLIN) {
          handleQueue();

          nextQueueBeat = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(queuebeatInterval_);
        }

        if (isBusy_ && state.size() > 1 && state[1] & ZMQ_POLLIN) {
          handleJobDone();

          // make sure we let the queue know that we are available again
          nextHeartBeat = std::chrono::steady_clock::now() - 2 *
            std::chrono::milliseconds(heartbeatInterval_);
        }

        timepoint_type now =
          std::chrono::steady_clock::now();

        if (now > nextQueueBeat) {
          std::cout << "W: heartbeat failure, can't reach queue" << std::endl;
          std::cout << "W: reconnecting in " << interval << " msec..." <<
            std::endl;

          std::this_thread::sleep_for(std::chrono::milliseconds(interval));

          if (interval < intervalMax_)
            interval *= 2;

          connect();

          nextQueueBeat = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(queuebeatInterval_);
        }

        //  Send heartbeat to queue if it's time
        if (now > nextHeartBeat) {
          sendHeartBeat();

          nextHeartBeat = now + std::chrono::milliseconds(heartbeatInterval_);
        }
      }
    }

  private:
    void handleQueue() {
      zmq::Socket::messages_type messages = workerSocket_.receive();

      const std::size_t messageSize = messages.size();

      // check if we got a valid message
      if (messageSize == 0) {
        throw std::runtime_error("WorkerApplication::go: got interrupted");
      }

      zmq::Socket::messages_type::iterator messagePtr =
        messages.begin();
      const zmq::Message& tag = *messagePtr++;

      if (tag.size() != 1) {
        throw std::runtime_error("WorkerApplication::go: invalid tag size");
      }

      switch (tag.data()[0]) {
        case QUEUE_HEARTBEAT_TAG:
          // do nothing
          if (messageSize != 1) {
            throw std::runtime_error("WorkerApplication::go: invalid heartbeak");
          }
          break;
        case QUEUE_JOB_TAG:
          // got a job
          if (messageSize != 4) {
            throw std::runtime_error("WorkerApplication::go: invalid job request");
          }

          if (!isBusy_)
            handleNewJob(messagePtr);
          else
            std::cout << "W: I am busy but I got a job. I'll ignore it" << std::endl;

          break;
      }
    }

    void sendHeartBeat() {
      zmq::Message queueReplyTag(1);
      zmq::Socket::messages_type heartbeat;
      std::cout << "beat: " << isBusy_ << std::endl;
      if (!isBusy_) {
        queueReplyTag.data()[0] = WORKER_HEARTBEAT_TAG;
        heartbeat.push_back(std::move(queueReplyTag));
        workerSocket_.send(heartbeat);
      } else {
        queueReplyTag.data()[0] = WORKER_UPDATE_TAG;
        heartbeat.push_back(std::move(queueReplyTag));

        heartbeat.push_back(client_);

        zmq::Message jobReplyTag(1);
        jobReplyTag.data()[0] = JOB_BUSY;
        heartbeat.push_back(std::move(jobReplyTag));
        heartbeat.push_back(jobID_);
        workerSocket_.send(heartbeat);
      }
    }

    void handleNewJob(zmq::Socket::messages_type::const_iterator messagePtr) {
      // push the address of the client
      std::cout << "got job" << std::endl;

      client_ = *messagePtr++;
      jobID_ = *messagePtr++;
      isBusy_ = true;

      zmq::Socket::messages_type jobRequest;
      jobRequest.push_back(*messagePtr);
      jobSocket_.send(jobRequest);
    }

    void handleJobDone() {
      std::cout << "done" << std::endl;
      zmq::Socket::messages_type jobReply = jobSocket_.receive();

      zmq::Socket::messages_type reply;
      zmq::Message queueReplyTag(1);
      zmq::Message jobReplyTag(1);

      queueReplyTag.data()[0] = WORKER_UPDATE_TAG;
      reply.push_back(std::move(queueReplyTag));

      // push the address of the client
      reply.push_back(std::move(client_));

      jobReplyTag.data()[0] = JOB_DONE;
      reply.push_back(std::move(jobReplyTag));
      reply.push_back(std::move(jobID_));

      reply.splice(reply.end(), jobReply);

      workerSocket_.send(reply);

      isBusy_ = false;

    }

    // do not copy
    WorkerApplication(const WorkerApplication&);

    void connect() {
      workerSocket_ = context_.createSocket(ZMQ_DEALER);
      workerSocket_.connect("tcp://localhost:5556");

      //  Tell queue we're ready for work
      zmq::Message message(1);
      message.data()[0] = WORKER_HEARTBEAT_TAG;
      zmq::Socket::messages_type messages = {message};
      workerSocket_.send(messages);
    }

    const std::size_t heartbeatInterval_ = 1000;
    const std::size_t queuebeatInterval_ = 3000;
    const std::size_t intervalInit_ = 1000;
    const std::size_t intervalMax_ = 32000;

    zmq::Context context_;
    zmq::Socket workerSocket_, jobSocket_;
    Job job_;

    bool isBusy_;
    zmq::Message client_;
    zmq::Message jobID_;
};

zmq::Message job(const zmq::Message& data) {
  std::this_thread::sleep_for(std::chrono::seconds(10));
  return data;
}

int main(int /*argc*/, const char** /*argv*/) {
  WorkerApplication worker(job);
  worker.init();

  worker.go();

  return 0;
}
