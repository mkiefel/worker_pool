#include "tags.h"

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>
#include <0mq/poller.h>

#include <iostream>
#include <cstddef>
#include <chrono>
#include <thread>
#include <list>
#include <unordered_map>
#include <cassert>

typedef std::size_t jobid_type;
typedef std::chrono::time_point<std::chrono::steady_clock> timepoint_type;
typedef std::list<std::size_t> waitingjobs_type;
typedef std::unordered_map<std::size_t, timepoint_type> busyjobs_type;

class ClientApplication {
  public:
    ClientApplication()
    : context_(), clientSocket_() {
    }

    ~ClientApplication() {
    }

    void init() {
      connect();
    }

    std::vector<zmq::Message> map(const std::vector<zmq::Message>& mapData) {
      std::size_t interval = intervalInit_;

      waitingjobs_type waitingJobs;
      busyjobs_type busyJobs;

      std::size_t unfinishedJobCount = mapData.size();
      for (std::size_t i = 0; i < unfinishedJobCount; ++i) {
        waitingJobs.push_back(i);
      }

      std::vector<zmq::Message> mappedData(unfinishedJobCount);

      while (unfinishedJobCount > 0) {
        requestJob(mapData, waitingJobs, busyJobs);

        std::vector<zmq::Socket> items = { clientSocket_ };

        std::vector<short> state = zmq::poll(items, heartbeatInterval_);

        if (state[0] & ZMQ_POLLIN) {
          zmq::Socket::messages_type messages = clientSocket_.receive();

          const std::size_t messageSize = messages.size();

          // check if we got a valid message
          if (messageSize < 2) {
            throw std::runtime_error("ClientApplication::go: got interrupted or "
                "got invalid message");
          }

          zmq::Socket::messages_type::iterator messagePtr =
            messages.begin();
          const zmq::Message& tag = *messagePtr++;
          jobid_type jobID = (*messagePtr++).data()[0];

          if (tag.size() != 1) {
            throw std::runtime_error("WorkerApplication::go: invalid tag size");
          }

          if (busyJobs.find(jobID) == busyJobs.end()) {
            std::cout << "WorkerApplication::go: invalid job id" << std::endl;
          }

          switch (tag.data()[0]) {
            case JOB_WAIT:
              // queue is busy; sleep and try again
              std::cout << "queue is full" << std::endl;

              handleJobWait(jobID, waitingJobs, busyJobs);

              std::this_thread::sleep_for(std::chrono::milliseconds(100));
              break;
            case JOB_QUEUED:
              // do nothing
              std::cout << "queued" << std::endl;

              updateJob(jobID, busyJobs);
              break;
            case JOB_BUSY:
              // do nothing
              std::cout << "busy" << std::endl;

              updateJob(jobID, busyJobs);
              break;
            case JOB_DONE:
              std::cout << "done" << std::endl;

              if (messageSize != 3) {
                throw std::runtime_error("WorkerApplication::go: invalid done message");
              }

              mappedData[jobID] == *messagePtr;
              handleJobDone(jobID, busyJobs);
              --unfinishedJobCount;
              break;
            default:
              throw std::runtime_error("ClientApplication::go: invalid job tag");
          }
        }

        if (false) {
          std::cout << "W: heartbeat failure, can't reach queue or worker "
            "died" << std::endl;
          std::cout << "W: reconnecting in " << interval << " msec..." <<
            std::endl;

          std::this_thread::sleep_for(std::chrono::milliseconds(interval));

          if (interval < intervalMax_)
            interval *= 2;

          connect();
        }

        checkJobs(waitingJobs, busyJobs);
      }

      return mappedData;
    }

  private:
    void checkJobs(waitingjobs_type& waitingJobs, busyjobs_type& busyJobs) const {
      timepoint_type now = std::chrono::steady_clock::now();

      busyjobs_type::iterator jobIt = busyJobs.begin();
      while (jobIt != busyJobs.end()) {
        if (jobIt->second < now) {
          std::cout << "delete" << std::endl;
          waitingJobs.push_back(jobIt->first);
          jobIt = busyJobs.erase(jobIt);
        } else {
          ++jobIt;
        }
      }
    }

    void handleJobDone(const jobid_type jobID, busyjobs_type& busyJobs) const {
      busyJobs.erase(jobID);
    }

    void updateJob(const jobid_type jobID, busyjobs_type& busyJobs) const {
      busyJobs[jobID] = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(jobbeatInterval_);
    }

    void handleJobWait(const jobid_type jobID, waitingjobs_type&
        waitingJobs, busyjobs_type& busyJobs) const {
      waitingJobs.push_back(jobID);
      busyJobs.erase(jobID);
    }

    void requestJob(const std::vector<zmq::Message>& mapData, waitingjobs_type&
        waitingJobs, busyjobs_type& busyJobs) {
      if (waitingJobs.empty())
        return;

      std::size_t jobID = waitingJobs.front();
      waitingJobs.pop_front();

      zmq::Message job(1);
      job.data()[0] = jobID;

      zmq::Socket::messages_type request;
      request.push_back(std::move(job));
      request.push_back(mapData[jobID]);

      clientSocket_.send(request);

      assert(busyJobs.find(jobID) == busyJobs.end());
      updateJob(jobID, busyJobs);
    }

    // do not copy
    ClientApplication(const ClientApplication&);

    void connect() {
      clientSocket_ = context_.createSocket(ZMQ_DEALER);
      clientSocket_.connect("tcp://localhost:5555");
    }

    const std::size_t heartbeatInterval_ = 1000;
    // the time that maximally may pass between to messages for a job beat
    const std::size_t jobbeatInterval_ = 3000;
    const std::size_t intervalInit_ = 1000;
    const std::size_t intervalMax_ = 32000;

    zmq::Context context_;
    zmq::Socket clientSocket_;
};


int main(int /*argc*/, const char** /*argv*/) {
  ClientApplication client;
  client.init();

  zmq::Message data(1);

  std::vector<zmq::Message> mapData;
  mapData.push_back(data);
  mapData.push_back(data);
  mapData.push_back(data);
  mapData.push_back(data);

  client.map(mapData);
}
