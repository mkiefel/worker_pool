#include "tags.h"

#include <0mq/message.h>
#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/poller.h>

#include <chrono>
#include <cstddef>
#include <list>
#include <memory>
#include <unordered_map>
#include <iostream>

typedef std::chrono::time_point<std::chrono::steady_clock> timepoint_type;

typedef zmq::Message identity_type;
typedef zmq::Message jobid_type;

class IdentityHash {
public:
    size_t operator() (const identity_type& identity) const {
      std::size_t h = 0;
      const char* d = identity.data();
      for (std::size_t i = 0; i < identity.size(); ++i) {
        h = (h << 1) ^ (std::hash<char>()(d[i]));
      }

      return h;
    }
};

class Worker {
  public:
    Worker(const identity_type& identity)
      : identity_(identity)
    {
    }

    void update(const timepoint_type& timePoint) {
      latestHeartBeat_ = timePoint +
        std::chrono::milliseconds(heartbeatInterval_);
    }

    bool isVanished(const timepoint_type& timePoint) const {
      return timePoint > latestHeartBeat_;
    }

    const identity_type& getIdentity() const {
      return identity_;
    }

  private:
    const std::size_t heartbeatInterval_ = 1000 * 3;

    const identity_type identity_;
    timepoint_type latestHeartBeat_;
};

class QueueApplication {
  private:
    typedef std::unordered_map<identity_type, Worker, IdentityHash> workermap_type;

  public:
    QueueApplication()
      : context_(), frontendSocket_(), backendSocket_()
    {
    }

    void init() {
      connect();
    }

    void go() {
      //  Send out heartbeats at regular intervals
      timepoint_type nextHeartBeat;
      nextHeartBeat = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(heartbeatInterval_);

      while (true) {
        std::vector<zmq::Socket> items = { backendSocket_, frontendSocket_ };

        std::vector<short> state = zmq::poll(items, heartbeatInterval_);

        // handle backend
        if (state[0] & ZMQ_POLLIN) {
          handleBackend();
        }

        // handle frontend
        if (state[1] & ZMQ_POLLIN) {
          handleFrontend();
        }

        // we do not care about vanished idle workers
        checkWorkers(idleWorkers_);

        assignJobs();

        timepoint_type now = std::chrono::steady_clock::now();
        if (now > nextHeartBeat) {
          sendHeartBeat(idleWorkers_);
          sendHeartBeat(busyWorkers_);

          sendJobQueued();

          nextHeartBeat = now +
            std::chrono::milliseconds(heartbeatInterval_);
        }
      }
    }

  private:
    void handleBackend() {
      zmq::Socket::messages_type messages = backendSocket_.receive();
      // check if we got Interrupted
      const std::size_t messageCount = messages.size();

      if (messageCount < 2) {
        throw std::runtime_error("QueueApplication::go: receive invalid "
            "message");
      }

      zmq::Socket::messages_type::iterator messagePtr =
        messages.begin();
      const zmq::Message& identity = *messagePtr++;
      const zmq::Message& tag = *messagePtr++;

      if (tag.size() != 1) {
        throw std::runtime_error("QueueApplication::go: invalid tag size");
      }

      zmq::Socket::messages_type messagesTail;
      messagesTail.splice(messagesTail.begin(), messages, messagePtr,
          messages.end());

      switch (tag.data()[0]) {
        case WORKER_HEARTBEAT_TAG:
          // this worker is idle again; in case it was busy, remove it from
          // the busy queue
          busyWorkers_.erase(identity);

          updateWorkers(idleWorkers_, identity);
          break;
        case WORKER_UPDATE_TAG:
          // worker busy messages are directly forwarded to the clients,
          // they have to take care if a worker dies
          updateWorkers(busyWorkers_, identity);
          frontendSocket_.send(messagesTail);
          break;
        default:
          throw std::runtime_error("QueueApplication::go: invalid tag:");
          break;
      }
    }

    void sendJobQueued() {
      for (const auto& job : cacheQueue_) {
        zmq::Socket::messages_type::const_iterator messagePtr =
          job.begin();
        const zmq::Message& client = *messagePtr++;
        const zmq::Message& jobID = *messagePtr++;

        sendJobTag(client, JOB_QUEUED, jobID);
      }
    }

    void handleFrontend() {
      zmq::Socket::messages_type messages = frontendSocket_.receive();

      // check if we got Interrupted
      if (messages.empty()) {
        throw std::runtime_error("QueueApplication::go: got interrupted");
      }

      if (cacheQueue_.size() < cacheQueueLength_) {
        cacheQueue_.push_back(messages);
      } else {
        // send wait and drop job
        zmq::Socket::messages_type::iterator messagePtr =
          messages.begin();
        const zmq::Message& client = *messagePtr++;
        const zmq::Message& jobID = *messagePtr++;

        sendJobTag(client, JOB_WAIT, jobID);
      }
    }

    void sendJobTag(const identity_type& client, const int tag, const
        jobid_type& jobID) {
      zmq::Socket::messages_type reply;
      reply.push_back(client);

      zmq::Message jobReplyTag(1);
      jobReplyTag.data()[0] = tag;
      reply.push_back(std::move(jobReplyTag));
      reply.push_back(std::move(jobID));

      frontendSocket_.send(reply);
    }

    void assignJobs() {
      auto jobIt = cacheQueue_.begin();
      auto workerIt = idleWorkers_.begin();

      while (workerIt != idleWorkers_.end() && jobIt != cacheQueue_.end()) {
        zmq::Socket::messages_type assigment;
        assigment.push_back(workerIt->second.getIdentity());

        zmq::Message queueTag(1);
        queueTag.data()[0] = QUEUE_JOB_TAG;
        assigment.push_back(std::move(queueTag));

        assigment.splice(assigment.end(), *jobIt);

        backendSocket_.send(assigment);

        // remove worker
        updateWorkers(busyWorkers_, workerIt->first);
        workerIt = idleWorkers_.erase(workerIt);
        jobIt = cacheQueue_.erase(jobIt);
      }
    }

    void sendHeartBeat(const workermap_type& workers) {
      zmq::Message tag(1);
      tag.data()[0] = QUEUE_HEARTBEAT_TAG;

      for (const auto& worker : workers) {
        zmq::Socket::messages_type heartbeat;
        heartbeat.push_back(worker.second.getIdentity());
        heartbeat.push_back(tag);

        backendSocket_.send(heartbeat);
      }
    }

    void checkWorkers(workermap_type& workers) const {
      timepoint_type now = std::chrono::steady_clock::now();

      workermap_type::iterator workerIt = workers.begin();
      while (workerIt != workers.end()) {
        if (workerIt->second.isVanished(now)) {
          std::cout << "delete" << std::endl;
          workerIt = workers.erase(workerIt);
        } else {
          ++workerIt;
        }
      }
    }

    void updateWorkers(workermap_type& workers, const
        identity_type& identity) const {
      workermap_type::iterator it = workers.find(identity);

      if (it == workers.end()) {
        auto insert = workers.emplace(identity, identity);

        it = insert.first;
      }

      timepoint_type now = std::chrono::steady_clock::now();
      it->second.update(now);
    }

    void connect() {
      frontendSocket_ = context_.createSocket(ZMQ_ROUTER);
      backendSocket_ = context_.createSocket(ZMQ_ROUTER);
      frontendSocket_.bind("tcp://*:5555");
      backendSocket_.bind("tcp://*:5556");
    }

    // do not copy
    QueueApplication(const QueueApplication&);

    const std::size_t heartbeatInterval_ = 1000;
    const std::size_t cacheQueueLength_ = 1;

    zmq::Context context_;
    zmq::Socket frontendSocket_;
    zmq::Socket backendSocket_;

    std::list<zmq::Socket::messages_type> cacheQueue_;
    workermap_type idleWorkers_;
    workermap_type busyWorkers_;
};

int main(int /*argc*/, const char** /*argv*/) {
  QueueApplication app;
  app.init();

  app.go();

  return 0;
}