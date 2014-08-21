#include "tags.h"
#include "clock.h"

#include <0mq/message.h>
#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/poller.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <chrono>
#include <cstddef>
#include <list>
#include <memory>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include <stdexcept>
#include <sstream>
#include <thread>

namespace zmqmap {

typedef std::chrono::time_point<steadyclock_type> timepoint_type;

typedef zmq::Message identity_type;
typedef zmq::Message jobid_type;
typedef zmq::Message call_type;

class IdentityHash {
public:
    size_t operator() (const identity_type& identity) const;
};

size_t IdentityHash::operator() (const identity_type& identity) const {
  std::size_t h = 0;
  const char* d = identity.data();
  for (std::size_t i = 0; i < identity.size(); ++i) {
    h = (h << 1) ^ (std::hash<char>()(d[i]));
  }

  return h;
}

class Worker {
  public:
    Worker(const identity_type& identity)
      : heartbeatInterval_(1000 * 3), identity_(identity)
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
    const std::size_t heartbeatInterval_;

    const identity_type identity_;
    timepoint_type latestHeartBeat_;
};

class Signal {
  public:
    Signal()
      : doWork_(true), thread_()
    {
    }

    ~Signal() {
      doWork_ = false;

      thread_->join();
    }

    void init(const std::string& bindStr, zmq::Context& context);

  private:
    void go(const std::string& bindStr, zmq::Context& context);

    // do not copy
    Signal(const Signal&);

    bool doWork_;
    std::unique_ptr<std::thread> thread_;
};

void Signal::init(const std::string& bindStr, zmq::Context& context) {
  // create a thread that waits on the job socket for work
  thread_ = std::unique_ptr<std::thread>(new
      std::thread(std::bind(&Signal::go, this, bindStr, context)));
}

void Signal::go(const std::string& bindStr, zmq::Context& context) {
  zmq::Socket signalSocket = context.createSocket(ZMQ_PAIR);
  signalSocket.connect(bindStr.c_str());

  zmq::Socket::messages_type signal {zmq::Message()};
  while (doWork_ && std::cin.good()) {
    std::cin.get();

    signalSocket.send(signal);
  }
}

class BrokerApplication {
  private:
    typedef std::unordered_map<identity_type, Worker, IdentityHash> workermap_type;

  public:
    BrokerApplication(const std::size_t cacheQueueLength)
    : heartbeatInterval_(1000), cacheQueueLength_(cacheQueueLength),
      context_(), frontendSocket_(), backendSocket_(),
      cacheQueue_(), idleWorkers_(), busyWorkers_()
    {
    }

    void init(const std::string& frontend, const std::string& backend);

    void go();

  private:
    void printStats() const {
      std::cout << "idle: " << std::setw(3) << idleWorkers_.size()
        << ", busy: " << std::setw(3) << busyWorkers_.size() << ", cache: "
        << std::setw(3) << cacheQueue_.size() << std::endl;
    }

    void handleBackend();

    void sendJobQueued();

    void handleFrontend();

    void sendJobTag(const identity_type& client, const int tag, const
        jobid_type& jobID, const call_type& call);

    void assignJobs();

    void sendKill(const workermap_type& workers) {
      broadcastTag(workers, QUEUE_KILL_TAG);
    }

    void sendHeartBeat(const workermap_type& workers) {
      broadcastTag(workers, QUEUE_HEARTBEAT_TAG);
    }

    void checkWorkers(workermap_type& workers) const;

    void broadcastTag(const workermap_type& workers, const int tag);

    void updateWorkers(workermap_type& workers, const identity_type& identity)
      const;

    void connect(const std::string& frontend, const std::string& backend);

    // do not copy
    BrokerApplication(const BrokerApplication&);

    const std::size_t heartbeatInterval_;
    const std::size_t cacheQueueLength_;

    zmq::Context context_;
    zmq::Socket frontendSocket_;
    zmq::Socket backendSocket_;
    zmq::Socket signalSocket_;

    std::list<zmq::Socket::messages_type> cacheQueue_;
    workermap_type idleWorkers_;
    workermap_type busyWorkers_;

    Signal signal_;
};

void BrokerApplication::checkWorkers(workermap_type& workers) const {
  timepoint_type now = steadyclock_type::now();

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

void BrokerApplication::broadcastTag(const workermap_type& workers, const int
    tag) {
  zmq::Message tagMsg(1);
  tagMsg.data()[0] = tag;

  for (const auto& worker : workers) {
    zmq::Socket::messages_type msgs;
    msgs.push_back(worker.second.getIdentity());
    msgs.push_back(tagMsg);

    backendSocket_.send(msgs);
  }
}

void BrokerApplication::updateWorkers(workermap_type& workers, const
    identity_type& identity) const {
  workermap_type::iterator it = workers.find(identity);

  if (it == workers.end()) {
    it = workers.insert(std::make_pair(identity, Worker(identity))).first;
  }

  timepoint_type now = steadyclock_type::now();
  it->second.update(now);
}

void BrokerApplication::connect(const std::string& frontend, const std::string&
    backend) {
  frontendSocket_ = context_.createSocket(ZMQ_ROUTER);
  backendSocket_ = context_.createSocket(ZMQ_ROUTER);
  frontendSocket_.bind(frontend);
  backendSocket_.bind(backend);
}

void BrokerApplication::sendJobQueued() {
  for (const auto& job : cacheQueue_) {
    zmq::Socket::messages_type::const_iterator messagePtr =
      job.begin();
    const zmq::Message& client = *messagePtr++;
    const zmq::Message& jobID = *messagePtr++;
    const zmq::Message& call = *messagePtr++;

    sendJobTag(client, JOB_QUEUED, jobID, call);
  }
}

void BrokerApplication::handleFrontend() {
  zmq::Socket::messages_type messages = frontendSocket_.receive();

  // check if we got Interrupted
  if (messages.empty()) {
    throw std::runtime_error("BrokerApplication::go: got interrupted");
  }

  if (cacheQueue_.size() < cacheQueueLength_) {
    cacheQueue_.push_back(messages);
  } else {
    // send wait and drop job
    zmq::Socket::messages_type::iterator messagePtr =
      messages.begin();
    const zmq::Message& client = *messagePtr++;
    const zmq::Message& jobID = *messagePtr++;
    const zmq::Message& call = *messagePtr++;

    sendJobTag(client, JOB_WAIT, jobID, call);
  }
}

void BrokerApplication::sendJobTag(const identity_type& client, const int tag,
    const jobid_type& jobID, const call_type& call) {
  zmq::Socket::messages_type reply;
  reply.push_back(client);

  zmq::Message jobReplyTag(1);
  jobReplyTag.data()[0] = tag;
  reply.push_back(std::move(jobReplyTag));
  reply.push_back(jobID);
  reply.push_back(call);

  frontendSocket_.send(reply);
}

void BrokerApplication::assignJobs() {
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

void BrokerApplication::handleBackend() {
  zmq::Socket::messages_type messages = backendSocket_.receive();
  // check if we got Interrupted
  const std::size_t messageCount = messages.size();

  if (messageCount < 2) {
    throw std::runtime_error("BrokerApplication::go: receive invalid "
        "message");
  }

  zmq::Socket::messages_type::iterator messagePtr =
    messages.begin();
  const zmq::Message& identity = *messagePtr++;
  const zmq::Message& tag = *messagePtr++;

  if (tag.size() != 1) {
    throw std::runtime_error("BrokerApplication::go: invalid tag size");
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
      throw std::runtime_error("BrokerApplication::go: invalid tag:");
      break;
  }
}

void BrokerApplication::go() {
  //  Send out heartbeats at regular intervals
  timepoint_type nextHeartBeat;
  nextHeartBeat = steadyclock_type::now() +
    std::chrono::milliseconds(heartbeatInterval_);

  while (true) {
    std::vector<zmq::Socket> items = { backendSocket_, frontendSocket_,
      signalSocket_ };

    std::vector<short> state = zmq::poll(items, heartbeatInterval_);

    // handle backend
    if (state[0] & ZMQ_POLLIN) {
      handleBackend();
    }

    // handle frontend
    if (state[1] & ZMQ_POLLIN) {
      handleFrontend();
    }

    if (state[2] & ZMQ_POLLIN) {
      sendKill(idleWorkers_);
      sendKill(busyWorkers_);

      signalSocket_.receive();
      //zmq::Socket::messages_type signal { zmq::Message() };
      //signalSocket_.send(signal);
    }

    checkWorkers(idleWorkers_);
    checkWorkers(busyWorkers_);

    assignJobs();

    timepoint_type now = steadyclock_type::now();
    if (now > nextHeartBeat) {
      sendHeartBeat(idleWorkers_);
      sendHeartBeat(busyWorkers_);

      sendJobQueued();

      nextHeartBeat = now +
        std::chrono::milliseconds(heartbeatInterval_);

      printStats();
    }
  }
}

void BrokerApplication::init(const std::string& frontend, const std::string&
    backend) {
  boost::uuids::uuid uuid;

  std::ostringstream bindStr;
  bindStr << "inproc://signal_" << uuid;

  signalSocket_ = context_.createSocket(ZMQ_PAIR);
  signalSocket_.bind(bindStr.str().c_str());
  signal_.init(bindStr.str(), context_);

  connect(frontend, backend);
}

}

int main(int argc, const char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " <queue-length> <frontend> <backend>" << std::endl
      << "e.g. " << argv[0] << " 100 'tcp://*:5555' 'tcp://*:5556'" << std::endl;

    return 1;
  }

  std::size_t queueLength;
  std::istringstream(argv[1]) >> queueLength;

  zmqmap::BrokerApplication app(queueLength);
  app.init(argv[2], argv[3]);

  app.go();

  return 0;
}
