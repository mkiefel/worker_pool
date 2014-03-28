#ifndef _ZMQ_WORKER_A787FDBAC388_
#define _ZMQ_WORKER_A787FDBAC388_

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>

#include <functional>
#include <thread>

class Job {
  public:
    typedef std::function<zmq::Message (const zmq::Message&)> jobfunction_type;

    Job(const jobfunction_type& jobFunction, zmq::Context& context);
    ~Job();

    void init(const std::string& bindStr);

  private:
    void go(const std::string& bindStr);

    // do not copy
    Job(const Job&);

    zmq::Context context_;
    const jobfunction_type jobFunction_;
    bool doWork_;
    std::unique_ptr<std::thread> thread_;
};

class ZmqWorker {
  public:
    typedef std::function<zmq::Message (const zmq::Message&)> jobfunction_type;

    ZmqWorker(const jobfunction_type& jobFunction);

    ~ZmqWorker();

    void init();
    void go();

  private:
    bool handleQueue();
    void sendHeartBeat();
    void handleNewJob(zmq::Socket::messages_type::const_iterator messagePtr);
    void handleJobDone();

    // do not copy
    ZmqWorker(const ZmqWorker&);

    void connect();

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

#endif /* _ZMQ_WORKER_A787FDBAC388_ */
