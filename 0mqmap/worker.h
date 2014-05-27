#ifndef _WORKER_A787FDBAC388_
#define _WORKER_A787FDBAC388_

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>

#include <functional>
#include <thread>

namespace zmqmap {

class Job {
  public:
    typedef std::function<zmq::Message (const zmq::Message&)> jobfunction_type;

    Job();
    ~Job();

    void init(const std::string& bindStr, const jobfunction_type& jobFunction,
        zmq::Context& context);

  private:
    void go(const std::string& bindStr, const jobfunction_type& jobFunction,
        zmq::Context& context);

    // do not copy
    Job(const Job&);

    bool doWork_;
    std::unique_ptr<std::thread> thread_;
};

class Worker {
  public:
    typedef std::function<zmq::Message (const zmq::Message&)> jobfunction_type;

    Worker(const std::string& brokerAddress);

    ~Worker();

    void init(const jobfunction_type& jobFunction);
    void go();

  private:
    bool handleQueue();
    void sendHeartBeat();
    void handleNewJob(zmq::Socket::messages_type::const_iterator messagePtr);
    void handleJobDone();

    // do not copy
    Worker(const Worker&);

    void connect();

    const std::size_t heartbeatInterval_;
    const std::size_t queuebeatInterval_;
    const std::size_t intervalInit_;
    const std::size_t intervalMax_;

    zmq::Context context_;
    zmq::Socket workerSocket_, jobSocket_;
    Job job_;

    bool isBusy_;
    zmq::Message client_;
    zmq::Message jobID_;
    zmq::Message call_;

    std::string brokerAddress_;
};

}

#endif /* _WORKER_A787FDBAC388_ */
