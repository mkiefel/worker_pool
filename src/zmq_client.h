#ifndef _ZMQ_CLIENT_43DACED51E97_
#define _ZMQ_CLIENT_43DACED51E97_

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>

#include <list>
#include <vector>
#include <cstddef>
#include <unordered_map>
#include <chrono>

class ZmqClient {
  public:
    ZmqClient();
    ~ZmqClient();

    void init();

    std::vector<zmq::Message> map(const std::vector<zmq::Message>& mapData);

  private:
    typedef std::chrono::time_point<std::chrono::steady_clock> timepoint_type;
    typedef std::size_t jobid_type;
    typedef std::list<std::size_t> waitingjobs_type;
    typedef std::unordered_map<std::size_t, timepoint_type> busyjobs_type;

    void resetWaitingJobs(waitingjobs_type& waitingJobs, busyjobs_type&
        busyJobs) const;

    void checkJobs(waitingjobs_type& waitingJobs, busyjobs_type& busyJobs)
      const;

    void handleJobDone(const jobid_type jobID, busyjobs_type& busyJobs) const;

    void updateJob(const jobid_type jobID, busyjobs_type& busyJobs) const;

    void handleJobWait(const jobid_type jobID, waitingjobs_type&
        waitingJobs, busyjobs_type& busyJobs) const;

    void requestJob(const std::vector<zmq::Message>& mapData, waitingjobs_type&
        waitingJobs, busyjobs_type& busyJobs);

    // do not copy
    ZmqClient(const ZmqClient&);

    void connect();

    // number of times we may reach the heartbeatInterval_ timeout without a
    // message from the queue
    const std::size_t heartbeatLiveness_ = 3;
    // maximal time we wait for messages from the queue
    const std::size_t heartbeatInterval_ = 1000;
    // the time that maximally may pass between to messages for a job beat
    const std::size_t jobbeatInterval_ = 3000;
    const std::size_t intervalInit_ = 1000;
    const std::size_t intervalMax_ = 32000;

    zmq::Context context_;
    zmq::Socket clientSocket_;
};


#endif /* _ZMQ_CLIENT_43DACED51E97_ */
