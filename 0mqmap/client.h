#ifndef _CLIENT_43DACED51E97_
#define _CLIENT_43DACED51E97_

#include "clock.h"

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>

#include <list>
#include <vector>
#include <cstddef>
#include <unordered_map>
#include <chrono>
#include <string>

namespace zmqmap {

class Client {
  public:
    Client(const std::string& brokerAddress);
    ~Client();

    void init();

    std::vector<zmq::Message> map(const std::vector<zmq::Message>& mapData);

  private:
    typedef std::chrono::time_point<steadyclock_type> timepoint_type;
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
    Client(const Client&);

    void connect();

    // number of times we may reach the heartbeatInterval_ timeout without a
    // message from the queue
    const std::size_t heartbeatLiveness_;
    // maximal time we wait for messages from the queue
    const std::size_t heartbeatInterval_;
    // the time that maximally may pass between to messages for a job beat
    const std::size_t jobbeatInterval_;
    const std::size_t intervalInit_;
    const std::size_t intervalMax_;

    std::string brokerAddress_;

    zmq::Context context_;
    zmq::Socket clientSocket_;

    std::size_t call_;
};

}

#endif /* _CLIENT_43DACED51E97_ */
