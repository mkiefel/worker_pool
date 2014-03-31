#include "tags.h"
#include "client.h"

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>
#include <0mq/poller.h>

#include <iostream>
#include <thread>
#include <cassert>
#include <cstring>

namespace zmqmap {

Client::Client()
: context_(), clientSocket_() {
}

Client::~Client() {
}

void Client::init() {
  connect();
}

std::vector<zmq::Message> Client::map(const std::vector<zmq::Message>&
    mapData) {
  std::size_t liveness = heartbeatLiveness_;
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
        throw std::runtime_error("Client::map: got interrupted or "
            "got invalid message");
      }

      zmq::Socket::messages_type::iterator messagePtr =
        messages.begin();
      const zmq::Message& tag = *messagePtr++;
      const zmq::Message& jobIDPart = *messagePtr++;
      jobid_type jobID;
      if (jobIDPart.size() != sizeof(jobid_type)) {
        throw std::runtime_error("Client::map: job id has wrong size");
      }
      std::memcpy(&jobID, jobIDPart.data(), sizeof(jobid_type));

      if (tag.size() != 1) {
        throw std::runtime_error("Client::map: invalid tag size");
      }

      if (busyJobs.find(jobID) == busyJobs.end()) {
        std::cout << "Client::map: invalid job id" << std::endl;
      }

      liveness = heartbeatLiveness_;
      switch (tag.data()[0]) {
        case JOB_WAIT:
          // queue is busy; sleep and try again
          handleJobWait(jobID, waitingJobs, busyJobs);

          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          break;
        case JOB_QUEUED:
          // do nothing
          updateJob(jobID, busyJobs);
          break;
        case JOB_BUSY:
          // do nothing
          updateJob(jobID, busyJobs);
          break;
        case JOB_DONE:
          if (messageSize != 3) {
            throw std::runtime_error("Client::map: invalid done message");
          }

          mappedData[jobID] = *messagePtr;
          handleJobDone(jobID, busyJobs);
          --unfinishedJobCount;
          break;
        default:
          throw std::runtime_error("Client::map: invalid job tag");
      }
    }

    if (--liveness == 0) {
      std::cout << "W: heartbeat failure, can't reach queue or worker "
        "died" << std::endl;
      std::cout << "W: reconnecting in " << interval << " msec..." <<
        std::endl;

      std::this_thread::sleep_for(std::chrono::milliseconds(interval));

      if (interval < intervalMax_)
        interval *= 2;

      // get all the busy jobs and push them back to the waitingJobs
      resetWaitingJobs(waitingJobs, busyJobs);

      connect();
      liveness = heartbeatLiveness_;
    }

    checkJobs(waitingJobs, busyJobs);
  }

  return mappedData;
}

void Client::resetWaitingJobs(waitingjobs_type& waitingJobs,
    busyjobs_type& busyJobs) const {
  busyjobs_type::iterator jobIt = busyJobs.begin();
  while (jobIt != busyJobs.end()) {
    waitingJobs.push_back(jobIt->first);
    jobIt = busyJobs.erase(jobIt);
  }
}

void Client::checkJobs(waitingjobs_type& waitingJobs, busyjobs_type&
    busyJobs) const {
  timepoint_type now = std::chrono::steady_clock::now();

  busyjobs_type::iterator jobIt = busyJobs.begin();
  while (jobIt != busyJobs.end()) {
    if (jobIt->second < now) {
      std::cout << "Client::checkJobs: delete" << std::endl;
      waitingJobs.push_back(jobIt->first);
      jobIt = busyJobs.erase(jobIt);
    } else {
      ++jobIt;
    }
  }
}

void Client::handleJobDone(const jobid_type jobID, busyjobs_type&
    busyJobs) const {
  busyJobs.erase(jobID);
}

void Client::updateJob(const jobid_type jobID, busyjobs_type& busyJobs)
  const {
  busyJobs[jobID] = std::chrono::steady_clock::now() +
    std::chrono::milliseconds(jobbeatInterval_);
}

void Client::handleJobWait(const jobid_type jobID, waitingjobs_type&
    waitingJobs, busyjobs_type& busyJobs) const {
  waitingJobs.push_back(jobID);
  busyJobs.erase(jobID);
}

void Client::requestJob(const std::vector<zmq::Message>& mapData,
    waitingjobs_type& waitingJobs, busyjobs_type& busyJobs) {
  if (waitingJobs.empty())
    return;

  std::size_t jobID = waitingJobs.front();
  waitingJobs.pop_front();

  zmq::Message job(sizeof(jobid_type));
  std::memcpy(job.data(), &jobID, sizeof(jobid_type));

  zmq::Socket::messages_type request;
  request.push_back(std::move(job));
  request.push_back(mapData[jobID]);

  clientSocket_.send(request);

  assert(busyJobs.find(jobID) == busyJobs.end());
  updateJob(jobID, busyJobs);
}

void Client::connect() {
  clientSocket_ = context_.createSocket(ZMQ_DEALER);
  clientSocket_.connect("tcp://localhost:5555");
}

}
