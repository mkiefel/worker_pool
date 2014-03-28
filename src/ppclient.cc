#include "tags.h"

#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>
#include <0mq/poller.h>

#include <iostream>
#include <cstddef>
#include <chrono>
#include <thread>

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
      std::size_t liveness = heartbeatLiveness_;
      std::size_t interval = intervalInit_;

      std::vector<zmq::Message> mappedData;

      std::chrono::time_point<std::chrono::steady_clock> nextHeartBeat;
      nextHeartBeat = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(heartbeatInterval_);

      bool isRunning = true;

      char jobID = 0;

      while (isRunning) {
        zmq::Message job(1);
        job.data()[0] = jobID++;

        zmq::Socket::messages_type request;
        request.push_back(std::move(job));
        request.push_back(zmq::Message());

        clientSocket_.send(request);

        bool isWaiting = true;
        while (isWaiting) {
          std::vector<zmq::Socket> items = { clientSocket_ };

          std::vector<short> state = zmq::poll(items, heartbeatInterval_);

          if (state[0] & ZMQ_POLLIN) {
            zmq::Socket::messages_type messages = clientSocket_.receive();

            // check if we got a valid message
            if (messages.empty()) {
              throw std::runtime_error("ClientApplication::go: got interrupted");
            }

            zmq::Socket::messages_type::iterator messagePtr =
              messages.begin();
            const zmq::Message& tag = *messagePtr++;

            if (tag.size() != 1) {
              throw std::runtime_error("WorkerApplication::go: invalid tag size");
            }

            liveness = heartbeatLiveness_;
            switch (tag.data()[0]) {
              case JOB_WAIT:
                // queue is busy; sleep and try again
                std::cout << "queue is full" << std::endl;
                isWaiting = false;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                break;
              case JOB_QUEUED:
                // do nothing
                std::cout << "queued" << std::endl;
                break;
              case JOB_BUSY:
                // do nothing
                std::cout << "busy" << std::endl;
                break;
              case JOB_DONE:
                // check if we got the right response
                if (messagePtr->size() == 1 && (messagePtr->data()[0]+1) == jobID) {
                  std::cout << "success" << std::endl;
                  isWaiting = false;
                  isRunning = false;
                } else {
                  std::cout << "got wrong job id" << std::endl;
                  isWaiting = false;
                }
                break;
              default:
                throw std::runtime_error("ClientApplication::go: invalid job tag");
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

            isWaiting = false;

            connect();
            liveness = heartbeatLiveness_;
          }
        }
      }

      return mappedData;
    }

  private:
    // do not copy
    ClientApplication(const ClientApplication&);

    void connect() {
      clientSocket_ = context_.createSocket(ZMQ_DEALER);
      clientSocket_.connect("tcp://localhost:5555");
    }

    const std::size_t heartbeatLiveness_ = 3;
    const std::size_t heartbeatInterval_ = 1000;
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
