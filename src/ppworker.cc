#include <0mq/context.h>
#include <0mq/socket.h>
#include <0mq/message.h>
#include <0mq/poller.h>

#include <iostream>
#include <cstddef>
#include <chrono>
#include <thread>

class Worker {
  public:
    Worker()
    : context_(), workerSocket_() {
    }

    ~Worker() {
    }

    void init() {
      connect();
    }

    void go() {
      //  If liveness hits zero, queue is considered disconnected
      std::size_t liveness = heartbeatLiveness_;
      std::size_t interval = intervalInit_;

      //  Send out heartbeats at regular intervals
      std::chrono::time_point<std::chrono::steady_clock> nextHeartBeat;
      nextHeartBeat = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(heartbeatInterval_);

      while (true) {
        std::vector<zmq::Socket> items = { workerSocket_ };

        std::vector<short> state = zmq::poll(items, heartbeatInterval_);

        if (state[0] & ZMQ_POLLIN) {
          //  Get message
          //  - 3-part envelope + content -> request
          //  - 1-part HEARTBEAT -> heartbeat
          zmq::Socket::messages_type messages = workerSocket_.receive();
          if (messages.empty())
            break;          //  Interrupted

          //  .split simulating problems
          //  To test the robustness of the queue implementation we 
          //  simulate various typical problems, such as the worker
          //  crashing or running very slowly. We do this after a few
          //  cycles so that the architecture can get up and running
          //  first:
          if (messages.size() == 3) {
            printf ("I: normal reply\n");
            workerSocket_.send(messages);

            liveness = heartbeatLiveness_;
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // XXX fix this with something else
            /*if (zctx_interrupted)
              break;*/
          }
          else {
            //  .split handle heartbeats
            //  When we get a heartbeat message from the queue, it means the
            //  queue was (recently) alive, so we must reset our liveness
            //  indicator:
            if (messages.size() == 1) {
              zmq::Message& frame = messages[0];
              // TODO check if the message has at least one byte
              if (frame.data()[0] == QUEUE_HEARTBEAT_TAG)
                liveness = heartbeatLiveness_;
              else {
                std::cout << "E: invalid message" << std::endl;
                //zmsg_dump(msg);
              }
            }
            else {
              std::cout << "E: invalid message" << std::endl;
              //zmsg_dump (msg);
            }

            interval = intervalInit_;
          }
        }
        else
        //  .split detecting a dead queue
        //  If the queue hasn't sent us heartbeats in a while, destroy the
        //  socket and reconnect. This is the simplest most brutal way of
        //  discarding any messages we might have sent in the meantime:
        if (--liveness == 0) {
          std::cout << "W: heartbeat failure, can't reach queue" << std::endl;
          std::cout << "W: reconnecting in " << interval << " msec..." <<
            std::endl;

          std::this_thread::sleep_for(std::chrono::milliseconds(interval));

          if (interval < intervalMax_)
            interval *= 2;

          // TODO kill worker if there is no successful connect
          connect();
          liveness = heartbeatLiveness_;
        }

        //  Send heartbeat to queue if it's time
        std::chrono::time_point<std::chrono::steady_clock> now =
          std::chrono::steady_clock::now();
        if (now > nextHeartBeat) {
          nextHeartBeat = now + std::chrono::milliseconds(heartbeatInterval_);

          std::cout << "I: worker heartbeat" << std::endl;
          zmq::Message message(1);
          message.data()[0] = WORKER_HEARTBEAT_TAG;
          zmq::Socket::messages_type messages = {message};
          workerSocket_.send(messages);
        }
      }
    }

  private:
    // do not copy
    Worker(const Worker&);

    void connect() {
      workerSocket_ = context_.createSocket(ZMQ_DEALER);
      workerSocket_.connect("tcp://localhost:5556");

      //  Tell queue we're ready for work
      std::cout << "I: worker ready" << std::endl;
      zmq::Message message(1);
      message.data()[0] = WORKER_READY_TAG;
      zmq::Socket::messages_type messages = {message};
      workerSocket_.send(messages);
    }

    void keepAlive() const {
    }

    const std::size_t heartbeatLiveness_ = 3;
    const std::size_t heartbeatInterval_ = 1000;
    const std::size_t intervalInit_ = 1000;
    const std::size_t intervalMax_ = 32000;

    enum {
      WORKER_READY_TAG = 1,
      WORKER_HEARTBEAT_TAG = 2
    };

    enum {
      QUEUE_HEARTBEAT_TAG = 2
    };

    enum Tag {
      KILL_TAG = 0,
      WORK_TAG = 1,
    };

    zmq::Context context_;
    zmq::Socket workerSocket_;
};


int main (void)
{
  Worker worker;
  worker.init();

  worker.go();

    //void *worker = s_worker_socket (ctx);

    ////  If liveness hits zero, queue is considered disconnected
    //size_t liveness = HEARTBEAT_LIVENESS;
    //size_t interval = INTERVAL_INIT;

    ////  Send out heartbeats at regular intervals
    //uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;

    //srandom ((unsigned) time (NULL));
    //int cycles = 0;
    //while (1) {
        //zmq_pollitem_t items [] = { { worker,  0, ZMQ_POLLIN, 0 } };
        //int rc = zmq_poll (items, 1, HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
        //if (rc == -1)
            //break;              //  Interrupted

        //if (items [0].revents & ZMQ_POLLIN) {
            ////  Get message
            ////  - 3-part envelope + content -> request
            ////  - 1-part HEARTBEAT -> heartbeat
            //zmsg_t *msg = zmsg_recv (worker);
            //if (!msg)
                //break;          //  Interrupted

            ////  .split simulating problems
            ////  To test the robustness of the queue implementation we 
            ////  simulate various typical problems, such as the worker
            ////  crashing or running very slowly. We do this after a few
            ////  cycles so that the architecture can get up and running
            ////  first:
            //if (zmsg_size (msg) == 3) {
                //cycles++;
                //if (cycles > 3 && randof (5) == 0) {
                    //printf ("I: simulating a crash\n");
                    //zmsg_destroy (&msg);
                    //break;
                //}
                //else
                //if (cycles > 3 && randof (5) == 0) {
                    //printf ("I: simulating CPU overload\n");
                    //sleep (3);
                    //if (zctx_interrupted)
                        //break;
                //}
                //printf ("I: normal reply\n");
                //zmsg_send (&msg, worker);
                //liveness = HEARTBEAT_LIVENESS;
                //sleep (1);              //  Do some heavy work
                //if (zctx_interrupted)
                    //break;
            //}
            //else
            ////  .split handle heartbeats
            ////  When we get a heartbeat message from the queue, it means the
            ////  queue was (recently) alive, so we must reset our liveness
            ////  indicator:
            //if (zmsg_size (msg) == 1) {
                //zframe_t *frame = zmsg_first (msg);
                //if (memcmp (zframe_data (frame), PPP_HEARTBEAT, 1) == 0)
                    //liveness = HEARTBEAT_LIVENESS;
                //else {
                    //printf ("E: invalid message\n");
                    //zmsg_dump (msg);
                //}
                //zmsg_destroy (&msg);
            //}
            //else {
                //printf ("E: invalid message\n");
                //zmsg_dump (msg);
            //}
            //interval = INTERVAL_INIT;
        //}
        //else
        ////  .split detecting a dead queue
        ////  If the queue hasn't sent us heartbeats in a while, destroy the
        ////  socket and reconnect. This is the simplest most brutal way of
        ////  discarding any messages we might have sent in the meantime:
        //if (--liveness == 0) {
            //printf ("W: heartbeat failure, can't reach queue\n");
            //printf ("W: reconnecting in %zd msec...\n", interval);
            //zclock_sleep (interval);

            //if (interval < INTERVAL_MAX)
                //interval *= 2;
            //zsocket_destroy (ctx, worker);
            //worker = s_worker_socket (ctx);
            //liveness = HEARTBEAT_LIVENESS;
        //}
        ////  Send heartbeat to queue if it's time
        //if (zclock_time () > heartbeat_at) {
            //heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
            //printf ("I: worker heartbeat\n");
            //zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
            //zframe_send (&frame, worker, 0);
        //}
    //}
    //zctx_destroy (&ctx);
    //return 0;
}
