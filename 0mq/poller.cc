#include <0mq/poller.h>

#include <0mq/socket.h>
#include <0mq/error.h>

#include <zmq.h>

namespace zmq {

std::vector<short> poll(std::vector<Socket>& items, long timeout_) {
  std::vector<zmq_pollitem_t> items_(items.size());

  for (std::size_t i = 0; i < items.size(); ++i) {
    items_[i].socket = items[i].getSocket().get();
    items_[i].fd = 0;
    items_[i].events = ZMQ_POLLIN;
    items_[i].revents = 0;
  }

  const int result = zmq_poll(items_.data(), items.size(), timeout_);
  if (result == -1)
    throw Error();

  std::vector<short> state(items_.size());

  for (std::size_t i = 0; i < items.size(); ++i) {
    state[i] = items_[i].revents;
  }

  return state;
}

}
