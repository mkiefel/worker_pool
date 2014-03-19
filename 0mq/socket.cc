#include <0mq/socket.h>

#include <0mq/message.h>
#include <0mq/error.h>

#include <zmq.h>

#include <cstddef>

namespace zmq {

Socket::Socket(std::shared_ptr<void> context, int type)
  : socket_(zmq_socket(context.get(), type), [] (void* ptr) {
        zmq_close(ptr);
      })
{
}

Socket::Socket()
  : socket_(nullptr)
{
}

const std::shared_ptr<void>& Socket::getSocket() {
  return socket_;
}

bool Socket::receiveSingle(Message& message, int flags) {
  const int result = zmq_msg_recv(&message.getMessage(),
      socket_.get(), flags);

  if (result >= 0)
    return true;

  if (zmq_errno() == EAGAIN)
    return false;

  throw Error();
}

Socket::messages_type Socket::receive(int flags) {
  int more;
  std::size_t moreSize = sizeof(decltype(more));

  messages_type messages(1);
  if (!receiveSingle(messages.back(), flags)) {
    messages.clear();
    return messages;
  }

  const int result = zmq_getsockopt(socket_.get(), ZMQ_RCVMORE, &more,
      &moreSize);
  if (result != 0)
    throw Error();

  while (more) {
    messages.resize(messages.size() + 1);

    if (!receiveSingle(messages.back(), flags))
      throw std::logic_error("Socket::receive: receive not successful");

    const int result = zmq_getsockopt(socket_.get(), ZMQ_RCVMORE, &more,
        &moreSize);
    if (result != 0)
      throw Error();
  }

  return messages;
}

bool Socket::send(messages_type &messages, int flags) {
  for (std::size_t i = 0; i < messages.size(); ++i) {
    const int nbytes = zmq_msg_send(&messages[i].getMessage(), socket_.get(),
        i+1 < messages.size() ? (flags | ZMQ_SNDMORE) : flags);

    if (!(nbytes >= 0)) {
      if (zmq_errno() == EAGAIN)
        return false;
      else
        throw Error();
    }
  }

  return true;
}

void Socket::bind(const std::string& addr) {
  const int result = zmq_bind(socket_.get(), addr.c_str());
  if (result != 0)
    throw Error();
}

void Socket::unbind(const std::string& addr) {
  const int result = zmq_unbind(socket_.get(), addr.c_str());
  if (result != 0)
    throw Error();
}

void Socket::connect(const std::string& addr) {
  const int result = zmq_connect(socket_.get(), addr.c_str());
  if (result != 0)
    throw Error();
}

void Socket::disconnect(const std::string& addr) {
  const int result = zmq_disconnect(socket_.get(), addr.c_str());
  if (result != 0)
    throw Error();
}

}
