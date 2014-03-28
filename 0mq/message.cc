#include <0mq/message.h>

#include <0mq/error.h>

#include <iostream>

namespace zmq {

Message::~Message() {
  const int result = zmq_msg_close(&message_);

  if (result != 0)
    throw Error();
}

Message::Message() {
  init();
}


void Message::init() {
  const int result = zmq_msg_init(&message_);

  if (result != 0)
    throw Error();
}

Message::Message(std::size_t size) {
  const int result = zmq_msg_init_size(&message_, size);

  if (result != 0)
    throw Error();
}

Message::Message(Message&& message) {
  init();

  const int result = zmq_msg_move(&message_, &message.message_);

  if (result != 0)
    throw Error();
}

Message::Message(const Message& message) {
  init();

  *this = message;
}

Message& Message::operator= (const Message& message) {
  const int result = zmq_msg_copy(&message_,
      const_cast<zmq_msg_t*>(&message.message_));
  if (result != 0)
    throw Error();

  return *this;
}

bool Message::operator== (const Message& message) const {
  bool result = this->size() == message.size();

  const char* d = this->data();
  const char* o = message.data();
  for (std::size_t i = 0; result && i < this->size(); ++i) {
    result = result && (d[i] == o[i]);
  }

  return result;
}

zmq_msg_t& Message::getMessage() {
  return message_;
}

std::size_t Message::size() const {
  return zmq_msg_size(const_cast<zmq_msg_t*>(&message_));
}

char* Message::data() {
  return reinterpret_cast<char*>(zmq_msg_data(&message_));
}

const char* Message::data() const {
  return reinterpret_cast<const char*>(zmq_msg_data(&message_));
}

}
