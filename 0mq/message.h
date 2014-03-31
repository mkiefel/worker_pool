#ifndef _MESSAGE_EE2A7791D639_
#define _MESSAGE_EE2A7791D639_

#include <zmq.h>

#include <cstddef>

namespace zmq {

class Socket;

class Message {
  private:
    friend class Socket;

    zmq_msg_t& getMessage();

    void init();

  public:
    Message(std::size_t size);
    Message();
    ~Message();
    Message(Message&& message);
    Message(const Message& message);

    Message& operator= (const Message& message);

    bool operator== (const Message& message) const;

    std::size_t size() const;
    char* data();
    const char* data() const;

  private:
    mutable zmq_msg_t message_;
};

}

#endif /* _MESSAGE_EE2A7791D639_ */
