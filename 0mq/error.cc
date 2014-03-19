#include <0mq/error.h>

#include <zmq.h>

namespace zmq {

Error::Error()
  : error_(zmq_errno())
{
}

const char* Error::what() const throw() {
  return zmq_strerror(error_);
}

}
