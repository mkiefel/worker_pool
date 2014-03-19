#include <0mq/context.h>

namespace zmq {

Context::Context()
  : context_(zmq_ctx_new(), [] (void* ptr) {
        zmq_ctx_destroy(ptr);
      })
{
}

Socket Context::createSocket(int type) {
  return Socket(context_, type);
}

}
