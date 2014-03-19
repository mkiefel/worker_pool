#ifndef _CONTEXT_F39C387DD7A2_
#define _CONTEXT_F39C387DD7A2_

#include <0mq/socket.h>

#include <zmq.h>

#include <memory>

namespace zmq {

class Context {
  public:
    Context();

    Socket createSocket(int type);

  private:
    std::shared_ptr<void> context_;
};

}

#endif // _CONTEXT_F39C387DD7A2_
