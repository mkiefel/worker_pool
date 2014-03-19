#ifndef _POLLER_C781C32ABBD0_
#define _POLLER_C781C32ABBD0_

#include <vector>

namespace zmq {

class Socket;

int poll(std::vector<Socket>& items, long timeout_ = -1);

}

#endif /* _POLLER_C781C32ABBD0_ */
