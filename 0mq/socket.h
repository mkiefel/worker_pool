#ifndef _SOCKET_46593F85D31E_
#define _SOCKET_46593F85D31E_

#include <list>
#include <memory>
#include <vector>

namespace zmq {

class Context;
class Message;

class Socket {
  private:
    friend Context;
    friend std::vector<short> poll(std::vector<Socket>& items, long timeout_);

    Socket(std::shared_ptr<void> context, int type);

    bool receiveSingle(Message& message, int flags);

  public:
    typedef std::list<Message> messages_type;

    Socket();

    bool send(messages_type& message, int flags = 0);
    messages_type receive(int flags = 0);

    void bind(const std::string& addr);
    void unbind(const std::string& addr);

    void connect(const std::string& addr);
    void disconnect(const std::string& addr);

  private:
    const std::shared_ptr<void>& getSocket();

    std::shared_ptr<void> socket_;
};

}

#endif /* _SOCKET_46593F85D31E_ */
