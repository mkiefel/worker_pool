#include <0mq/message.h>

#include <0mqmap/zmq_client.h>

#include <vector>

int main(int /*argc*/, const char** /*argv*/) {
  ZmqClient client;
  client.init();

  zmq::Message data(1);

  std::vector<zmq::Message> mapData;
  mapData.push_back(data);
  mapData.push_back(data);
  mapData.push_back(data);
  mapData.push_back(data);

  client.map(mapData);
}
