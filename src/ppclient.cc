#include <0mq/message.h>

#include <0mqmap/client.h>

#include <vector>

int main(int /*argc*/, const char** /*argv*/) {
  zmqmap::Client client;
  client.init();

  zmq::Message data(1);

  std::vector<zmq::Message> mapData;
  mapData.push_back(data);
  mapData.push_back(data);
  mapData.push_back(data);
  mapData.push_back(data);

  client.map(mapData);
}
