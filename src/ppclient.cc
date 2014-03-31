#include <0mq/message.h>

#include <0mqmap/client.h>

#include <vector>
#include <iostream>

int main(int /*argc*/, const char** /*argv*/) {
  zmqmap::Client client;
  client.init();

  zmq::Message data(1);

  std::vector<zmq::Message> mapData;

  for (char i = 0; i < 4; ++i) {
    data.data()[0] = i;
    mapData.push_back(data);
  }

  std::vector<zmq::Message> result = client.map(mapData);

  for (char i = 0; i < 4; ++i) {
    std::cout << "got: " << static_cast<int>(result[i].data()[0]) <<
      std::endl;
  }

  client.map(mapData);
}
