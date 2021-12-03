#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zmq.hpp>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/base_object.hpp>

#include "Peregrine.hh"

class MsgPayload
{
private:
  friend class boost::serialization::access;
  int msgType;
  std::vector<Peregrine::SmallGraph> smGraph;
  int startPt;
  int endPt;
  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  template <class Archive>
  void serialize(Archive &a, const unsigned version)
  {
    a &msgType &smGraph &startPt &endPt &result;
  }

public:
  MsgPayload() {}

  std::vector<Peregrine::SmallGraph> getSmallGraphs()
  {
    return smGraph;
  }

  int getType() { return msgType; }

  int getStartPt() { return startPt; }

  int getEndPt() { return endPt; }

  void setRange(int start, int end)
  {
    startPt = start;
    endPt = end;
  }

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> getResult() { return result; }

  MsgPayload(int type, std::vector<Peregrine::SmallGraph> i, std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result) : msgType(type), smGraph(i), result(result) {}
};

bool is_directory(const std::string &path)
{
  struct stat statbuf;
  if (stat(path.c_str(), &statbuf) != 0)
    return 0;
  return S_ISDIR(statbuf.st_mode);
}

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> [# threads] <Master Address>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  size_t nthreads = argc < 3 ? 1 : std::stoi(argv[2]);
  const std::string remoteAddr(argv[3]);
  int pID = 0;

  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::req);
  std::cout << "Connecting to " << remoteAddr << std::endl;
  sock.connect(remoteAddr);

  MsgPayload init_payload = MsgPayload(MsgTypes::handshake, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
  std::string init_serial = boost_utils::serialize<MsgPayload>(init_payload);
  zmq::mutable_buffer send_buf = zmq::buffer(init_serial);
  auto res = sock.send(send_buf, zmq::send_flags::none);
  zmq::message_t recv_msg(2048);
  auto recv_res = sock.recv(recv_msg, zmq::recv_flags::none);
  MsgPayload handshake_deserialized = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
  auto patterns = handshake_deserialized.getSmallGraphs();
  pID = handshake_deserialized.getStartPt();

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> tmpResult;
  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  MsgPayload sent_payload = MsgPayload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
  // send pID along with request message
  sent_payload.setRange(pID, pID);
  std::string sent_serial = boost_utils::serialize<MsgPayload>(sent_payload);
  zmq::mutable_buffer transmit_buf = zmq::buffer(sent_serial);

  auto t1 = utils::get_timestamp();

  while (true)
  {
    // request new patterns from master node
    res = sock.send(transmit_buf, zmq::send_flags::none);
    recv_res = sock.recv(recv_msg, zmq::recv_flags::none);
    MsgPayload deserialized = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
    std::cout << "Received range:" << deserialized.getStartPt() << "-" << deserialized.getEndPt() << std::endl;
    // receive command to end
    if (deserialized.getType() == MsgTypes::goodbye)
    {
      // send back result, and say bye to server
      MsgPayload resultToSend(MsgTypes::goodbye, std::vector<Peregrine::SmallGraph>(), result);
      std::string serializedResult = boost_utils::serialize<MsgPayload>(resultToSend);
      // std::cout << serializedResult << std::endl;
      send_buf = zmq::buffer(serializedResult);
      res = sock.send(send_buf, zmq::send_flags::none);
      break;
    }
    else if (deserialized.getType() == MsgTypes::wait)
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    else
    {
      if (is_directory(data_graph_name))
      {
        tmpResult = Peregrine::count(data_graph_name, patterns, nthreads, deserialized.getStartPt(), deserialized.getEndPt());
      }
      else
      {
        Peregrine::SmallGraph G(data_graph_name);
        tmpResult = Peregrine::count(G, patterns, nthreads, deserialized.getStartPt(), deserialized.getEndPt());
      }
    }
    if (result.size() == 0)
    {
      result.insert(result.end(), tmpResult.begin(), tmpResult.end());
    }
    else
    {
      for (int i = 0; i < result.size(); i++)
      {
        result[i].second += tmpResult[i].second;
      }
    }
  }

  std::cout << "Result has been sent to server." << std::endl;
  sock.close();

  auto t2 = utils::get_timestamp();

  utils::Log{} << "-------"
               << "\n";
  utils::Log{} << "Time taken: " << (t2 - t1) / 1e6 << "s"
               << "\n";

  return 0;
}
