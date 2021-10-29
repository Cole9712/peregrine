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
  std::string payload0;
  std::string payload1;
  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  template <class Archive>
  void serialize(Archive &a, const unsigned version)
  {
    a & msgType &smGraph &payload0 &payload1 &result;
  }

public:
  MsgPayload() {}

  std::vector<Peregrine::SmallGraph> getSmallGraphs()
  {
    return smGraph;
  }

  int getType() { return msgType; }

  std::string getPayload0()
  {
    return payload0;
  }

  std::string getPayload1()
  {
    return payload1;
  }

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> getResult() { return result; }

  MsgPayload(int type, std::vector<Peregrine::SmallGraph> i, std::string x, std::string y, std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result) : msgType(type), smGraph(i), payload0(x), payload1(y), result(result) {}
};

template <class T>
std::string serialize(T &dg)
{
  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << dg;
  return ss.str();
}

template <class T>
T deserialize(std::string input)
{
  std::stringstream ss(input);
  boost::archive::text_iarchive ia(ss);

  T obj;
  ia >> obj;
  return obj;
}

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
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-motifs | #-clique> [# threads] <Master Address>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t nthreads = argc < 4 ? 1 : std::stoi(argv[3]);
  const std::string remoteAddr(argv[4]);
  auto t1 = utils::get_timestamp();

  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::req);
  std::cout << "Connecting to " << remoteAddr << std::endl;
  sock.connect(remoteAddr);

  MsgPayload init_payload = MsgPayload(0, std::vector<Peregrine::SmallGraph>(), "", "", std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
  std::string init_serial = serialize<MsgPayload>(init_payload);
  zmq::mutable_buffer send_buf = zmq::buffer(init_serial);
  auto res = sock.send(send_buf, zmq::send_flags::none);

  // recv patterns from master node
  zmq::message_t recv_msg(2048);
  auto recv_res = sock.recv(recv_msg, zmq::recv_flags::none);

  std::cout << "Data: " << recv_msg.to_string() << std::endl;
  std::cout << "Size: " << recv_msg.size() << std::endl;

  MsgPayload deserialized = deserialize<MsgPayload>(recv_msg.to_string());
  std::vector<Peregrine::SmallGraph> patterns = deserialized.getSmallGraphs();

  std::vector<std::string> result_pattern;
  std::vector<uint64_t> result_counts;

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  if (is_directory(data_graph_name))
  {
    result = Peregrine::count(data_graph_name, patterns, nthreads, std::stoi(deserialized.getPayload0()), std::stoi(deserialized.getPayload1()));
  }
  else
  {
    Peregrine::SmallGraph G(data_graph_name);
    result = Peregrine::count(G, patterns, nthreads, std::stoi(deserialized.getPayload0()), std::stoi(deserialized.getPayload1()));
  }

  // send back the result to the server
  MsgPayload resultToSend(1, std::vector<Peregrine::SmallGraph>(), "", "", result);
  
  std::string serializedResult = serialize<MsgPayload>(resultToSend);
  // std::cout << serializedResult << std::endl;
  send_buf = zmq::buffer(serializedResult);
  res = sock.send(send_buf, zmq::send_flags::none);
  recv_res = sock.recv(recv_msg, zmq::recv_flags::none);

  std::cout << "Result has been sent to server." << std::endl;

  auto t2 = utils::get_timestamp();

  // utils::Log{} << "-------" << "\n";
  // utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

  return 0;
}
