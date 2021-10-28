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
  std::vector<Peregrine::SmallGraph> smGraph;
  std::string payload0;
  std::string payload1;
  template <class Archive>
  void serialize(Archive &a, const unsigned version)
  {
    a &smGraph &payload0 &payload1;
  }

public:
  MsgPayload() {}

  std::vector<Peregrine::SmallGraph> getSmallGraphs()
  {
    return smGraph;
  }

  std::string getPayload0()
  {
    return payload0;
  }

  std::string getPayload1()
  {
    return payload1;
  }

  MsgPayload(std::vector<Peregrine::SmallGraph> i, std::string x, std::string y = "") : smGraph(i), payload0(x), payload1(y) {}
};

class FinalResult {
private:
  friend class boost::serialization::access;
  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  template <class Archive>
  void serialize(Archive &a, const unsigned version)
  {
    a & result;
  }

public:
  FinalResult() {}
  FinalResult(std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> input) : result(input) {}

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> getResult() { return result; }
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
  zmq::message_t msg("hey!", 4);
  auto res = sock.send(msg, zmq::send_flags::none);

  // recv patterns from master node
  msg.rebuild(2048);
  auto recv_res = sock.recv(msg, zmq::recv_flags::none);

  std::cout << "Data: " << msg.to_string() << std::endl;
  std::cout << "Size: " << msg.size() << std::endl;

  MsgPayload deserialized = deserialize<MsgPayload>(msg.to_string());
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
  FinalResult resultToSend(result);
  std::string serializedResult = serialize<FinalResult>(resultToSend);
  zmq::mutable_buffer send_buf = zmq::buffer(serializedResult);
  res = sock.send(send_buf, zmq::send_flags::none);
  recv_res = sock.recv(msg, zmq::recv_flags::none);

  std::cout << "Result has been sent to server." << std::endl;

  auto t2 = utils::get_timestamp();

  // utils::Log{} << "-------" << "\n";
  // utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

  return 0;
}
