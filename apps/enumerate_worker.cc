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
  std::vector<unsigned long> support;
  int startPt;
  int endPt;
  std::string remark;
  template <class Archive>
  void serialize(Archive &a, const unsigned version)
  {
    a &msgType &smGraph &support &startPt &endPt &remark;
  }

public:
  MsgPayload() {}

  std::vector<Peregrine::SmallGraph> getSmallGraphs()
  {
    return smGraph;
  }

  int getType() { return msgType; }

  std::vector<unsigned long> getSupport() { return support; }

  std::string getRemark() { return remark; }

  int getStartPt() { return startPt; }

  int getEndPt() { return endPt; }

  void setRemark(const std::string input) { remark = input; }

  void setRange(int start, int end)
  {
    startPt = start;
    endPt = end;
  }

  MsgPayload(int type, std::vector<Peregrine::SmallGraph> i, std::vector<unsigned long> s) : msgType(type), smGraph(i), support(s) {}
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
    std::cerr << "USAGE: " << argv[0] << "<Master Address> [# threads] " << std::endl;
    return -1;
  }

  size_t nthreads = argc < 2 ? 1 : std::stoi(argv[2]);
  const std::string remoteAddr(argv[1]);

  const auto process = [](auto &&a, auto &&cm)
  { a.map(cm.pattern, 1); };
  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> psupps;

  std::vector<uint64_t> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;

  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::req);
  std::cout << "Connecting to " << remoteAddr << std::endl;
  sock.connect(remoteAddr);

  // handshake
  MsgPayload init_payload = MsgPayload(MsgTypes::handshake, std::vector<Peregrine::SmallGraph>(), std::vector<unsigned long>());
  std::string init_serial = boost_utils::serialize<MsgPayload>(init_payload);
  zmq::mutable_buffer send_buf = zmq::buffer(init_serial);
  auto res = sock.send(send_buf, zmq::send_flags::none);
  zmq::message_t recv_msg(2048);
  auto recv_res = sock.recv(recv_msg, zmq::recv_flags::none);
  MsgPayload init_deserialized = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
  const std::string data_graph_name = init_deserialized.getRemark();

  MsgPayload sent_payload = MsgPayload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), std::vector<unsigned long>());

  auto t1 = utils::get_timestamp();
  while (true)
  {
    // request new range from master node
    std::string sent_serial = boost_utils::serialize<MsgPayload>(sent_payload);
    zmq::mutable_buffer transmit_buf = zmq::buffer(sent_serial);
    res = sock.send(transmit_buf, zmq::send_flags::none);
    recv_res = sock.recv(recv_msg, zmq::recv_flags::none);
    MsgPayload deserialized = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());

    // receive command to end
    if (deserialized.getType() == MsgTypes::goodbye)
    {
      break;
    }
    else
    {
      freq_patterns.clear();
      supports.clear();
      if (is_directory(data_graph_name))
      {
        psupps = Peregrine::match<Peregrine::Pattern, uint64_t, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(data_graph_name, deserialized.getSmallGraphs(), 
          nthreads, process, Peregrine::default_viewer<uint64_t>, deserialized.getStartPt(), deserialized.getEndPt());
      }
      else
      {
        Peregrine::SmallGraph G(data_graph_name);
        psupps = Peregrine::match<Peregrine::Pattern, uint64_t, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(G, deserialized.getSmallGraphs(), 
          nthreads, process, Peregrine::default_viewer<uint64_t>, deserialized.getStartPt(), deserialized.getEndPt());
      }

      for (const auto &[p, supp] : psupps)
      {
        freq_patterns.push_back(p);
        supports.push_back(supp);
      }

      sent_payload = MsgPayload(MsgTypes::transmit, freq_patterns, supports);
    }
  }
  auto t2 = utils::get_timestamp();

  utils::Log{} << "-------"
               << "\n";
  utils::Log{} << "Time taken: " << (t2 - t1) / 1e6 << "s"
               << "\n";

  return 0;
}
