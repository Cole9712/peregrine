// Suppress Boost serialization unused parameter warning
#pragma GCC diagnostic ignored "-Wunused-parameter"
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
#include <chrono>
#include <thread>

#include "Peregrine.hh"

#include "Domain.hh"

class MsgPayload
{
private:
  friend class boost::serialization::access;
  int msgType;
  std::vector<Peregrine::SmallGraph> smGraph;
  int iteration;
  std::vector<Domain> domains;
  int startPt;
  int endPt;
  std::string remark;
  template <class Archive>
  void serialize(Archive &a, const unsigned version)
  {
    a &msgType &smGraph &iteration &domains &startPt &endPt &remark;
  }

public:
  MsgPayload() {}

  std::vector<Peregrine::SmallGraph> getSmallGraphs()
  {
    return smGraph;
  }

  int getType() { return msgType; }

  int getIteration()
  {
    return iteration;
  }

  std::vector<Domain> getDomains() { return domains; }

  std::string getRemark() { return remark; }

  int getStartPt() { return startPt; }

  int getEndPt() { return endPt; }

  void setRemark(const std::string input) { remark = input; }

  void setRange(int start, int end)
  {
    startPt = start;
    endPt = end;
  }

  MsgPayload(int type, std::vector<Peregrine::SmallGraph> i, int x, std::vector<Domain> s) : msgType(type), smGraph(i), iteration(x), domains(s) {}
};

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    std::cerr << "USAGE: " << argv[0] << "[# threads] <Master Address>" << std::endl;
    return -1;
  }

  std::string data_graph_name = "";
  size_t nthreads = argc < 2 ? 1 : std::stoi(argv[1]);
  const std::string remoteAddr(argv[2]);

  const auto view = [](auto &&v)
  { return v.get_support(); };
  const auto process = [](auto &&a, auto &&cm)
  {
    a.map(cm.pattern, cm.mapping);
  };
  std::vector<Domain> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;

  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::req);
  std::cout << "Connecting to " << remoteAddr << std::endl;
  sock.connect(remoteAddr);

  // handshake
  MsgPayload init_payload = MsgPayload(MsgTypes::handshake, std::vector<Peregrine::SmallGraph>(), 0, std::vector<Domain>());
  std::string init_serial = boost_utils::serialize<MsgPayload>(init_payload);
  zmq::mutable_buffer send_buf = zmq::buffer(init_serial);
  auto res = sock.send(send_buf, zmq::send_flags::none);
  zmq::message_t recv_msg(50000);
  auto recv_res = sock.recv(recv_msg, zmq::recv_flags::none);
  MsgPayload init_deserialized = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
  data_graph_name = init_deserialized.getRemark();

  int local_step = 0;
  MsgPayload sent_payload = MsgPayload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), local_step, std::vector<Domain>());
  utils::timestamp_t time_taken = 0;

  auto t1 = utils::get_timestamp();
  while (true)
  {
    // request new range from master node
    std::string sent_serial = boost_utils::serialize<MsgPayload>(sent_payload);
    zmq::mutable_buffer transmit_buf = zmq::buffer(sent_serial);
    res = sock.send(transmit_buf, zmq::send_flags::none);
    recv_res = sock.recv(recv_msg, zmq::recv_flags::none);
    auto t3 = utils::get_timestamp();
    MsgPayload deserialized = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
    local_step = deserialized.getIteration();
    // receive command to end
    if (deserialized.getType() == MsgTypes::goodbye)
    {
      break;
    }
    else if (deserialized.getType() == MsgTypes::wait)
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      sent_payload = MsgPayload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), local_step, std::vector<Domain>());
    }
    else
    {
      // std::cout << "Pattern vector length: " << deserialized.getSmallGraphs().size() << std::endl;
      freq_patterns.clear();
      supports.clear();
      Peregrine::DataGraph dg(data_graph_name);
      // std::cout << "StartPt: " << deserialized.getStartPt() << " EndPt:" << deserialized.getEndPt() << std::endl;
      auto psupps = Peregrine::match<Peregrine::Pattern, Domain, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(dg,
                                                                                                                deserialized.getSmallGraphs(), nthreads, process, view, deserialized.getStartPt(), deserialized.getEndPt());

      for (const auto &[p, supp] : psupps)
      {
        freq_patterns.push_back(p);
        supports.push_back(supp);
      }
      // std::cout << "Vector length: " << freq_patterns.size() << " " << supports.size() << std::endl;
      sent_payload = MsgPayload(MsgTypes::transmit, freq_patterns, local_step, supports);
    }
    auto t4 = utils::get_timestamp();
    time_taken += (t4 - t3);
  }
  auto t2 = utils::get_timestamp();

  utils::Log{} << "-------"
               << "\n";
  utils::Log{} << "Time taken all: " << (t2 - t1) / 1e6 << "s"
               << "\n";
  utils::Log{} << "Time taken for matching: " << time_taken / 1e6 << "s"
               << "\n";

  return 0;
}
