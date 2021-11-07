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

#include "Domain.hh"

class MsgPayload
{
private:
  friend class boost::serialization::access;
  int msgType;
  std::vector<Peregrine::SmallGraph> smGraph;
  int iteration;
  std::vector<unsigned long> support;
  template <class Archive>
  void serialize(Archive &a, const unsigned version)
  {
    a &msgType &smGraph &iteration &support;
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

  std::vector<unsigned long> getSupport() { return support; }

  MsgPayload(int type, std::vector<Peregrine::SmallGraph> i, int x, std::vector<unsigned long> s) : msgType(type), smGraph(i), iteration(x), support(s) {}
};

int main(int argc, char *argv[])
{
  if (argc < 4)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <max size> <support threshold> [vertex-induced] <# workers> <bind port> <patterns per process>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  uint32_t k = std::stoi(argv[2]);

  uint64_t threshold = std::stoi(argv[3]);

  size_t nworkers;
  std::string bindPort;
  int nPointsPerProcess;
  int nPatterns;
  size_t nthreads = std::thread::hardware_concurrency();
  bool extension_strategy = Peregrine::PatternGenerator::EDGE_BASED;

  uint32_t step = 1;

  // decide whether user provided # threads or extension strategy
  if (argc == 7)
  {
    std::string arg(argv[4]);
    if (arg.starts_with("v")) // asking for vertex-induced
    {
      extension_strategy = Peregrine::PatternGenerator::VERTEX_BASED;
      step = 2;
    }
    else if (!arg.starts_with("e")) // not asking for edge-induced
    {
      nworkers = std::stoi(arg);
    }
    nworkers = std::stoi(arg);
    bindPort = argv[5];
    nPatterns = std::stoi(argv[6]);
  }
  else if (argc == 8)
  {
    for (std::string arg : {argv[4], argv[5]})
    {
      if (arg.starts_with("v")) // asking for vertex-induced
      {
        extension_strategy = Peregrine::PatternGenerator::VERTEX_BASED;
        step = 2;
      }
      else if (!arg.starts_with("e")) // not asking for edge-induced
      {
        nworkers = std::stoi(arg);
      }
    }
    nworkers = std::stoi(argv[5]);
    bindPort = argv[6];
    nPatterns = std::stoi(argv[7]);
  }

  const auto view = [](auto &&v)
  { return v.get_support(); };

  std::vector<uint64_t> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;

  std::cout << k << "-FSM with threshold " << threshold << std::endl;

  Peregrine::DataGraph dg(data_graph_name);

  Peregrine::DataGraph *dgv = &dg;
  uint32_t vgs_count = dgv->get_vgs_count();
  uint32_t num_vertices = dgv->get_vertex_count();
  uint64_t all_tasks_num = num_vertices * vgs_count;

  std::cout << "All tasks " << all_tasks_num << std::endl;

  // initial discovery
  auto t1 = utils::get_timestamp();
  {
    const auto process = [](auto &&a, auto &&cm)
    {
      uint32_t merge = cm.pattern[0] == cm.pattern[1] ? 0 : 1;
      a.map(cm.pattern, std::make_pair(cm.mapping, merge));
    };

    std::vector<Peregrine::SmallGraph> patterns = {Peregrine::PatternGenerator::star(2)};
    patterns.front().set_labelling(Peregrine::Graph::DISCOVER_LABELS);
    auto psupps = Peregrine::match<Peregrine::Pattern, DiscoveryDomain<1>, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(dg, patterns, nthreads, process, view);
    for (const auto &[p, supp] : psupps)
    {
      if (supp >= threshold)
      {
        freq_patterns.push_back(p);
        supports.push_back(supp);
      }
    }
  }

  auto t3 = utils::get_timestamp();
  std::vector<Peregrine::SmallGraph> patterns = Peregrine::PatternGenerator::extend(freq_patterns, extension_strategy);
  freq_patterns.clear();
  supports.clear();

  // const auto process = [](auto &&a, auto &&cm) {
  //   a.map(cm.pattern, cm.mapping);
  // };

  // prepare data for worker processes
  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::rep);
  sock.bind("tcp://*:" + bindPort);
  int connectClients = 0;
  int stoppedClients = 0;
  int pausedClients = 0;
  uint64_t vecPtr = 0;
  zmq::message_t recv_msg(2048);

  while (stoppedClients < nworkers)
  {
    auto res = sock.recv(recv_msg, zmq::recv_flags::none);
    auto recved_payload = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
    if (recved_payload.getType() == MsgTypes::handshake)
    {
      MsgPayload sent_payload(MsgTypes::handshake, std::vector<Peregrine::SmallGraph>(), threshold, std::vector<unsigned long>());
      std::string serialized = boost_utils::serialize(sent_payload);
      zmq::mutable_buffer send_buf = zmq::buffer(serialized);
      auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
      connectClients++;
    }
    else if (recved_payload.getType() == MsgTypes::transmit)
    {
      int endPos = vecPtr + nPatterns;
      if (patterns.empty() || step >= k)
      {
        MsgPayload sent_payload(MsgTypes::goodbye, std::vector<Peregrine::SmallGraph>(), 0, std::vector<unsigned long>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        stoppedClients++;
      }
      else if (vecPtr > patterns.size())
      {
        auto p = recved_payload.getSmallGraphs();
        auto supp = recved_payload.getSupport();
        for (int i = 0; i < p.size(); i++)
        {
          freq_patterns.push_back(p[i]);
          supports.push_back(supp[i]);
        }
        MsgPayload sent_payload(MsgTypes::wait, std::vector<Peregrine::SmallGraph>(), step + 1, std::vector<unsigned long>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);

        if (recved_payload.getIteration() <= step)
        {
          ++pausedClients;
        }
        if (pausedClients == nworkers)
        {
          pausedClients = 0;
          vecPtr = 0;
          ++step;
          patterns = Peregrine::PatternGenerator::extend(freq_patterns, extension_strategy);
          if (step < k && !patterns.empty())
          {
            freq_patterns.clear();
            supports.clear();
          }
        }
      }
      else if (endPos > patterns.size())
      {
        auto p = recved_payload.getSmallGraphs();
        auto supp = recved_payload.getSupport();
        for (int i = 0; i < p.size(); i++)
        {
          freq_patterns.push_back(p[i]);
          supports.push_back(supp[i]);
        }
        std::vector<Peregrine::SmallGraph> sent_vec(patterns.begin() + vecPtr, patterns.end());
        MsgPayload sent_payload(MsgTypes::transmit, sent_vec, step, std::vector<unsigned long>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nPatterns;
      }
      else
      {
        auto p = recved_payload.getSmallGraphs();
        auto supp = recved_payload.getSupport();
        for (int i = 0; i < p.size(); i++)
        {
          freq_patterns.push_back(p[i]);
          supports.push_back(supp[i]);
        }
        std::vector<Peregrine::SmallGraph> sent_vec(patterns.begin() + vecPtr, patterns.begin() + endPos);
        MsgPayload sent_payload(MsgTypes::transmit, sent_vec, step, std::vector<unsigned long>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nPatterns;
      }
    }
  }
  auto t2 = utils::get_timestamp();

  std::cout << freq_patterns.size() << " frequent patterns: " << std::endl;
  for (uint32_t i = 0; i < freq_patterns.size(); ++i)
  {
    std::cout << freq_patterns[i].to_string() << ": " << supports[i] << std::endl;
  }

  std::cout << "Part 1 finished in " << (t3 - t1) / 1e6 << "s" << std::endl;
  std::cout << "Part 2 finished in " << (t2 - t3) / 1e6 << "s" << std::endl;
  return 0;
}
