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

int main(int argc, char *argv[])
{
  if (argc < 4)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <max size> <support threshold> [vertex-induced] <# workers> <bind port> <points per process>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  uint32_t k = std::stoi(argv[2]);

  uint64_t threshold = std::stoi(argv[3]);

  size_t nworkers;
  std::string bindPort;
  int nPointsPerProcess;
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
    nPointsPerProcess = std::stoi(argv[6]);
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
    nPointsPerProcess = std::stoi(argv[7]);
  }


  const auto view = [](auto &&v) { return v.get_support(); };

  std::vector<uint64_t> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;

  std::cout << k << "-FSM with threshold " << threshold << std::endl;

  Peregrine::DataGraph dg(data_graph_name);

  Peregrine::DataGraph* dgv = &dg;
  uint32_t vgs_count = dgv->get_vgs_count();
  uint32_t num_vertices = dgv->get_vertex_count();
  uint64_t all_tasks_num = num_vertices * vgs_count;

  std::cout << "All tasks " << all_tasks_num << std::endl;

  // initial discovery
  // auto t1 = utils::get_timestamp();
  // {
  //   const auto process = [](auto &&a, auto &&cm) {
  //     uint32_t merge = cm.pattern[0] == cm.pattern[1] ? 0 : 1;
  //     a.map(cm.pattern, std::make_pair(cm.mapping, merge));
  //   };

  //   std::vector<Peregrine::SmallGraph> patterns = {Peregrine::PatternGenerator::star(2)};
  //   patterns.front().set_labelling(Peregrine::Graph::DISCOVER_LABELS);
  //   auto psupps = Peregrine::match<Peregrine::Pattern, DiscoveryDomain<1>, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(dg, patterns, nthreads, process, view);
  //   for (const auto &[p, supp] : psupps)
  //   {
  //     if (supp >= threshold)
  //     {
  //       freq_patterns.push_back(p);
  //       supports.push_back(supp);
  //     }
  //   }
  // }

  // // prepare data for worker processes
  // zmq::context_t ctx;
  // zmq::socket_t sock(ctx, zmq::socket_type::rep);
  // sock.bind("tcp://*:" + bindPort);


  // auto t3 = utils::get_timestamp();
  // std::vector<Peregrine::SmallGraph> patterns = Peregrine::PatternGenerator::extend(freq_patterns, extension_strategy);

  // const auto process = [](auto &&a, auto &&cm) {
  //   a.map(cm.pattern, cm.mapping);
  // };


  // while (step < k && !patterns.empty())
  // {
  //   freq_patterns.clear();
  //   supports.clear();
  //   auto psupps = Peregrine::match<Peregrine::Pattern, Domain, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(dg, patterns, nthreads, process, view);

  //   for (const auto &[p, supp] : psupps)
  //   {
  //     if (supp >= threshold)
  //     {
  //       freq_patterns.push_back(p);
  //       supports.push_back(supp);
  //     }
  //   }

  //   patterns = Peregrine::PatternGenerator::extend(freq_patterns, extension_strategy);
  //   step += 1;
  // }
  // auto t2 = utils::get_timestamp();

  // std::cout << freq_patterns.size() << " frequent patterns: " << std::endl;
  // for (uint32_t i = 0; i < freq_patterns.size(); ++i)
  // {
  //   std::cout << freq_patterns[i].to_string() << ": " << supports[i] << std::endl;
  // }

  // std::cout << "Part 1 finished in " << (t3-t1)/1e6 << "s" << std::endl;
  // std::cout << "Part 2 finished in " << (t2-t3)/1e6 << "s" << std::endl;
  return 0;
}
