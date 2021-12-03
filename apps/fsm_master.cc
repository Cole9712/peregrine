// Suppress Boost serialization unused parameter warning
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
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

template <typename T>
void organize_vectors(std::vector<Peregrine::SmallGraph> &freq_patterns, std::vector<T> &support)
{
  auto inputPatterns = freq_patterns;
  auto inputSupport = support;
  std::vector<std::string> str_patterns;
  freq_patterns.clear();
  support.clear();

  for (long unsigned int i = 0; i < inputPatterns.size(); i++)
  {
    str_patterns.push_back(inputPatterns[i].to_string());
  }

  while (inputPatterns.size() > 0)
  {
    freq_patterns.push_back(inputPatterns[0]);
    T tmpSup = inputSupport[0];
    std::string tmpStr = str_patterns[0];
    str_patterns.erase(str_patterns.begin());
    inputSupport.erase(inputSupport.begin());
    inputPatterns.erase(inputPatterns.begin());

    for (long unsigned int i = 0; i < str_patterns.size(); i++)
    {
      if (tmpStr.compare(str_patterns[i]) == 0)
      {
        tmpSup += inputSupport[i];
        str_patterns.erase(str_patterns.begin() + i);
        inputSupport.erase(inputSupport.begin() + i);
        inputPatterns.erase(inputPatterns.begin() + i);
        i--;
      }
    }
    support.push_back(tmpSup);
  }
}

int main(int argc, char *argv[])
{
  if (argc < 4)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <max size> <support threshold> [vertex-induced] <# workers> <bind port> <tasks per worker>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  uint32_t k = std::stoi(argv[2]);

  uint64_t threshold = std::stoi(argv[3]);

  size_t nworkers = 1;
  std::string bindPort;
  int nTasks = 1;
  size_t nthreads = std::thread::hardware_concurrency();
  bool extension_strategy = Peregrine::PatternGenerator::EDGE_BASED;

  int step = 1;

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
    nTasks = std::stoi(argv[6]);
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
    nTasks = std::stoi(argv[7]);
  } else {
    return 0;
  }

  const auto view = [](auto &&v)
  { return v.get_support(); };

  std::vector<DiscoveryDomain<1>> supports_init;
  std::vector<Domain> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;
  std::vector<uint64_t> supports_result;
  std::vector<Peregrine::SmallGraph> tmp_patterns;

  std::cout << k << "-FSM with threshold " << threshold << std::endl;

  Peregrine::DataGraph dg(data_graph_name);

  Peregrine::DataGraph *dgv = &dg;

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
      freq_patterns.push_back(p);
      supports_init.push_back(supp);
    }
  }

  organize_vectors<DiscoveryDomain<1>>(freq_patterns, supports_init);

  // // For testing: print statistics
  // for (long unsigned int i = 0; i < freq_patterns.size(); i++)
  // {
  //   std::cout << freq_patterns[i].to_string() << ": " << supports_init[i].get_support() << std::endl;
  // }

  for (long unsigned int i = 0; i < freq_patterns.size(); i++)
  {
    if (supports_init[i].get_support() >= threshold)
    {
      tmp_patterns.push_back(freq_patterns[i]);
    }
  }

  auto t3 = utils::get_timestamp();
  std::vector<Peregrine::SmallGraph> patterns = Peregrine::PatternGenerator::extend(tmp_patterns, extension_strategy);
  freq_patterns.clear();
  tmp_patterns.clear();
  supports.clear();

  uint32_t vgs_count, num_vertices;
  uint64_t all_tasks_num;

  // prepare data for worker processes
  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::rep);
  sock.bind("tcp://*:" + bindPort);
  int connectClients = 0;
  int stoppedClients = 0;
  int pausedClients = 0;
  uint64_t vecPtr = 0;
  zmq::message_t recv_msg(20000);

  vgs_count = dgv->get_vgs_count();
  num_vertices = dgv->get_vertex_count();
  all_tasks_num = num_vertices * vgs_count;
  std::cout << "All tasks " << all_tasks_num << std::endl;

  while ((size_t)stoppedClients < nworkers)
  {
    (void) sock.recv(recv_msg, zmq::recv_flags::none);
    auto recved_payload = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
    if (recved_payload.getType() == MsgTypes::handshake)
    {
      MsgPayload sent_payload(MsgTypes::handshake, std::vector<Peregrine::SmallGraph>(), 0, std::vector<Domain>());
      sent_payload.setRemark(data_graph_name);
      std::string serialized = boost_utils::serialize(sent_payload);
      zmq::mutable_buffer send_buf = zmq::buffer(serialized);
      sock.send(send_buf, zmq::send_flags::dontwait);
      connectClients++;
    }
    else if (recved_payload.getType() == MsgTypes::transmit)
    {
      int endPos = vecPtr + nTasks;
      if (patterns.empty() || (uint32_t)step >= k)
      {
        MsgPayload sent_payload(MsgTypes::goodbye, std::vector<Peregrine::SmallGraph>(), 0, std::vector<Domain>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        sock.send(send_buf, zmq::send_flags::dontwait);
        stoppedClients++;
      }
      else if (vecPtr > all_tasks_num)
      {
        auto p = recved_payload.getSmallGraphs();
        auto domains = recved_payload.getDomains();
        for (long unsigned int i = 0; i < p.size(); i++)
        {
          freq_patterns.push_back(p[i]);
          supports.push_back(domains[i]);
        }
        MsgPayload sent_payload(MsgTypes::wait, std::vector<Peregrine::SmallGraph>(), step + 1, std::vector<Domain>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        sock.send(send_buf, zmq::send_flags::dontwait);

        if (recved_payload.getIteration() <= step)
        {
          ++pausedClients;
        }
        if ((size_t)pausedClients == nworkers)
        {
          pausedClients = 0;
          vecPtr = 0;
          ++step;
          // for (long unsigned int i = 0; i < freq_patterns.size(); i++)
          // {
          //   if (freq_patterns[i].to_string().compare("[1,1-2,1][1,1-3,1][2,1-4,1]") == 0)
          //   {
          //     std::cout << freq_patterns[i].to_string() << ": " << supports[i] << std::endl;
          //   }
          // }
          organize_vectors<Domain>(freq_patterns, supports);
          // std::cout << "-----------------After Organize----------------" << std::endl;
          // for (long unsigned int i = 0; i < freq_patterns.size(); i++)
          // {
          //   std::cout << freq_patterns[i].to_string() << ": " << supports[i].get_support() << std::endl;
          // }
          for (long unsigned int i = 0; i < freq_patterns.size(); i++)
          {
            if (supports[i].get_support() >= threshold)
            {
              tmp_patterns.push_back(freq_patterns[i]);
              supports_result.push_back(supports[i].get_support());
            }
          }
          // std::cout << "-----------------After Filter----------------" << std::endl;
          // for (long unsigned int i = 0; i < tmp_patterns.size(); i++)
          // {
          //   std::cout << tmp_patterns[i].to_string() << ": " << supports_result[i] << std::endl;
          // }
          // std::cout << "----------------------------------------" << std::endl;

          patterns = Peregrine::PatternGenerator::extend(tmp_patterns, extension_strategy);
          if ((uint32_t)step < k && !patterns.empty())
          {
            freq_patterns.clear();
            supports.clear();
            tmp_patterns.clear();
            supports_result.clear();
          }
        }
      }
      else if ((uint64_t)endPos > all_tasks_num)
      {
        auto p = recved_payload.getSmallGraphs();
        auto domains = recved_payload.getDomains();
        for (long unsigned int i = 0; i < p.size(); i++)
        {
          freq_patterns.push_back(p[i]);
          supports.push_back(domains[i]);
        }
        std::vector<Peregrine::SmallGraph> sent_vec(patterns.begin(), patterns.end());
        MsgPayload sent_payload(MsgTypes::transmit, sent_vec, step, std::vector<Domain>());
        sent_payload.setRange(vecPtr, all_tasks_num);
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nTasks;
      }
      else
      {
        auto p = recved_payload.getSmallGraphs();
        auto domains = recved_payload.getDomains();
        for (long unsigned int i = 0; i < p.size(); i++)
        {
          freq_patterns.push_back(p[i]);
          supports.push_back(domains[i]);
        }
        std::vector<Peregrine::SmallGraph> sent_vec(patterns.begin(), patterns.end());
        MsgPayload sent_payload(MsgTypes::transmit, sent_vec, step, std::vector<Domain>());
        sent_payload.setRange(vecPtr, endPos);
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nTasks;
      }
    }
  }
  auto t2 = utils::get_timestamp();

  std::cout << tmp_patterns.size() << " frequent patterns: " << std::endl;
  for (uint32_t i = 0; i < tmp_patterns.size(); ++i)
  {
    std::cout << tmp_patterns[i].to_string() << ": " << supports_result[i] << std::endl;
  }

  std::cout << "Initial Discovery finished in " << (t3 - t1) / 1e6 << "s" << std::endl;
  std::cout << "Matching finished in " << (t2 - t3) / 1e6 << "s" << std::endl;
  return 0;
}
