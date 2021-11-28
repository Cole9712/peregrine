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

void organize_vectors(std::vector<Peregrine::SmallGraph> &freq_patterns, std::vector<unsigned long> &support)
{
  auto inputPatterns = freq_patterns;
  auto inputSupport = support;
  std::vector<std::string> str_patterns;
  freq_patterns.clear();
  support.clear();

  for (int i = 0; i < inputPatterns.size(); i++)
  {
    str_patterns.push_back(inputPatterns[i].to_string());
  }

  while (inputPatterns.size() > 0)
  {
    freq_patterns.push_back(inputPatterns[0]);
    unsigned long tmpSup = 0;
    std::string tmpStr = str_patterns[0];

    for (int i = 0; i < str_patterns.size(); i++)
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
  if (argc < 3)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-motifs | #-clique> <# workers> <bind port> <tasks per worker>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t nworkers = std::stoi(argv[3]);
  const std::string bindPort(argv[4]);
  int nTasks = std::stoi(argv[5]);

  std::vector<Peregrine::SmallGraph> patterns;
  if (auto end = pattern_name.rfind("motifs"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end-1));
    patterns = Peregrine::PatternGenerator::all(k,
        Peregrine::PatternGenerator::VERTEX_BASED,
        Peregrine::PatternGenerator::INCLUDE_ANTI_EDGES);
  }
  else if (auto end = pattern_name.rfind("clique"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end-1));
    patterns.emplace_back(Peregrine::PatternGenerator::clique(k));
  }
  else
  {
    patterns.emplace_back(pattern_name);
  }

  auto t1 = utils::get_timestamp();

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  uint64_t all_tasks_num;

  // prepare data for worker processes
  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::rep);
  sock.bind("tcp://*:" + bindPort);
  int connectClients = 0;
  int stoppedClients = 0;
  uint64_t vecPtr = 0;
  zmq::message_t recv_msg(2048);
  bool tasksSoldOut = false;
  std::vector<uint64_t> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;

  // Get number of vertices
  if (is_directory(data_graph_name)) {
    Peregrine::DataGraph dg(data_graph_name);
    Peregrine::DataGraph *dgv = &dg;
    all_tasks_num = dgv->get_vertex_count();
  } else {
    Peregrine::SmallGraph G(data_graph_name);
    all_tasks_num = G.num_vertices();
  }
  std::cout << "All tasks " << all_tasks_num << std::endl;

  while (stoppedClients < nworkers)
  {
    auto res = sock.recv(recv_msg, zmq::recv_flags::none);
    auto recved_payload = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
    if (recved_payload.getType() == MsgTypes::handshake)
    {
      MsgPayload sent_payload(MsgTypes::handshake, std::vector<Peregrine::SmallGraph>(), std::vector<unsigned long>());
      sent_payload.setRemark(data_graph_name);
      std::string serialized = boost_utils::serialize(sent_payload);
      zmq::mutable_buffer send_buf = zmq::buffer(serialized);
      auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
      connectClients++;
    }
    else if (recved_payload.getType() == MsgTypes::transmit)
    {
      int endPos = vecPtr + nTasks;
      if (tasksSoldOut)
      {
        MsgPayload sent_payload(MsgTypes::goodbye, std::vector<Peregrine::SmallGraph>(), std::vector<unsigned long>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        stoppedClients++;
      }
      else if (endPos >= all_tasks_num)
      {
        tasksSoldOut = true;
        auto p = recved_payload.getSmallGraphs();
        auto supp = recved_payload.getSupport();
        for (int i = 0; i < p.size(); i++)
        {
          freq_patterns.push_back(p[i]);
          supports.push_back(supp[i]);
        }
        std::vector<Peregrine::SmallGraph> sent_vec(patterns.begin(), patterns.end());
        MsgPayload sent_payload(MsgTypes::transmit, sent_vec, std::vector<unsigned long>());
        sent_payload.setRange(vecPtr, all_tasks_num);
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nTasks;
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
        std::vector<Peregrine::SmallGraph> sent_vec(patterns.begin(), patterns.end());
        MsgPayload sent_payload(MsgTypes::transmit, sent_vec, std::vector<unsigned long>());
        sent_payload.setRange(vecPtr, endPos);
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nTasks;
      }
    }
  }

  organize_vectors(freq_patterns, supports);

  std::cout << freq_patterns.size() << " frequent patterns: " << std::endl;
  for (uint32_t i = 0; i < freq_patterns.size(); ++i)
  {
    std::cout << freq_patterns[i].to_string() << ": " << supports[i] << std::endl;
  }
  auto t2 = utils::get_timestamp();


  std::cout << "Finished in " << (t2 - t1) / 1e6 << "s" << std::endl;

  return 0;
}
