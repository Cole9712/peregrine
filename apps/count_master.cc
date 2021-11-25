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
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-motifs | #-clique> <# workers> <bind port> <# tasks per request>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t nworkers = std::stoi(argv[3]);
  const std::string bindPort(argv[4]);
  int nTasks = std::stoi(argv[5]);

  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::rep);
  sock.bind("tcp://*:" + bindPort);

  std::vector<Peregrine::SmallGraph> patterns;
  if (auto end = pattern_name.rfind("motifs"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end - 1));
    patterns = Peregrine::PatternGenerator::all(k,
                                                Peregrine::PatternGenerator::VERTEX_BASED,
                                                Peregrine::PatternGenerator::INCLUDE_ANTI_EDGES);
  }
  else if (auto end = pattern_name.rfind("clique"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end - 1));
    patterns.emplace_back(Peregrine::PatternGenerator::clique(k));
  }
  else
  {
    patterns.emplace_back(pattern_name);
  }
  Peregrine::DataGraph *dg;
  auto new_patterns = Peregrine::getNewPatterns(patterns);
  dg = new Peregrine::DataGraph(data_graph_name);
  dg->set_rbi(new_patterns.front());
  uint32_t vgs_count = dg->get_vgs_count();
  uint32_t num_vertices = dg->get_vertex_count();
  uint32_t num_tasks = num_vertices * vgs_count;
  std::cout << "Total number of tasks:" << num_tasks << std::endl;

  std::vector<std::string> result_pattern;
  std::vector<uint64_t> result_counts;

  zmq::message_t recv_msg(2048);
  int connectClients = 0;
  int stoppedClients = 0;
  uint64_t vecPtr = 0;
  bool tasksSoldOut = false;
  auto t1 = utils::get_timestamp();
  while (stoppedClients < nworkers)
  {
    auto res = sock.recv(recv_msg, zmq::recv_flags::none);
    auto recved_payload = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
    if (recved_payload.getType() == MsgTypes::handshake)
    {
      MsgPayload sent_payload(MsgTypes::handshake, patterns, std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
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
        MsgPayload sent_payload(MsgTypes::goodbye, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
      }
      else if (endPos >= num_tasks)
      {
        tasksSoldOut = true;
        MsgPayload sent_payload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
        sent_payload.setRange(vecPtr, num_tasks);
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
      }
      else
      {
        MsgPayload sent_payload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
        sent_payload.setRange(vecPtr, endPos);
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nTasks;
      }
    }
    else
    {
      // received result from worker
      zmq::message_t msg;
      auto res2 = sock.send(msg, zmq::send_flags::dontwait);
      stoppedClients++;
      utils::Log{} << "stoppedClients++"
                   << "\n";
      auto workerResult = recved_payload.getResult();
      // std::cout << recv_msg.to_string() << std::endl;
      if (result_pattern.size() == 0)
      {
        for (int i = 0; i < workerResult.size(); i++)
        {
          result_pattern.push_back(workerResult[i].first.to_string());
          result_counts.push_back(workerResult[i].second);
        }
      } else {
        for (int i = 0; i < workerResult.size(); i++)
        {
          result_counts[i] += workerResult[i].second;
        }
      }
    }
  }

  auto t2 = utils::get_timestamp();

  utils::Log{} << "-------"
               << "\n";
  utils::Log{} << "all patterns finished after " << (t2 - t1) / 1e6 << "s"
               << "\n";

  for (int i = 0; i < result_pattern.size(); i++)
  {
    std::cout << result_pattern[i] << ": " << result_counts[i] << std::endl;
  }
  // for (int i = 0; i < result.size(); i++) {
  //   std::cout << result[i].first << ": " << reduced_sum[i] << std::endl;
  // }

  return 0;
}
