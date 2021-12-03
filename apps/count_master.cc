#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zmq.hpp>
#include <thread>
#include <atomic>
#include <algorithm>

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

class zmq_monitor_t : public zmq::monitor_t
{
public:
  std::vector<std::string> connectionList;
  int *nworkers;
  zmq_monitor_t(int *nworkers)
  {
    this->nworkers = nworkers;
  }

  void on_event_disconnected(const zmq_event_t &event,
                             const char *addr) override
  {
    std::cout << "got disconnected from " << addr << std::endl;
    connectionList.erase(std::remove(connectionList.begin(), connectionList.end(), addr), connectionList.end());
    (*nworkers)--;
  }
  void on_event_accepted(const zmq_event_t &event,
                         const char *addr) override
  {
    std::cout << "got connection from " << addr << std::endl;
    connectionList.push_back(addr);
  }
};

bool is_directory(const std::string &path)
{
  struct stat statbuf;
  if (stat(path.c_str(), &statbuf) != 0)
    return 0;
  return S_ISDIR(statbuf.st_mode);
}

void detectFailure(zmq_monitor_t &monitor, bool &tasksSoldOut)
{
  while (!tasksSoldOut)
  {
    monitor.check_event(100);
  }
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
  int *nworkers = new int(0);
  (*nworkers) = std::stoi(argv[3]);
  const std::string bindPort(argv[4]);
  int nTasks = std::stoi(argv[5]);
  int stoppedClients = 0;
  std::vector<uint64_t> pCtr;
  bool tasksSoldOut = false;

  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::rep);
  sock.bind("tcp://*:" + bindPort);

  zmq_monitor_t mon(nworkers);
  mon.init(sock, "inproc://connectmon", ZMQ_EVENT_ALL);

  auto monitorThread = std::thread(detectFailure, std::ref(mon), std::ref(tasksSoldOut));

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
  dg = new Peregrine::DataGraph(data_graph_name);
  uint32_t num_tasks = dg->get_vertex_count();
  std::cout << "Total number of Vertices:" << num_tasks << std::endl;

  std::vector<std::string> result_pattern;
  std::vector<uint64_t> result_counts;

  for (int i = 0; i < patterns.size(); i++)
  {
    result_pattern.push_back(patterns[i].to_string());
    result_counts.push_back(0);
  }

  // calculate the buffer size needed for receiving messages from workers
  size_t bufferSize = 100;
  for (const auto &pattern : patterns)
  {
    bufferSize += sizeof(pattern);
    bufferSize += 10;
  }

  zmq::message_t recv_msg(bufferSize);
  uint64_t vecPtr = 0;
  auto t1 = utils::get_timestamp();

  while (stoppedClients < (*nworkers))
  {
    auto res = sock.recv(recv_msg, zmq::recv_flags::none);
    auto recved_payload = boost_utils::deserialize<MsgPayload>(recv_msg.to_string());
    if (recved_payload.getType() == MsgTypes::handshake)
    {
      MsgPayload sent_payload(MsgTypes::handshake, patterns, std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
      // set pID for incoming worker
      sent_payload.setRange(pCtr.size(), 0);
      pCtr.push_back(0);
      std::string serialized = boost_utils::serialize(sent_payload);
      zmq::mutable_buffer send_buf = zmq::buffer(serialized);
      auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
    }
    else if (recved_payload.getType() == MsgTypes::transmit)
    {
      if (recved_payload.getResult().size() != 0)
      {
        auto workerResult = recved_payload.getResult();
        for (int i = 0; i < workerResult.size(); i++)
        {
          result_counts[i] += workerResult[i].second;
        }
      }
      int endPos = vecPtr + nTasks;
      if (vecPtr >= num_tasks)
      {
        if ((*nworkers) == pCtr.size() && tasksSoldOut)
        {
          MsgPayload sent_payload(MsgTypes::goodbye, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
          std::string serialized = boost_utils::serialize(sent_payload);
          zmq::mutable_buffer send_buf = zmq::buffer(serialized);
          auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        }
        else
        {
          // check which process fails
          pCtr[recved_payload.getStartPt()] = -1;
          int failedClientPos;
          int pausedClients = 0;
          for (int i = 0; i < pCtr.size(); ++i)
          {
            if (pCtr[i] == -1)
            {
              pausedClients++;
            }
            else
            {
              failedClientPos = i;
            }
          }

          if ((*nworkers) == pausedClients)
          {
            if (pausedClients == pCtr.size())
            {
              tasksSoldOut = true;
              // wait for monitor thread to exit
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
              MsgPayload sent_payload(MsgTypes::goodbye, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
              std::string serialized = boost_utils::serialize(sent_payload);
              zmq::mutable_buffer send_buf = zmq::buffer(serialized);
              auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
            }
            else
            {
              pCtr[recved_payload.getStartPt()] = pCtr[failedClientPos];
              pCtr.erase(pCtr.begin() + failedClientPos);

              MsgPayload sent_payload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
              if (pCtr[recved_payload.getStartPt()] + nTasks > num_tasks)
              {
                sent_payload.setRange(pCtr[recved_payload.getStartPt()], num_tasks);
              }
              else
              {
                sent_payload.setRange(pCtr[recved_payload.getStartPt()], pCtr[recved_payload.getStartPt()] + nTasks);
              }
              std::string serialized = boost_utils::serialize(sent_payload);
              zmq::mutable_buffer send_buf = zmq::buffer(serialized);
              auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
            }
          }
          else
          {
            MsgPayload sent_payload(MsgTypes::wait, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
            std::string serialized = boost_utils::serialize(sent_payload);
            zmq::mutable_buffer send_buf = zmq::buffer(serialized);
            auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
          }
        }
      }
      else if (endPos >= num_tasks)
      {
        MsgPayload sent_payload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
        sent_payload.setRange(vecPtr, num_tasks);
        pCtr[recved_payload.getStartPt()] = vecPtr;
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nTasks;
      }
      else
      {
        MsgPayload sent_payload(MsgTypes::transmit, std::vector<Peregrine::SmallGraph>(), std::vector<std::pair<Peregrine::SmallGraph, uint64_t>>());
        sent_payload.setRange(vecPtr, endPos);
        pCtr[recved_payload.getStartPt()] = vecPtr;
        std::string serialized = boost_utils::serialize(sent_payload);
        zmq::mutable_buffer send_buf = zmq::buffer(serialized);
        auto res2 = sock.send(send_buf, zmq::send_flags::dontwait);
        vecPtr += nTasks;
      }
    }
    else
    {
      // received goodbye from worker
      zmq::message_t msg;
      auto res2 = sock.send(msg, zmq::send_flags::dontwait);
      stoppedClients++;
      utils::Log{} << "stoppedClients++"
                   << "\n";
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

  monitorThread.join();
  // for (int i = 0; i < result.size(); i++) {
  //   std::cout << result[i].first << ": " << reduced_sum[i] << std::endl;
  // }

  return 0;
}
