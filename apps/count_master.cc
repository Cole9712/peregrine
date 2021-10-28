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
        template<class Archive>
        void serialize(Archive &a, const unsigned version){
            a & smGraph & payload0 & payload1;
        }
    public:
        MsgPayload(){}

        std::vector<Peregrine::SmallGraph> getSmallGraphs(){
          return smGraph;
        }

        std::string getPayload0(){
          return payload0;
        }

        std::string getPayload1(){
          return payload1;
        }

        MsgPayload(std::vector<Peregrine::SmallGraph> i, std::string x, std::string y=""):smGraph(i), payload0(x), payload1(y){}
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
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-motifs | #-clique> <# workers> <bind port>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t nworkers = std::stoi(argv[3]);
  const std::string bindPort(argv[4]);

  auto t1 = utils::get_timestamp();

  zmq::context_t ctx;
  zmq::socket_t sock(ctx, zmq::socket_type::rep);
  sock.bind("tcp://127.0.0.1:" + bindPort);

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
  
  zmq::message_t recv_msg(1024);
  for (int i = 0; i < nworkers; ++i) {
    MsgPayload init_payload(patterns, std::to_string(i), std::to_string(nworkers));
    std::cout << serialize<MsgPayload>(init_payload) << std::endl;
    
    std::string serialized = serialize(init_payload);
    zmq::mutable_buffer init_buf = zmq::buffer(serialized);    
    auto res = sock.recv(recv_msg, zmq::recv_flags::none);
    auto res2 = sock.send(init_buf, zmq::send_flags::dontwait);
    // std::cout << "Got message: " << std::endl;
    // void *vptr = recv_buf.data();
    // for (int j = 0; j < 10; ++j) {
    //   std::cout << *(char *)(vptr+j);
    // }
    // fflush(stdout);
  }

  // getting results from workers
  std::vector<std::string> result_pattern;
  std::vector<uint64_t> result_counts;

  zmq::message_t msg;
  for (int i = 0; i < nworkers; ++i) {
    auto res = sock.recv(recv_msg, zmq::recv_flags::none);
    auto res2 = sock.send(msg, zmq::send_flags::dontwait);
    std::cout << "Recvd Data:" << recv_msg.to_string() << std::endl;
    auto workerResult = deserialize<FinalResult>(recv_msg.to_string()).getResult();
    for (int i = 0; i < workerResult.size(); i++) {
      result_pattern.push_back(workerResult[i].first.to_string());
      result_counts.push_back(workerResult[i].second);
    }
  }



  auto t2 = utils::get_timestamp();

  utils::Log{} << "-------" << "\n";
  utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

  for (int i = 0; i < result_pattern.size(); i++) {
    std::cout << result_pattern[i] << ": " << result_counts[i] << std::endl;
  }  
  // for (int i = 0; i < result.size(); i++) {
  //   std::cout << result[i].first << ": " << reduced_sum[i] << std::endl;
  // }

  

  return 0;
}
