#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mpi.h>

#include "Peregrine.hh"

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
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-motifs | #-clique> [# threads]" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t nthreads = argc < 4 ? 1 : std::stoi(argv[3]);
  MPI_Init(NULL, NULL);
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
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

  std::vector<std::string> result_pattern;
  std::vector<uint64_t> result_counts;

  MPI_Barrier(MPI_COMM_WORLD);
  auto t1 = utils::get_timestamp();

  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  if (is_directory(data_graph_name))
  {
    result = Peregrine::count(data_graph_name, patterns, nthreads, world_rank, world_size);
  }
  else
  {
    Peregrine::SmallGraph G(data_graph_name);
    result = Peregrine::count(G, patterns, nthreads, world_rank, world_size);
  }

  for (int i = 0; i < result.size(); i++) {
    result_pattern.push_back(result[i].first.to_string());
    result_counts.push_back(result[i].second);
  }
  // char buf1[2048];
  // MPI_Gather(&result_pattern[0], result_pattern.capacity(), )

  uint64_t reduced_sum[result.size()];
  for (int i = 0; i < result.size(); i++) {
    reduced_sum[i] = 0;

    MPI_Reduce(&result_counts[i], &reduced_sum[i], 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  }

  auto t2 = utils::get_timestamp();

  if (world_rank == 0) {
    utils::Log{} << "-------" << "\n";
    utils::Log{} << "all patterns finished after " << (t2-t1)/1e6 << "s" << "\n";

    for (int i = 0; i < result.size(); i++) {
      std::cout << result[i].first << ": " << reduced_sum[i] << std::endl;
    }
  }
  

  MPI_Finalize();
  return 0;
}
