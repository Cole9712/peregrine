#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Peregrine.hh"

#include "Domain.hh"

template <typename T>
void organize_vectors(std::vector<Peregrine::SmallGraph> &freq_patterns, std::vector<T> &support)
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
    T tmpSup = inputSupport[0];
    std::string tmpStr = str_patterns[0];
    str_patterns.erase(str_patterns.begin());
    inputSupport.erase(inputSupport.begin());
    inputPatterns.erase(inputPatterns.begin());

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
  if (argc < 4)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <max size> <support threshold> [vertex-induced] [# threads]" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  uint32_t k = std::stoi(argv[2]);

  uint64_t threshold = std::stoi(argv[3]);

  size_t nthreads = std::thread::hardware_concurrency();
  bool extension_strategy = Peregrine::PatternGenerator::EDGE_BASED;

  uint32_t step = 1;

  // decide whether user provided # threads or extension strategy
  if (argc == 5)
  {
    std::string arg(argv[4]);
    if (arg.starts_with("v")) // asking for vertex-induced
    {
      extension_strategy = Peregrine::PatternGenerator::VERTEX_BASED;
      step = 2;
    }
    else if (!arg.starts_with("e")) // not asking for edge-induced
    {
      nthreads = std::stoi(arg);
    }
  }
  else if (argc == 6)
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
        nthreads = std::stoi(arg);
      }
    }
  }

  const auto view = [](auto &&v)
  { return v.get_support(); };

  std::vector<DiscoveryDomain<1>> supports;
  std::vector<Peregrine::SmallGraph> freq_patterns;
  std::vector<uint64_t> supports_result;

  std::cout << k << "-FSM with threshold " << threshold << std::endl;

  Peregrine::DataGraph dg(data_graph_name);

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
      supports.push_back(supp);
    }
  }

  organize_vectors<DiscoveryDomain<1>>(freq_patterns, supports);
  for (int i = 0; i < freq_patterns.size(); i++)
  {
    std::cout << freq_patterns[i].to_string() << ": " << supports[i].get_support() << std::endl;
  }
  std::vector<Peregrine::SmallGraph> tmp_patterns;
  for (int i = 0; i < freq_patterns.size(); i++)
  {
    if (supports[i].get_support() >= threshold)
    {
      tmp_patterns.push_back(freq_patterns[i]);
    }
  }

  auto t3 = utils::get_timestamp();
  std::vector<Peregrine::SmallGraph> patterns = Peregrine::PatternGenerator::extend(tmp_patterns, extension_strategy);

  const auto process = [](auto &&a, auto &&cm)
  {
    a.map(cm.pattern, cm.mapping);
  };

  std::vector<Domain> supports1;
  std::cout << "Pattern vector length: " << patterns.size() << std::endl;
  while (step < k && !patterns.empty())
  {

    freq_patterns.clear();
    supports1.clear();
    supports_result.clear();
    tmp_patterns.clear();
    auto psupps = Peregrine::match<Peregrine::Pattern, Domain, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(dg, patterns, nthreads, process, view);

    for (const auto &[p, supp] : psupps)
    {
      freq_patterns.push_back(p);
      supports1.push_back(supp);
    }

    organize_vectors<Domain>(freq_patterns, supports1);
    for (int i = 0; i < freq_patterns.size(); i++)
    {
      std::cout << freq_patterns[i].to_string() << ": " << supports1[i].get_support() << std::endl;
    }
    for (int i = 0; i < freq_patterns.size(); i++)
    {
      if (supports1[i].get_support() >= threshold)
      {
        tmp_patterns.push_back(freq_patterns[i]);
        supports_result.push_back(supports1[i].get_support());
      }
    }

    patterns = Peregrine::PatternGenerator::extend(tmp_patterns, extension_strategy);
    std::cout << "Number of new patterns: " << patterns.size() << std::endl;

    step += 1;
  }
  auto t2 = utils::get_timestamp();

  std::cout << tmp_patterns.size() << " frequent patterns: " << std::endl;
  for (uint32_t i = 0; i < tmp_patterns.size(); ++i)
  {
    std::cout << tmp_patterns[i].to_string() << ": " << supports_result[i] << std::endl;
  }

  std::cout << "Part 1 finished in " << (t3 - t1) / 1e6 << "s" << std::endl;
  std::cout << "Part 2 finished in " << (t2 - t3) / 1e6 << "s" << std::endl;
  return 0;
}
