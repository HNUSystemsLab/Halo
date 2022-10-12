#include <stdlib.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <regex>
#include <string>
#include <vector>

#include "cpucounters.h"
#include "hash_api.h"
#include "timer.h"
#include "utils.h"

using namespace std;
using namespace pcm;
// #define LATENCY
// #define PM_PCM
enum { OP_INSERT, OP_READ, OP_DELETE, OP_UPDATE };
enum workload_type {
  YCSB,
  PiBench,
};
enum Hash {
  Halo_t,
  VIPER_t,
  CCEH_t,
  Dash_t,
  Clevel_t,
  PCLHT_t,
  SOFT_t,
};

uint64_t LOAD_SIZE;
uint64_t RUN_SIZE;
uint64_t MAX_SIZE_LOAD = 200000000ULL;
uint64_t MAX_SIZE_RUN = 200000000ULL;
void run_test(workload_type wlt, int num_thread, string load_data,
              string run_data, hash_api *&h, string workload) {
  char values[] =
      "NvhE8N7yR26f4bbpMJnUKgHncH6QbsI10HyxlvYHKFiMk5nPNDbueF2xKLzteSd0NazU2APk"
      "JWXvBW2oUu8dkZnWMMu37G8TH2qm"
      "S0c8A9z41pxrC6ZU79OnfCZ06DsNXWY3U4dt1JTGQVvylBdZSlHWXC4PokCxsMdjv8xRptFM"
      "MQyHZRqMhNDnrsGKA12DEr7Zur0n"
      "tZpsyreMmPwuw7WMRnoN5wAYWtkqDwXyQlYb4RgtSc4xsonpTx2UhIUi15oJTx1CXAhmECac"
      "CQfntFnrSZt5qs1L64eeJ9Utus0N"
      "mKgEFV8qYDsNtJ21TkjCyCDhVIATkEugCw1BfNIB9AZDGiqXc0llp4rlJPl4bIG2QC4La3M1"
      "oh3yGlZTmdvN5pj1sIGkolpdoYVJ"
      "0NZM9KAo1d5sGFv9yGC7X0CTDOqyRu5c4NPktU70NbKqWNXa1kcaigIfeAuvJBs0Wso2osHz"
      "OjrbawgpfBPs1ePaWHgw7vbOcu9v"
      "Cqz1GnmdQw4mGSo4cc6tebQuKqLkQHuXa1MdRmzinBRoGQBQehqrDmmfNhcxfozcU7hOTjFA"
      "jryJ4HdSK57gOlrte5sZlvDW9rFd"
      "4OxG6WtFdZomRQPTNc4D9t7smqBR9EYDSjiAAqmIgZUiycHrlv6JQzEiexjqfGUbo8oJV6wi"
      "u7l3Jlfb94uByDxoexkMT5AjJzls"
      "er1dc9EfQz88q5Hv00g53Q3H6jcgicoY8YW5K4josd2e53ikesQi2kzqvTI9xxM5wtFexkFm"
      "8wFdMs6YmNpvNgTf37Hz204wX1Sf"
      "djFmCYEcP533LYcGB7CslEVMPYRZXHBT98XKtt8RqES7HBW65xSJRSj3qhIDUsgeu2Flo4Yq"
      "S68QoE69JzyBnwmmYw6uulVLVIAe"
      "iLl49oUhEiEjem8RrHPpEvrUoLDWwMdh14MfxwmEQbtGnUHEpRktUB6b7JTJN8OHBlLrvr71"
      "TkRK728ZgRv32rMZJ46O17qHTYc4"
      "AepNCGbpTII0J05OYiush6hiDo6H5pVHVUWy3nm7BBrBzEHVOCBMHNniw4CIzfavGLaUfgjl"
      "Bg0D4JBmYmkg0A4maCXsE9QTnGbA"
      "fQErGZkdMnRxXJ5EJ627e7zuFuVtazb0L65B3nU5R9tyUl2bTZiDcakK9evrTXoTkbkGjkCO"
      "iMSThGFScb6Lsgvl5wNCzlUZCxof"
      "jYQCLusRkXEm0CNVuifTnytctwLfKjwob4hJ0WxlQN9FV9Mm9zT61EQ8zEMrqr6hf7XMqhcQ"
      "R7DWAaf1fM4oNLIA7ZdKaspUaU6h"
      "oP2w3t3MktVaBp6MgS6Apbkb7EsihETHHqKFkKMCkYBbKfgsq7Jy49T1Wx2UJsD3XX03kVBb"
      "qRWmryYoMIqiCTCTqa0jIKzqQEnN";
  if (workload == "ycsbi") MAX_SIZE_RUN = 800000000;
  string insert("INSERT");
  string remove("REMOVE");
  string read("READ");
  string update("UPDATE");
  ifstream infile_load(load_data);
  string op;
  uint64_t key;
  uint64_t value_len;
  vector<uint64_t> init_keys(MAX_SIZE_LOAD);
  vector<uint64_t> keys(MAX_SIZE_RUN);
  vector<uint64_t> init_value_lens(MAX_SIZE_LOAD);
  vector<uint64_t> value_lens(MAX_SIZE_RUN);
  vector<int> ops(MAX_SIZE_RUN);
  int count = 0;
  while ((count < MAX_SIZE_LOAD) && infile_load.good()) {
    infile_load >> op >> key >> value_len;
    if (!op.size()) continue;
    if (op.size() && op.compare(insert) != 0) {
      cout << "READING LOAD FILE FAIL!\n";
      cout << op << endl;
      return;
    }
    init_keys[count] = key;
    init_value_lens[count] = value_len;
    count++;
  }
  LOAD_SIZE = count;
  infile_load.close();
  if (workload == "ycsbe") LOAD_SIZE = 0;
  fprintf(stderr, "Loaded %lu keys for initialing.\n", LOAD_SIZE);

  int *r = new int[1024];
  ifstream infile_run(run_data);
  count = 0;
  while ((count < MAX_SIZE_RUN) && infile_run.good()) {
    infile_run >> op >> key;
    if (op.compare(insert) == 0) {
      infile_run >> value_len;
      ops[count] = OP_INSERT;
      keys[count] = key;
      value_lens[count] = value_len;
    } else if (op.compare(update) == 0) {
      infile_run >> value_len;
      ops[count] = OP_UPDATE;
      keys[count] = key;
      value_lens[count] = value_len;
    } else if (op.compare(read) == 0) {
      ops[count] = OP_READ;
      keys[count] = key;
    } else if (op.compare(remove) == 0) {
      ops[count] = OP_DELETE;
      keys[count] = key;
    } else {
      continue;
    }
    count++;
  }
  RUN_SIZE = count;

#ifdef HALOT
#ifdef NONVAR
  Pair_t<size_t, size_t> *p = new Pair_t<size_t, size_t>[RUN_SIZE];
#elif VARVALUE
  Pair_t<size_t, std::string> *p = new Pair_t<size_t, std::string>[RUN_SIZE];
#else
  Pair_t<std::string, std::string> *p =
      new Pair_t<std::string, std::string>[RUN_SIZE];
#endif
#endif
  fprintf(stderr, "Loaded %d keys for running.\n", count);
  Timer tr;
  tr.start();
  h = new hash_api();
  printf("hash: %s %.1f ms.\n", h->hash_name().c_str(),
         tr.elapsed<std::chrono::milliseconds>());
#ifdef PM_PCM
  set_signal_handlers();
  PCM *m = PCM::getInstance();
  auto status = m->program();
  if (status != PCM::Success) {
    std::cout << "Error opening PCM: " << status << std::endl;
    if (status == PCM::PMUBusy)
      m->resetPMU();
    else
      exit(0);
  }
  print_cpu_details();
#endif
  auto part = LOAD_SIZE / num_thread;

  {
    // Load
    Timer sw;
    thread ths[num_thread];
    sw.start();
    auto insert = [&](size_t start, size_t len, int tid) {
      auto end = start + len;
#ifdef VIPERT

      auto c = h->get_client();
      for (size_t i = start; i < end; i++)
        h->insert(init_keys[i], init_value_lens[i],
                  reinterpret_cast<char *>(values), c);

#else
      for (size_t i = start; i < end; i++) {
        h->insert(init_keys[i], init_value_lens[i],
                  reinterpret_cast<char *>(values), tid, &r[i % 1024]);
      }

      h->wait();
#endif
    };
#ifdef PM_PCM
    auto before_state = getSystemCounterState();
#endif
    for (size_t i = 0; i < num_thread; i++) {
      ths[i] = thread(insert, part * i, part, i);
    }
    for (size_t i = 0; i < num_thread; i++) {
      ths[i].join();
    }
    auto t = sw.elapsed<std::chrono::milliseconds>();
    printf("Throughput: load, %f Mops/s\n",
           (LOAD_SIZE / 1000000.0) / (t / 1000.0));
#ifdef PM_PCM
    auto after_sstate = getSystemCounterState();
    cout << "MB ReadFromPMM: "
         << getBytesReadFromPMM(before_state, after_sstate) / 1000000 << " "
         << (getBytesReadFromPMM(before_state, after_sstate) / 1000000.0) /
                (t / 1000.0)
         << " MB/s" << endl;
    cout << "MB WrittenToPMM: "
         << getBytesWrittenToPMM(before_state, after_sstate) / 1000000 << " "
         << (getBytesWrittenToPMM(before_state, after_sstate) / 1000000.0) /
                (t / 1000.0)
         << " MB/s" << endl;
#endif
  }

  part = RUN_SIZE / num_thread;
  // Run
  Timer sw;
#ifdef LATENCY
  vector<size_t> latency_all;
  Mutex latency_mtx;
#endif
  std::function<void(size_t start, size_t len, int tid)> fun;
  auto operate = [&](size_t start, size_t len, int tid) {
    vector<size_t> latency;
    auto end = start + len;
    Timer l;
#ifdef VIPERT
    auto c = h->get_client();
    for (size_t i = start; i < end; i++) {
#ifdef LATENCY
      l.start();
#endif
      if (ops[i] == OP_INSERT) {
        h->insert(keys[i], value_lens[i], reinterpret_cast<char *>(values), c);
      } else if (ops[i] == OP_UPDATE) {
        h->update(keys[i], value_lens[i], reinterpret_cast<char *>(values), c);
      } else if (ops[i] == OP_READ) {
        uint64_t v;
        auto r = h->find(keys[i], c, &v);
      } else if (ops[i] == OP_DELETE) {
        h->erase(keys[i], c);
      }
#ifdef LATENCY
      latency.push_back(l.elapsed<std::chrono::nanoseconds>());
#endif
    }
    h->load_factor(c);
#else
#ifdef LATENCY
#ifdef HALOT
    l.start();
#endif
#endif
    bool rf = false;
    for (size_t i = start; i < end; i++) {
#ifdef LATENCY
#ifndef HALOT
      l.start();
#endif
#endif
      if (ops[i] == OP_INSERT) {
        rf = h->insert(keys[i], value_lens[i], reinterpret_cast<char *>(values),
                       tid, &r[i % 1024]);
      } else if (ops[i] == OP_UPDATE) {
        rf = h->update(keys[i], value_lens[i], reinterpret_cast<char *>(values),
                       tid, &r[i % 1024]);
      } else if (ops[i] == OP_READ) {
#ifdef HALOT
        rf = h->find(keys[i], &p[i]);
#else
        rf = h->find(keys[i]);
#endif
      } else if (ops[i] == OP_DELETE) {
        h->erase(keys[i], tid);
        rf = true;
      }
#ifdef LATENCY
#ifndef HALOT
      latency.push_back(l.elapsed<std::chrono::nanoseconds>());
#else
      if (rf) {
        latency.push_back(l.elapsed<std::chrono::nanoseconds>());
        rf = false;
        l.start();
      }
#endif

#endif
    }
#endif

#ifdef LATENCY
    lock_guard<Mutex> lock(latency_mtx);
    latency_all.insert(latency_all.end(), latency.begin(), latency.end());
#endif
    h->wait();
  };
  fun = operate;
  thread ths[num_thread];
#ifdef PM_PCM
  auto before_state = getSystemCounterState();
#endif
  sw.start();
  for (size_t i = 0; i < num_thread; i++) {
    ths[i] = thread(fun, part * i, part, i);
  }
  for (size_t i = 0; i < num_thread; i++) {
    ths[i].join();
  }
  auto t = sw.elapsed<std::chrono::milliseconds>();

  printf("Throughput: run, %f Mops/s\n",
         ((RUN_SIZE * 1.0) / 1000000) / (t / 1000));
#ifdef HALOT
  h->load_factor();
#endif

#ifdef PM_PCM
  auto after_sstate = getSystemCounterState();
  cout << "MB ReadFromPMM: "
       << getBytesReadFromPMM(before_state, after_sstate) / 1000000 << " "
       << (getBytesReadFromPMM(before_state, after_sstate) / 1000000.0) /
              (t / 1000.0)
       << " MB/s" << endl;
  cout << "MB WrittenToPMM: "
       << getBytesWrittenToPMM(before_state, after_sstate) / 1000000 << " "
       << (getBytesWrittenToPMM(before_state, after_sstate) / 1000000.0) /
              (t / 1000.0)
       << " MB/s" << endl;
#endif
#ifdef LATENCY
  sort(latency_all.begin(), latency_all.end());
  auto sz = latency_all.size();
  size_t avg = 0;
  for (size_t i = 0; i < sz; i++) {
    avg += latency_all[i];
  }
  avg /= sz;

  cout << "Latency: " << avg << " ns\n";
  cout << "\t0 " << latency_all[0] << "\n"
       << "\t50% " << latency_all[size_t(0.5 * sz)] << "\n"
       << "\t90% " << latency_all[size_t(0.9 * sz)] << "\n"
       << "\t99% " << latency_all[size_t(0.99 * sz)] << "\n"
       << "\t99.9% " << latency_all[size_t(0.999 * sz)] << "\n"
       << "\t99.99% " << latency_all[size_t(0.9999 * sz)] << "\n"
       << "\t99.999% " << latency_all[size_t(0.99999 * sz)] << "\n"
       << "\t100% " << latency_all[sz - 1] << endl;
#endif
  delete[] r;

#ifdef HALOT
  delete[] p;
#endif
}

int main(int argc, char **argv) {
  bool recovery = false;
  printf("workload: %s, threads: %s\n", argv[1], argv[2]);
  // if (argc == 4) recovery = true;
  // if (recovery) {
  //   hash_api *h;
  //   Timer tr;
  //   tr.start();
  //   h = new hash_api();
  //   printf("hash: %s Recovery cost %.1f ms.\n", h->hash_name().c_str(),
  //          tr.elapsed<std::chrono::milliseconds>());
  //   delete h;
  //   return 0;
  // }
  string workload = argv[1];
  workload_type wlt;
  string load_data = "";
  string run_data = "";
  if (workload.find("ycsb") != string::npos) {
    load_data = "YCSB/workloads/ycsb_load_workload";
    load_data += workload[workload.size() - 1];
    run_data = "YCSB/workloads/ycsb_run_workload";
    run_data += workload[workload.size() - 1];
    wlt = YCSB;
  } else if (workload.find("PiBench") != string::npos) {
    load_data = "PiBench/";
    load_data += workload;
    load_data += ".load";
    run_data = "PiBench/";
    run_data += workload;
    run_data += ".run";
    wlt = PiBench;
  }
  int num_thread = atoi(argv[2]);
  hash_api *h;
  run_test(wlt, num_thread, load_data, run_data, h, workload);
  // auto pid = getpid();
  // std::array<char, 128> buffer;
  // std::unique_ptr<FILE, decltype(&pclose)> pipe(
  //     popen(("cat /proc/" + to_string(pid) + "/status").c_str(), "r"),
  //     pclose);
  // if (!pipe) {
  //   throw std::runtime_error("popen() failed!");
  // }
  // while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
  //   string result = buffer.data();
  //   if (result.find("VmRSS") != string::npos) {
  //     std::string mem_ocp = std::regex_replace(
  //         result, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
  //     printf("DRAM consumption: %.1f GB.\n", stof(mem_ocp) / 1024 / 1024);
  //     break;
  //   }
  // }
#ifdef SOFTT
  vmem_stats_print(vmp1, "");
#elif PCLHTT
  vmem_stats_print(PCLHT::vmp, "");
#endif
  delete h;
  return 0;
}
