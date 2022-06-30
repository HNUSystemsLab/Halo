#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>

#include "key_generator.hpp"
#include "operation_generator.hpp"
#include "value_generator.hpp"

using namespace std;
enum class distribution_t : uint8_t {
  UNIFORM = 0,
  SELFSIMILAR = 1,
  ZIPFIAN = 2
};
string OPS[3] = {"READ", "INSERT", "REMOVE"};
class generator {
 public:
  generator(distribution_t key_distribution, size_t key_space_sz,
            float key_skew, int key_size, int value_size, float read_ratio,
            float insert_ratio, float remove_ratio)
      : op_generator_(read_ratio, insert_ratio, remove_ratio),
        value_generator_(value_size) {
    switch (key_distribution) {
      case distribution_t::UNIFORM:
        key_generator_ =
            std::make_unique<uniform_key_generator_t>(key_space_sz, key_size);
        break;

      case distribution_t::SELFSIMILAR:
        key_generator_ = std::make_unique<selfsimilar_key_generator_t>(
            key_space_sz, key_size, key_skew);
        break;

      case distribution_t::ZIPFIAN:
        key_generator_ = std::make_unique<zipfian_key_generator_t>(
            key_space_sz, key_size, key_skew);
        break;

      default:
        std::cout << "Error: unknown distribution!" << std::endl;
        exit(0);
    }
  }
  operation_generator_t op_generator_;

  /// Key generator.
  std::unique_ptr<key_generator_t> key_generator_;

  /// Value generator.
  value_generator_t value_generator_;
};

// key_distribution, key_space_sz, key_skew, value_size, read_ratio,
//     insert_ratio, remove_ratio
int main(int argc, char **argv) {
  if (argc < 11) {
    cout << "args:"
         << "\n\tkey_distribution"
         << "\n\tkey_space_sz_load: "
         << "\n\tkey_space_sz_run: "
         << "\n\tkey_skew(only for SELFSIMILAR and ZIPFIAN): "
         << "\n\tvalue_size: "
         << "\n\tread_ratio: "
         << "\n\tinsert_ratio: "
         << "\n\tremove_ratio: "
         << "\n\tnegative_ratio"
         << "\n\tOutput file: "
         << "\n\tSequence: " << endl;
  }
  string dist = argv[1];
  size_t key_space_sz_load = stoull(argv[2]);
  size_t key_space_sz_run = stoull(argv[3]);
  float key_skew = stof(argv[4]);
  size_t key_size = 8;
  size_t value_size = stoull(argv[5]);
  float read_ratio = stof(argv[6]);
  float insert_ratio = stof(argv[7]);
  float remove_ratio = stof(argv[8]);
  float negative_ratio = stof(argv[9]);

  string outfile = argv[10];
  int sequence = 0;
  if (argc == 12) sequence = stoull(argv[11]);
  auto key_distribution = distribution_t::UNIFORM;
  if (dist.compare("UNIFORM") == 0)
    key_distribution = distribution_t::UNIFORM;
  else if (dist.compare("SELFSIMILAR") == 0)
    key_distribution = distribution_t::SELFSIMILAR;
  else if (dist.compare("ZIPFIAN") == 0) {
    std::cout << "WARNING: initializing ZIPFIAN generator might take time."
              << std::endl;
    key_distribution = distribution_t::ZIPFIAN;
  } else {
    std::cout << "Invalid key distribution, must be one of "
              << "[UNIFORM | SELFSIMILAR | ZIPFIAN], but is " << dist
              << std::endl;
    exit(1);
  }
  auto sum = read_ratio + insert_ratio + remove_ratio;
  if (sum != 1.0) {
    std::cout << "Sum of ratios should be 1.0 but is " << sum << std::endl;
    exit(1);
  }
  cout << "args:\n"
       << "\tkey_distribution: " << dist << "\n"
       << "\tkey_space_sz_load: " << key_space_sz_load << "\n"
       << "\tkey_space_sz_run: " << key_space_sz_run << "\n"
       << "\tkey_skew(only for SELFSIMILAR and ZIPFIAN): " << key_skew << "\n"
       << "\tkey_size: " << key_size << "\n"
       << "\tvalue_size: " << value_size << "\n"
       << "\tread_ratio: " << read_ratio << "\n"
       << "\tinsert_ratio: " << insert_ratio << "\n"
       << "\tremove_ratio: " << remove_ratio << "\n"
       << "\tOutput file: " << outfile << endl;

  bool in_sequence = true;
  bool non_sequence = false;
  ofstream load(outfile + ".load", ios::out);
  ofstream run(outfile + ".run", ios::out);
  generator g(key_distribution, key_space_sz_run, key_skew, key_size,
              value_size, read_ratio, insert_ratio, remove_ratio);
  size_t negative_size = negative_ratio * key_space_sz_run;
  size_t count = 0;
  size_t *init_keys = new size_t[key_space_sz_load];
  cout << "load data generating..." << endl;
  for (size_t i = 0; i < key_space_sz_load; i++) {
    if (i % 1000000 == 0) {
      cout << "\r" << i * 100 / key_space_sz_load << "%";
      fflush(stdout);
    }
    const char *key_ptr;
    key_ptr = g.key_generator_->next(non_sequence);
    size_t key = *reinterpret_cast<size_t *>(const_cast<char *>(key_ptr));
    init_keys[i] = key;

    load << OPS[1] << " " << key << " " << value_size << "\n";
  }
  cout << "\nrun data generating..." << endl;
  for (size_t i = 0; i < key_space_sz_run; i++) {
    if (i % 1000000 == 0) {
      cout << "\r" << i * 100 / key_space_sz_run << "%";
      fflush(stdout);
    }
    const char *key_ptr;
    if (negative_size && count++ < negative_size)
      key_ptr = g.key_generator_->next(in_sequence);
    else
      key_ptr = g.key_generator_->next(non_sequence);
    size_t key = *reinterpret_cast<size_t *>(const_cast<char *>(key_ptr));
    if (sequence) key = i;
    auto op = g.op_generator_.next();
    if (op == operation_t::REMOVE) key = init_keys[i % key_space_sz_load];
    if (sequence) op = operation_t::INSERT;
    if (op == operation_t::READ || op == operation_t::REMOVE)
      run << OPS[(int)op] << " " << key << "\n";
    else
      run << OPS[(int)op] << " " << key << " " << value_size << "\n";
  }
  delete[] init_keys;
  load.close();
  run.close();
  return 0;
}