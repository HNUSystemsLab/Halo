#include <string>
constexpr size_t pool_size = 1024ul * 1024ul * 1024ul * 128ul;
std::string index_pool_name = "/mnt/pmem/hash/";

#ifdef HALOT
#define NONVAR 1
// #define VARVALUE 1
#include "Halo/Halo.hpp"
using namespace HALO;
class hash_api {
 public:
#ifdef NONVAR
  Halo<size_t, size_t> *t;
#elif VARVALUE
  Halo<size_t, std::string> *t;
#else
  Halo<std::string, std::string> *t;
#endif
  hash_api(size_t sz = 16 * 1024 * 1024) {
    PM_PATH = "/mnt/pmem/hash/HaLo/";
#ifdef NONVAR
    t = new Halo<size_t, size_t>(sz);
#elif VARVALUE
    t = new Halo<size_t, std::string>(sz);
#else
    t = new Halo<std::string, std::string>(sz);
#endif
  }
  ~hash_api() { delete t; }
  std::string hash_name() { return "Halo"; };
  bool find(size_t key, void *p) {
#ifdef NONVAR
    auto pt = reinterpret_cast<Pair_t<size_t, size_t> *>(p);
    pt->set_key(key);
#elif VARVALUE
    auto pt = reinterpret_cast<Pair_t<size_t, std::string> *>(p);
    pt->set_key(key);
#else
    auto pt = reinterpret_cast<Pair_t<std::string, std::string> *>(p);
    pt->set_key(reinterpret_cast<char *>(&key), 8);
#endif
    return t->Get(pt);
  }
  bool insert(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p(key, *reinterpret_cast<size_t *>(value));
#elif VARVALUE
    Pair_t<size_t, std::string> p(key, value, value_len);
#else
    Pair_t<std::string, std::string> p(reinterpret_cast<char *>(&key), 8, value,
                                       value_len);
#endif
    return t->Insert(p, r);
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p(key, *reinterpret_cast<size_t *>(value));
#elif VARVALUE
    Pair_t<size_t, std::string> p(key, value, value_len);
#else
    Pair_t<std::string, std::string> p(reinterpret_cast<char *>(&key), 8, value,
                                       value_len);
#endif
    return t->Update(p, r);
  }

  bool erase(size_t key, int tid = 0) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p;
    p.set_key(key);
#elif VARVALUE
    Pair_t<size_t, std::string> p;
    p.set_key(key);
#else
    Pair_t<std::string, std::string> p;
    p.set_key(reinterpret_cast<char *>(&key), 8);
#endif
    t->Delete(p);
    return true;
  }
  void wait() { t->wait_all(); }
  void load_factor() { t->load_factor(); }
};
#endif
#ifdef CCEHT
#include "third/CCEH/CCEH_baseline.h"
class hash_api {
 public:
  cceh::CCEH<size_t> *cceh;
  hash_api(size_t sz = 1024 * 16) {
    bool file_exist = false;
    index_pool_name += "CCEH.data";
    if (FileExists(index_pool_name.c_str())) file_exist = true;
    Allocator::Initialize(index_pool_name.c_str(), pool_size);
    cceh = reinterpret_cast<cceh::CCEH<size_t> *>(
        Allocator::GetRoot(sizeof(cceh::CCEH<size_t>)));
    if (!file_exist) {
      new (cceh) cceh::CCEH<size_t>(sz, Allocator::Get()->pm_pool_);
    } else {
      new (cceh) cceh::CCEH<size_t>();
    }
  }
  ~hash_api() { Allocator::Close_pool(); }
  std::string hash_name() { return "CCEH"; };
  bool find(size_t key) {
    cceh->Get(key);
    return true;
  };
  bool insert(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    cceh->Insert(key, DEFAULT);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    return false;
  }
  bool erase(size_t key, int tid = 0) {
    cceh->Delete(key);
    return true;
  }
  void wait() {}
  void load_factor() {}
};
#endif

#ifdef DASHT
#include "third/Dash/ex_finger.h"
class hash_api {
 public:
  extendible::Finger_EH<size_t> *dash;
  // sz is the number of segment.
  hash_api(size_t sz = 1024 * 16) {
    bool file_exist = false;
    index_pool_name += "DASH.data";

    if (FileExists(index_pool_name.c_str())) file_exist = true;
    Allocator::Initialize(index_pool_name.c_str(), pool_size);
    dash = reinterpret_cast<extendible::Finger_EH<size_t> *>(
        Allocator::GetRoot(sizeof(extendible::Finger_EH<size_t>)));
    if (!file_exist) {
      new (dash) extendible::Finger_EH<size_t>(sz, Allocator::Get()->pm_pool_);
    } else {
      new (dash) extendible::Finger_EH<size_t>();
    }
  }
  ~hash_api() { Allocator::Close_pool(); }
  std::string hash_name() { return "Dash"; };
  bool find(size_t key) {
    dash->Get(key);
    return true;
  };
  bool insert(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    dash->Insert(key, DEFAULT);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    return false;
  }

  bool erase(size_t key, int tid = 0) {
    dash->Delete(key);
    return true;
  }
  void wait() {}
  void load_factor() {}
};
#endif

#ifdef CLEVELT
#include <filesystem>
// clang-format off
#include "third/CLevel/libpmemobj++/make_persistent.hpp"
#include "third/CLevel/libpmemobj++/p.hpp"
#include "third/CLevel/libpmemobj++/persistent_ptr.hpp"
#include "third/CLevel/libpmemobj++/pool.hpp"
#include "third/CLevel/src/libpmemobj_cpp_examples_common.hpp"
#include "third/CLevel/libpmemobj++/experimental/clevel_hash.hpp"
// clang-format on
#define LAYOUT "clevel_hash"
namespace nvobj = pmem::obj;
class hash_api {
  typedef nvobj::experimental::clevel_hash<nvobj::p<uint64_t>,
                                           nvobj::p<uint64_t>>
      persistent_map_type;

  struct root {
    nvobj::persistent_ptr<persistent_map_type> cons;
  };
  nvobj::pool<root> pop;
  nvobj::persistent_ptr<persistent_map_type> map;

 public:
  hash_api(size_t sz = 1024 * 16, int tnum = 32) {
    const char *path = "/mnt/pmem/pmem.data ";
    if (!std::filesystem::exists(path))
      pop =
          nvobj::pool<root>::create(path, LAYOUT, pool_size, S_IWUSR | S_IRUSR);
    else
      pop = nvobj::pool<root>::open(path, LAYOUT);
    auto proot = pop.root();
    nvobj::transaction::manual tx(pop);
    proot->cons = nvobj::make_persistent<persistent_map_type>();
    proot->cons->set_thread_num(tnum);
    map = proot->cons;
    nvobj::transaction::commit();
  }
  ~hash_api() { pop.close(); }
  std::string hash_name() { return "CLevel"; };
  bool find(size_t key) {
    pmem::obj::p<uint64_t> k = key;
    auto r = map->search(persistent_map_type::key_type(k));
    return true;
  };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    auto rr = map->insert(persistent_map_type::value_type(key, key), tid);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    auto rr = map->update(persistent_map_type::value_type(key, key + 1), tid);
    return true;
  }
  bool erase(size_t key, int tid) {
    auto r = map->erase(persistent_map_type::key_type(key), tid);
    return true;
  }
  void wait() {}
  void load_factor() {}
};

#endif

#ifdef PCLHTT
#include "third/PCLHT/clht_lb_res.h"
class hash_api {
 public:
  PCLHT::clht_t *iclht;
  hash_api(size_t sz = 1024 * 1024 * 16) {
    sz = log2(sz / ENTRIES_PER_BUCKET);
    sz = pow(2, sz);
    iclht = PCLHT::clht_create(sz);
  }
  bool find(size_t key) {
    auto r = PCLHT::clht_get(iclht, key);
    return true;
  }
  std::string hash_name() { return "PCLHT"; };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    PCLHT::clht_put(iclht, key, key);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    // PCLHT::clht_put_replace(iclht, key, key);
    return false;
  }
  bool erase(size_t key, int tid) {
    PCLHT::clht_remove(iclht, key);
    return true;
  }
  void wait() {}
  void load_factor() {}
};
#endif
#ifdef CLHTT
#include "third/CLHT/include/clht_lb_res.h"
class hash_api {
 public:
  clht_t *iclht;
  hash_api(size_t sz = 1024 * 1024 * 16) {
    sz = log2(sz / ENTRIES_PER_BUCKET);
    sz = pow(2, sz);
    iclht = clht_create(sz);
  }
  bool find(size_t key) {
    auto r = clht_get(iclht->ht, key);
    return true;
  }
  std::string hash_name() { return "CLHT"; };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    clht_put(iclht, key, key);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid = 0,
              int *r = nullptr) {
    // clht_put_replace(iclht, key, key);
    return false;
  }
  bool erase(size_t key, int tid) {
    clht_remove(iclht, key);
    return true;
  }
  void wait() {}
  void load_factor() {}
};
#endif

#ifdef SOFTT
#include "third/SOFT/SOFTList.h"
constexpr size_t BUCKET_NUM = 16 * 1024 * 1024;
class hash_api {
 public:
  hash_api() { table = new SOFTList<size_t>[BUCKET_NUM]; }
  std::string hash_name() { return "SOFT"; };
  bool insert(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    SOFTList<size_t> &bucket = getBucket(key);
    bucket.insert(key, key);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value, int tid,
              int *r = nullptr) {
    return true;
  }
  void thread_ini(int id) {
    init_alloc(id);
    init_volatileAlloc(id);
  }

  bool find(size_t key) {
    SOFTList<size_t> &bucket = getBucket(key);
    auto r = bucket.contains(key);
    return true;
  }

  bool erase(size_t key, int tid) {
    SOFTList<size_t> &bucket = getBucket(key);
    bucket.remove(key);
    return true;
  }
  void load_factor() {}
  void wait() {}

 private:
  static size_t h(size_t k, size_t _len = 8,
                  size_t _seed = static_cast<size_t>(0xc70f6907UL)) {
    auto _ptr = reinterpret_cast<void *>(&k);
    return std::_Hash_bytes(_ptr, _len, _seed);
  }
  SOFTList<size_t> &getBucket(uintptr_t k) { return table[h(k) % BUCKET_NUM]; }
  SOFTList<size_t> *table;
};
#endif

#ifdef VIPERT
#include <filesystem>

#include "third/viper/viper.hpp"

class hash_api {
 public:
  std::unique_ptr<viper::Viper<uint64_t, uint64_t>> viper_db;
  hash_api() {
    const size_t inital_size = pool_size;
    index_pool_name += "VIPER.data";
    if (!std::filesystem::exists(index_pool_name))
      viper_db = viper::Viper<uint64_t, uint64_t>::create(index_pool_name,
                                                          inital_size);
    else
      viper_db = viper::Viper<uint64_t, uint64_t>::open(index_pool_name);
  }
  ~hash_api() {}
  std::string hash_name() { return "Viper"; };
  bool find(size_t key, viper::Viper<uint64_t, uint64_t>::Client &c,
            uint64_t *v) {
    auto r = c.get(key, v);
    return true;
  }

  bool insert(size_t key, size_t value_len, char *value,
              viper::Viper<uint64_t, uint64_t>::Client &c) {
    c.put(key, key);
    return true;
  }
  bool update(size_t key, size_t value_len, char *value,
              viper::Viper<uint64_t, uint64_t>::Client &c) {
    c.put(key, key + 1);
    return true;
  }
  viper::Viper<uint64_t, uint64_t>::Client get_client() {
    return viper_db->get_client();
  }

  bool erase(size_t key, viper::Viper<uint64_t, uint64_t>::Client &c) {
    c.remove(key);
    return true;
  }
  void wait() {}
  void load_factor(viper::Viper<uint64_t, uint64_t>::Client &c) {
    // cout << "get_total_used_pmem: " << c.get_total_used_pmem() << endl;
    // cout << "get_total_allocated_pmem: " << c.get_total_allocated_pmem()
    //      << endl;
  }
};
#endif
