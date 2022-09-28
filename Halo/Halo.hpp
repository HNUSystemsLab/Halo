#pragma once

#include <emmintrin.h>
#include <libpmem.h>
#include <malloc.h>

#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "Pair_t.h"
#include "timer.h"

namespace HALO {
using namespace std;

#define ALIGNED(N) __attribute__((aligned(N)))
#define Likely(x) __builtin_expect((x), 1)
#define Unlikely(x) __builtin_expect((x), 0)
#define CAS_U64_BOOL(a, b, c) (CAS_U64(a, b, c) == b)

#define GET_CLHT_INDEX(kh, n) ((kh >> 56) % n)
#define ROUND_UP(s, n) (((s) + (n)-1) & (~(n - 1)))
constexpr size_t MAX_BATCHING_SIZE = 256;
constexpr size_t READ_BUFFER_SIZE = 16 /* Pairs */;

using clht_val_t = volatile size_t;
using clht_lock_t = volatile uint8_t;
constexpr int CACHE_LINE_SIZE = 64;
constexpr int ENTRIES_PER_BUCKET = 3;
enum LOCK_STATE { LOCK_FREE = 0, LOCK_UPDATE = 1, LOCK_RESIZE = 2 };
constexpr int CLHT_PERC_EXPANSIONS = 1;
constexpr int CLHT_MAX_EXPANSIONS = 24;
constexpr int CLHT_PERC_FULL_DOUBLE = 50; /* % */
constexpr int CLHT_OCCUP_AFTER_RES = 20;
constexpr int CLHT_PERC_FULL_HALVE = 5; /* % */
constexpr int CLHT_RATIO_HALVE = 8;

extern string PM_PATH;
constexpr size_t PAGE_SIZE = 64ULL * 1024 * 1024 /* bytes */;
constexpr size_t PRESERVE_SIZE_EACH_PAGE = 64 /* bytes */;
constexpr size_t RESTORE_FILE_SIZE = 1 * 1024 * 1024 * 1024UL;
constexpr size_t METADATA_SIZE = 4 * 1024 * 1024UL;
// it indicates a capacity of 64M*1024*1024 = 64TB
constexpr size_t MAX_PAGE_NUM = 1024 * 1024;
constexpr size_t TABLE_NUM = 4;
constexpr size_t CORE_NUM = 128;

struct root;
constexpr float CLEAN_THRESHOLD = 60;
class PM_MemoryManager;
class DRAM_MemoryManager;
class MemoryManagerPool;
class Segment;
class CLHT;
char *get_DPage_addr(size_t offset);
constexpr size_t MAX_BUFFER_PAIR_SIZE = 32;
constexpr size_t MAX_WRITE_BUFFER_SIZE = 2048;
constexpr size_t MAX_PAIR = 16;
extern MemoryManagerPool memory_manager_Pool;
extern thread_local PM_MemoryManager mmanager;
extern thread_local int *INSERT_RESULT_POINTER[MAX_BUFFER_PAIR_SIZE];
extern thread_local size_t WRITE_BUFFER_COUNTER;
extern thread_local char WRITE_BUFFER[MAX_WRITE_BUFFER_SIZE];
extern thread_local size_t WRITE_BUFFER_SIZE;
extern thread_local size_t WRITE_PASS_COUNT;
extern thread_local void *BUFFER_READ[READ_BUFFER_SIZE];
extern thread_local size_t BUFFER_READ_COUNTER;
enum INSERT_STATE { EXIST = -1, INSERTING = 0, DONE = 1 };
extern root *ROOT;
extern atomic_size_t PPage_table[MAX_PAGE_NUM];

// for reclaim
extern std::mutex RECLAIM_MTX;
extern atomic_bool RECLAIM;
extern std::vector<thread> reclaim_threads;
extern std::mutex thread_mutex;
extern bool RECLAIM_LOCK[CORE_NUM];

constexpr bool SNAPSHOT = true;
constexpr bool LOGCLEAN = false;

constexpr size_t DEAFULT_SEGMENT_SIZE = 16 * 1024 * 1024;

void READ_LOCK();
void WAIT_READ_LOCK();
void RELEASE_READ_LOCK();

struct SEGMENT_SIZE_AND_SNAPSHOT_VERSION {
  size_t SEGMENT_SIZE;
  size_t SNAPSHOT_VERSION;
};
std::vector<size_t> nphase();
struct root {
  SEGMENT_SIZE_AND_SNAPSHOT_VERSION
  SS[TABLE_NUM];  // the SEGMENT_SIZE and SNAPSHOT_VERSIONS belong to the
                  // same SEGMENT are in the same cacheline.
                  // log-free concistency guarantee
  size_t SEGMENT_CHECKPOINT[TABLE_NUM][CORE_NUM];
  size_t CURRENT_DPAGE_ID[TABLE_NUM];
  size_t CURRENT_DPAGE_OFFSET[TABLE_NUM];
  size_t CURRENT_PPAGE_ID[CORE_NUM];
  size_t CURRENT_PPAGE_OFFSET[CORE_NUM];
  size_t PPAGE_ID;
  size_t DPAGE_ID;
  int clean;
  std::mutex mtx;
};
struct PAGE_METADATA {
  size_t PAGEID;
  size_t LOCAL_OFFSET;
  size_t FREED;
  size_t ALLOCATOR_ID;
  size_t RECLAIMED;
};
class MemoryManager {
 public:
  MemoryManager() {
    status = false;
    local_offset = PAGE_SIZE + 1;
    base_addr = nullptr;
  };
  ~MemoryManager(){};
  virtual void creat_new_space() = 0;
  void init(int);
  // offset and addr
  virtual pair<size_t, char *> halloc(size_t size) = 0;
  /**
   * @brief
   *
   * @param sz the size of the file to be created (in Bytes).
   * @param filename the name of the file to store data.
   * @return void*
   */
  static void *map_pm_file(size_t sz, string filename) {
    void *ptr;
    size_t mapped_len;
    int is_pmem;
    if ((ptr = pmem_map_file(filename.c_str(), sz, PMEM_FILE_CREATE, 0666,
                             &mapped_len, &is_pmem)) == NULL) {
      perror("pmem_map_file");
    }
    return ptr;
  }

  void delete_pm_file(size_t page_id);
  bool status;
  char *base_addr;
  int ID;
  size_t current_PAGE_ID;
  size_t local_offset;
  static atomic_size_t PAGE_ID;
};
class PM_MemoryManager : public MemoryManager {
 public:
  bool workthread;
  PM_MemoryManager() {
    ID = -1;
    workthread = false;
  }
  ~PM_MemoryManager();
  void creat_new_space();
  void update_metadata();
  void realloc(size_t page_id);
  virtual pair<size_t, char *> halloc(size_t size);
  static size_t PAGE_ID;
  static mutex mtx;
};
class DRAM_MemoryManager : public MemoryManager {
 public:
  DRAM_MemoryManager(int id, size_t version) {
    ID = id;
    snapshot_version = version;
  }
  void creat_new_space();
  virtual pair<size_t, char *> halloc(size_t size);
  size_t nphase();
  void clean();
  void persist(Segment *, std::vector<size_t>);
  void reclaim(Segment *);
  std::vector<size_t> pages;
  size_t snapshot_version;
  mutex alloc_mtx;
  static size_t PAGE_ID;
  static mutex mtx;
};

class MemoryManagerPool {
 public:
  MemoryManagerPool();
  void get_PM_MemoryManager(PM_MemoryManager *p, bool work = true) {
    lock_guard<mutex> guard(mtx_pm_pool);
    for (size_t i = 0; i < CORE_NUM; i++) {
      if (!pm[i].status) {
        p->ID = i;
        p->base_addr = pm[i].base_addr;
        p->current_PAGE_ID = pm[i].current_PAGE_ID;
        p->local_offset = pm[i].local_offset;
        pm[i].status = true;
        memory_manager_Pool.ppage_in_use[i] = p->current_PAGE_ID;
        if (work) {
          thread_counter++;
          p->workthread = true;
        } else {
          p->workthread = false;
        }
        return;
      }
    }
    cerr << "THREAD_NUM exceeded\n======================================="
         << endl;
    exit(0);
  }
  void log_out(PM_MemoryManager *p) {
    lock_guard<mutex> guard(mtx_pm_pool);
    int id = p->ID;
    pm[id].base_addr = p->base_addr;
    pm[id].current_PAGE_ID = p->current_PAGE_ID;
    pm[id].local_offset = p->local_offset;
    pm[id].status = false;
    memory_manager_Pool.ppage_in_use[id] = INVALID;
    if (p->workthread) thread_counter--;
  }
  void init_MemoryManager(MemoryManager *m, int i);
  std::vector<size_t> startup(CLHT *clhts[TABLE_NUM]);
  void shutdown(CLHT *clhts[TABLE_NUM]);
  void creat();
  void info();
  bool is_in_allocating(size_t page_id) {
    for (size_t i = 0; i < CORE_NUM; i++) {
      if (ppage_in_use[i] == page_id) return true;
    }
    return false;
  }
  PM_MemoryManager pm[CORE_NUM];
  bool CURRENT_PPAGE_ID[CORE_NUM];
  size_t ppage_in_use[CORE_NUM];
  atomic_size_t thread_counter;
  static mutex mtx_pm_pool;
};

static inline size_t hash_func(
    const void *k, size_t _len,
    size_t _seed = static_cast<size_t>(0xc70f6907UL)) {
  return std::_Hash_bytes(k, _len, _seed);
}
struct Bucket {
  clht_lock_t lock;
  uint32_t hops;
  size_t key[ENTRIES_PER_BUCKET];
  clht_val_t val[ENTRIES_PER_BUCKET];
  volatile size_t next;
} ALIGNED(CACHE_LINE_SIZE);

struct Segment {
  union {
    struct {
      size_t num_buckets;
      Bucket *buckets;
      size_t hash;
      size_t snapshot_version;
      uint8_t next_cache_line[CACHE_LINE_SIZE - (3 * sizeof(size_t)) -
                              (sizeof(void *))];
      DRAM_MemoryManager *hallocD;
      Segment *table_prev;
      Segment *table_new;
      volatile uint32_t num_expands;
      union {
        volatile uint32_t num_expands_threshold;
        uint32_t num_buckets_prev;
      };
      volatile int32_t is_helper;
      volatile int32_t helper_done;
      size_t version_min;
    };
    uint8_t padding[2 * CACHE_LINE_SIZE];
  };
} ALIGNED(CACHE_LINE_SIZE);
struct CLHT {
  union {
    struct {
      Segment *table;
      uint8_t next_cache_line[CACHE_LINE_SIZE - (sizeof(void *))];
      Segment *table_resizing;
      size_t resize_location;
      int64_t ID;
      volatile clht_lock_t resize_lock;
      volatile clht_lock_t gc_lock;
      volatile clht_lock_t status_lock;
    };
    uint8_t padding[2 * CACHE_LINE_SIZE];
  };
  uint8_t TRYLOCK_ACQ(volatile uint8_t *addr) {
    uint8_t oldval;
    __asm__ __volatile__("xchgb %0,%1"
                         : "=q"(oldval), "=m"(*addr)
                         : "0"((unsigned char)0xff), "m"(*addr)
                         : "memory");
    return (uint8_t)oldval;
  }
  void TRYLOCK_RLS(clht_lock_t &lock) { lock = LOCK_STATE::LOCK_FREE; }

  inline void LOCK_RLS(clht_lock_t *lock) {
    _mm_sfence();
    *lock = 0;
  }
  int LOCK_ACQ(clht_lock_t *lock, Segment *h) {
    char once = 1;
    clht_lock_t l;
    while ((l = __sync_val_compare_and_swap(lock, LOCK_STATE::LOCK_FREE,
                                            LOCK_STATE::LOCK_UPDATE)) ==
           LOCK_STATE::LOCK_UPDATE) {
      if (once) once = 0;
      _mm_pause();
    }
    if (l == LOCK_STATE::LOCK_RESIZE) {
      while (h->table_new == NULL) {
        _mm_pause();
        _mm_mfence();
      }
      return 0;
    }
    return 1;
  }
  // Swap size_t
  static inline size_t swap_uint64(volatile size_t *target, size_t x) {
    __asm__ __volatile__("xchgq %0,%1"
                         : "=r"((size_t)x)
                         : "m"(*(volatile size_t *)target), "0"((size_t)x)
                         : "memory");
    return x;
  }
  static inline int LOCK_ACQ_RES(clht_lock_t *lock) {
    clht_lock_t l;
    while ((l = __sync_val_compare_and_swap(lock, LOCK_FREE,
                                            LOCK_STATE::LOCK_RESIZE)) ==
           LOCK_STATE::LOCK_UPDATE)
      _mm_pause();
    if (l == LOCK_STATE::LOCK_RESIZE) return 0;
    return 1;
  }
  uint32_t clht_put_seq(Segment *hashtable, size_t key, clht_val_t val,
                        size_t bin) {
    volatile Bucket *bucket = hashtable->buckets + bin;
    uint32_t j;

    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == INVALID) {
          bucket->val[j] = val;
          bucket->key[j] = key;
          return true;
        }
      }

      if (bucket->next == INVALID) {
        int null;
        auto r = clht_bucket_create_stats(hashtable, &null);
        auto b = r.second;
        bucket->next = r.first;
        b->val[0] = val;
        b->key[0] = key;
        return true;
      }

      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (true);
  }
  CLHT(size_t num_buckets, int id, size_t version, bool ini) {
    ID = id;
    table = clht_hashtable_create(num_buckets, version, ini);
    resize_location = UINT64_MAX;
    resize_lock = LOCK_FREE;
    gc_lock = LOCK_FREE;
    status_lock = LOCK_FREE;
  }
  /* Insert a key-value entry into a hash table. */
  int clht_put(size_t key, clht_val_t val) {
    Segment *hashtable = table;
    size_t bin = clht_hash(hashtable, key);
    volatile Bucket *bucket = hashtable->buckets + bin;

    clht_lock_t *lock = &bucket->lock;
    while (!LOCK_ACQ(lock, hashtable)) {
      hashtable = table;
      size_t bin = clht_hash(hashtable, key);

      bucket = hashtable->buckets + bin;
      lock = &bucket->lock;
    }

    size_t *empty = NULL;
    clht_val_t *empty_v = NULL;

    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == key) {
          LOCK_RLS(lock);
          return false;
        } else if (empty == NULL && bucket->key[j] == INVALID) {
          empty = (size_t *)&bucket->key[j];
          empty_v = &bucket->val[j];
        }
      }

      int resize = 0;
      if (Likely(bucket->next == INVALID)) {
        if (Unlikely(empty == NULL)) {
          auto r = clht_bucket_create_stats(hashtable, &resize);
          Bucket *b = r.second;
          b->val[0] = val;
          b->key[0] = key;
          bucket->next = r.first;
        } else {
          *empty_v = val;
          *empty = key;
        }

        LOCK_RLS(lock);
        if (Unlikely(resize)) {
          ht_status(1, 0);
        }
        return true;
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (true);
  }
  /* Remove a key-value entry from a hash table. */
  template <typename KEY, typename VALUE>
  pair<size_t, size_t> clht_remove(size_t key, Pair_t<KEY, VALUE> *Null) {
    Segment *hashtable = table;
    size_t bin = clht_hash(hashtable, key);
    volatile Bucket *bucket = hashtable->buckets + bin;

    clht_lock_t *lock = &bucket->lock;
    while (!LOCK_ACQ(lock, hashtable)) {
      hashtable = table;
      size_t bin = clht_hash(hashtable, key);

      bucket = hashtable->buckets + bin;
      lock = &bucket->lock;
    }
    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == key) {
          size_t sz = 0;
          auto key = bucket->key[j];
          auto poffset = bucket->val[j];
          bucket->key[j] = INVALID;
          bucket->val[j] = INVALID;
          auto page_id = poffset / PAGE_SIZE;
          auto addr = reinterpret_cast<char *>(PPage_table[page_id].load() +
                                               poffset % PAGE_SIZE);
          auto freed =
              &reinterpret_cast<PAGE_METADATA *>(PPage_table[page_id].load())
                   ->FREED;
          auto p = reinterpret_cast<Pair_t<KEY, VALUE> *>(addr);
          sz = __sync_fetch_and_add(freed, p->size());
          sz += p->size();
          pmem_persist(freed, 8);
          p->set_op_persist(OP_t::DELETED);
          LOCK_RLS(lock);
          return {sz, page_id};
        }
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (Unlikely(bucket != 0));
    LOCK_RLS(lock);
    return {0, 0};
  }
  void clht_remove(size_t key) {
    Segment *hashtable = table;
    size_t bin = clht_hash(hashtable, key);
    volatile Bucket *bucket = hashtable->buckets + bin;

    clht_lock_t *lock = &bucket->lock;
    while (!LOCK_ACQ(lock, hashtable)) {
      hashtable = table;
      size_t bin = clht_hash(hashtable, key);

      bucket = hashtable->buckets + bin;
      lock = &bucket->lock;
    }
    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == key) {
          bucket->key[j] = INVALID;
          bucket->val[j] = INVALID;
          LOCK_RLS(lock);
          return;
        }
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (Unlikely(bucket != 0));
    LOCK_RLS(lock);
    return;
  }
  /* Retrieve a key-value entry from a hash table. */
  pair<clht_val_t, uint8_t> clht_get(size_t key) {
    size_t bin = clht_hash(table, key);
    if (resize_lock)
      ;
    volatile Bucket *bucket = table->buckets + bin;
    // auto bbb = bucket;
    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        clht_val_t val = bucket->val[j];
        if (bucket->key[j] == key) {
          if (Likely(bucket->val[j] == val)) {
            return {val, 0};
          } else {
            return {INVALID, 0};
          }
        }
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (Unlikely(bucket != NULL));
    return {INVALID, 0};
  }
  /* Insert a key-value pair into a hashtable with replacement. */
  template <typename KEY, typename VALUE>
  pair<int, size_t> clht_put_replace(size_t key, Pair_t<KEY, VALUE> *p) {
    Segment *hashtable = table;
    size_t bin = clht_hash(hashtable, key);
    volatile Bucket *bucket = hashtable->buckets + bin;
    clht_lock_t *lock = &bucket->lock;

    while (!LOCK_ACQ(lock, hashtable)) {
      hashtable = table;
      size_t bin = clht_hash(hashtable, key);

      bucket = hashtable->buckets + bin;
      lock = &bucket->lock;
    }

    size_t *empty = NULL;
    clht_val_t *empty_v = NULL;

    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == key) {
          empty = (size_t *)&bucket->key[j];
          empty_v = &bucket->val[j];
          break;
        }
      }

      int r = 0;
      size_t old_offset = 0;
      if (Likely(bucket->next == INVALID)) {
        if (Unlikely(empty == NULL)) {
          LOCK_RLS(lock);
          return {0, 0};
        } else {
          old_offset = *empty_v;
          auto old = reinterpret_cast<Pair_t<KEY, VALUE> *>(
              PPage_table[old_offset / PAGE_SIZE].load() +
              old_offset % PAGE_SIZE);
          p->set_version(old->version);
          auto o_a = mmanager.halloc(p->size());
          p->store_persist(o_a.second);
          auto f = &reinterpret_cast<PAGE_METADATA *>(
                        PPage_table[old_offset / PAGE_SIZE].load())
                        ->FREED;
          r = __sync_fetch_and_add(f, old->size()) + old->size();
          *empty_v = o_a.first;
          LOCK_RLS(lock);
          return {r, old_offset};
        }
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (true);
  }
  /* update during recovery */
  template <typename KEY, typename VALUE>
  void clht_put_replace(size_t key, size_t poffset, Pair_t<KEY, VALUE> *Null) {
    Segment *hashtable = table;
    size_t bin = clht_hash(hashtable, key);
    volatile Bucket *bucket = hashtable->buckets + bin;
    clht_lock_t *lock = &bucket->lock;

    while (!LOCK_ACQ(lock, hashtable)) {
      hashtable = table;
      size_t bin = clht_hash(hashtable, key);

      bucket = hashtable->buckets + bin;
      lock = &bucket->lock;
    }

    size_t *empty = NULL;
    clht_val_t *empty_v = NULL;

    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == key) {
          empty = (size_t *)&bucket->key[j];
          empty_v = &bucket->val[j];
          break;
        }
      }

      int r = 0;
      size_t old_offset = 0;
      if (Likely(bucket->next == INVALID)) {
        if (Unlikely(empty == NULL)) {
          LOCK_RLS(lock);
          return;
        } else {
          old_offset = *empty_v;
          auto old = reinterpret_cast<Pair_t<KEY, VALUE> *>(
              PPage_table[old_offset / PAGE_SIZE].load() +
              old_offset % PAGE_SIZE);
          auto p = reinterpret_cast<Pair_t<KEY, VALUE> *>(
              PPage_table[poffset / PAGE_SIZE].load() + poffset % PAGE_SIZE);
          if (p->version > old->version) {
            if (old->op != OP_t::DELETED && old->op != OP_t::TRASH) {
              auto f = &reinterpret_cast<PAGE_METADATA *>(
                            PPage_table[old_offset / PAGE_SIZE].load())
                            ->FREED;
              __sync_fetch_and_add(f, old->size_ori());
            }
            *empty_v = poffset;
          }
          LOCK_RLS(lock);
          return;
        }
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (true);
  }
  template <typename KEY, typename VALUE>
  int clht_put_move(size_t key, PM_MemoryManager &mmanager,
                    Pair_t<KEY, VALUE> *p, size_t offset_old,
                    size_t *reclaimed) {
    Segment *hashtable = table;
    size_t bin = clht_hash(hashtable, key);
    volatile Bucket *bucket = hashtable->buckets + bin;

    clht_lock_t *lock = &bucket->lock;
    while (!LOCK_ACQ(lock, hashtable)) {
      hashtable = table;
      size_t bin = clht_hash(hashtable, key);

      bucket = hashtable->buckets + bin;
      lock = &bucket->lock;
    }

    size_t *empty = NULL;
    clht_val_t *empty_v = NULL;

    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == key) {
          empty = (size_t *)&bucket->key[j];
          empty_v = &bucket->val[j];
          break;
        }
      }
      int resize = 0;
      if (Likely(bucket->next == INVALID)) {
        if (Unlikely(empty == NULL)) {
          assert(empty == NULL);
        } else {
          if (*empty_v == offset_old) {
            auto r = mmanager.halloc(p->size());
            p->store_persist(r.second);
            *empty_v = r.first;
            mmanager.update_metadata();
            _mm_stream_si64(reinterpret_cast<long long *>(reclaimed),
                            *reinterpret_cast<long long *>(&offset_old));
            pmem_drain();
          }
        }
        LOCK_RLS(lock);
        return true;
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (true);
  }
  int ht_resize_pes(int is_increase, int by) {
    Segment *ht_old = table;

    if (TRYLOCK_ACQ(&resize_lock)) {
      return 0;
    }

    size_t num_buckets_new;
    if (is_increase == true) {
      num_buckets_new = by * ht_old->num_buckets;
    } else {
      num_buckets_new = ht_old->num_buckets / CLHT_RATIO_HALVE;
    }
    // printf("from %f to %f.\n", 1.0 * ht_old->num_buckets / 1024 / 1024,
    //        1.0 * num_buckets_new / 1024 / 1024);
    Segment *ht_new =
        clht_hashtable_create(num_buckets_new, ht_old->snapshot_version + 1);
    auto checkpoint = nphase();
    int32_t b;
    for (b = 0; b < ht_old->num_buckets; b++) {
      Bucket *bu_cur = ht_old->buckets + b;
      bucket_cpy(bu_cur, ht_new);
    }
    ht_new->table_prev = ht_old;

    swap_uint64((size_t *)&table, (size_t)ht_new);
    ht_old->table_new = ht_new;
    ht_new->hallocD->persist(ht_new, checkpoint);
    ht_old->hallocD->reclaim(ht_old);
    TRYLOCK_RLS(resize_lock);
    return 1;
  }
  int bucket_cpy(volatile Bucket *bucket, Segment *ht_new) {
    if (!LOCK_ACQ_RES(&bucket->lock)) {
      return 0;
    }
    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        size_t key = bucket->key[j];
        if (key != INVALID) {
          size_t bin = clht_hash(ht_new, key);
          clht_put_seq(ht_new, key, bucket->val[j], bin);
        }
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (bucket != NULL);

    return 1;
  }
  size_t clht_size(Segment *hashtable) {
    size_t num_buckets = hashtable->num_buckets;
    volatile Bucket *bucket = NULL;
    size_t size = 0;

    size_t bin;
    for (bin = 0; bin < num_buckets; bin++) {
      bucket = hashtable->buckets + bin;

      uint32_t j;
      do {
        for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
          if (bucket->key[j] > 0) {
            size++;
          }
        }

        bucket = (Bucket *)get_DPage_addr(bucket->next);
      } while (bucket != NULL);
    }
    return size;
  }

  size_t ht_status(int resize_increase, int just_print) {
    if (TRYLOCK_ACQ(&status_lock) && !resize_increase) {
      return 0;
    }

    Segment *hashtable = table;
    size_t num_buckets = hashtable->num_buckets;
    volatile Bucket *bucket = NULL;
    size_t size = 0;
    int expands = 0;
    int expands_max = 0;

    size_t bin;
    for (bin = 0; bin < num_buckets; bin++) {
      bucket = hashtable->buckets + bin;

      int expands_cont = -1;
      expands--;
      uint32_t j;
      do {
        expands_cont++;
        expands++;
        for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
          if (bucket->key[j] > 0) {
            size++;
          }
        }

        bucket = (Bucket *)get_DPage_addr(bucket->next);
      } while (bucket != NULL);

      if (expands_cont > expands_max) {
        expands_max = expands_cont;
      }
    }

    double full_ratio =
        100.0 * size / ((hashtable->num_buckets) * ENTRIES_PER_BUCKET);

    if (just_print) {
      printf(
          "[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% / "
          "expands: %4d / max expands: %2d\n",
          99, hashtable->num_buckets, size, full_ratio, expands, expands_max);
    } else {
      if ((full_ratio > 0 && full_ratio > CLHT_PERC_FULL_DOUBLE) ||
          expands_max > CLHT_MAX_EXPANSIONS || resize_increase) {
        int inc_by = (full_ratio / CLHT_OCCUP_AFTER_RES);
        int inc_by_pow2 = pow2roundup(inc_by);

        if (inc_by_pow2 == 1) {
          inc_by_pow2 = 2;
        }
        ht_resize_pes(1, inc_by_pow2);
      }
    }

    TRYLOCK_RLS(status_lock);
    return size;
  }
  static inline size_t pow2roundup(size_t x) {
    if (x == 0) return 1;
    --x;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    return x + 1;
  }

  inline int is_power_of_two(unsigned int x) {
    return ((x != 0) && !(x & (x - 1)));
  }

  static inline int is_odd(int x) { return x & 1; }

  pair<size_t, Bucket *> clht_bucket_create_stats(Segment *h, int *resize) {
    auto r = h->hallocD->halloc(sizeof(Bucket));
    Bucket *bucket = (Bucket *)r.second;
    bucket->lock = 0;

    uint32_t j;
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      bucket->key[j] = INVALID;
    }
    bucket->next = INVALID;

    if (__sync_add_and_fetch(&h->num_expands, 1) == h->num_expands_threshold) {
      /* printf("      -- hit threshold (%u ~ %u)\n", h->num_expands,
       * h->num_expands_threshold); */
      *resize = 1;
    }
    return {r.first, reinterpret_cast<Bucket *>(r.second)};
  }

  Segment *clht_hashtable_create(size_t num_buckets, size_t version,
                                 bool ini = true) {
    Segment *hashtable = NULL;

    if (num_buckets == 0) {
      return NULL;
    }

    /* Allocate the table itself. */
    hashtable = (Segment *)memalign(CACHE_LINE_SIZE, sizeof(Segment));
    if (hashtable == NULL) {
      printf("** malloc @ hatshtalbe\n");
      return NULL;
    }

    hashtable->buckets =
        (Bucket *)memalign(CACHE_LINE_SIZE, num_buckets * (sizeof(Bucket)));
    if (hashtable->buckets == NULL) {
      printf("** alloc: hashtable->table\n");
      fflush(stdout);
      free(hashtable);
      return NULL;
    }

    size_t i;
    if (ini)
      for (i = 0; i < num_buckets; i++) {
        hashtable->buckets[i].lock = LOCK_FREE;
        hashtable->buckets[i].next = INVALID;
        uint32_t j;
        for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
          hashtable->buckets[i].key[j] = INVALID;
        }
      }

    hashtable->num_buckets = num_buckets;
    hashtable->hash = num_buckets - 1;
    hashtable->snapshot_version = version;
    hashtable->hallocD = new DRAM_MemoryManager(ID, version);
    hashtable->table_new = NULL;
    hashtable->table_prev = NULL;
    hashtable->num_expands = 0;
    hashtable->num_expands_threshold = (CLHT_PERC_EXPANSIONS * num_buckets);
    if (hashtable->num_expands_threshold == 0) {
      hashtable->num_expands_threshold = 1;
    }
    hashtable->is_helper = 1;
    hashtable->helper_done = 0;

    return hashtable;
  }

  /* Hash a key for a particular hash table. */
  size_t clht_hash(Segment *hashtable, size_t key) {
    /* size_t hashval; */
    /* return __ac_Jenkins_hash_64(key) & (hashtable->hash); */
    /* return hashval % hashtable->num_buckets; */
    /* return key % hashtable->num_buckets; */
    /* return key & (hashtable->num_buckets - 1); */
    // auto k = hash_func(key, sizeof(key));
    return key & (hashtable->hash);
  }

  static inline int bucket_exists(volatile Bucket *bucket, size_t key) {
    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] == key) {
          return true;
        }
      }
      bucket = (Bucket *)get_DPage_addr(bucket->next);
    } while (Unlikely(bucket != NULL));
    return false;
  }

} ALIGNED(CACHE_LINE_SIZE);

/**
 * @brief Halo hash table define.
 *
 */
template <typename KEY, typename VALUE>
class Halo {
 public:
  Halo(size_t N) {
    cout << typeid(KEY).name() << " " << typeid(VALUE).name() << endl;
    memset(clhts, 0, TABLE_NUM * sizeof(void *));

    if (SNAPSHOT && filesystem::exists(PM_PATH)) {
      Timer t;
      t.start();

      // Recover memory manager
      auto checkpoints = memory_manager_Pool.startup(clhts);

      // Redo log entries if there was a system crash
      auto redo_log = [](size_t start_page_id, int aid,
                         CLHT *clhts[TABLE_NUM]) {
        for (size_t i = start_page_id; i < MAX_PAGE_NUM; i++) {
          auto addr = PPage_table[i].load();
          if (addr == INVALID) continue;
          auto id = reinterpret_cast<PAGE_METADATA *>(addr)->ALLOCATOR_ID;
          if (aid == id) {
            auto current = addr + PRESERVE_SIZE_EACH_PAGE;
            auto poffset = aid * PAGE_SIZE + PRESERVE_SIZE_EACH_PAGE;
            auto end =
                addr + reinterpret_cast<PAGE_METADATA *>(addr)->LOCAL_OFFSET;
            while (current < end) {
              auto p = reinterpret_cast<Pair_t<KEY, VALUE> *>(current);
              if (p->get_op() == OP_t::INSERT) {
                auto hkey =
                    hash_func(reinterpret_cast<void *>(p->key()), p->klen());
                auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
                clhts[n]->clht_put(hkey, poffset);
              } else if (p->get_op() == OP_t::UPDATE) {
                if constexpr (sizeof(KEY) > 8 || sizeof(VALUE) > 8) {
                  auto hkey =
                      hash_func(reinterpret_cast<void *>(p->key()), p->klen());
                  auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
                  clhts[n]->clht_put_replace(hkey, poffset, p);
                }
              } else if (p->get_op() == OP_t::DELETED) {
                auto hkey =
                    hash_func(reinterpret_cast<void *>(p->key()), p->klen());
                auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
                clhts[n]->clht_remove(hkey);
              }
            }
          }
        }
      };
      if (!ROOT->clean) {
        std::vector<thread> ts;
        for (size_t i = 0; i < CORE_NUM; i++)
          ts.push_back(std::thread(redo_log, checkpoints[i], i, clhts));
        for (auto &&i : ts) i.join();
      }
      // print();
      std::cout << "Recover cost " << t.elapsed<std::chrono::milliseconds>()
                << " ms." << endl;
      ROOT->clean = false;
      pmem_persist(&ROOT->clean, sizeof(ROOT->clean));
    } else {
      memory_manager_Pool.creat();
      cout << "Create new table." << endl;
      auto sz = N / TABLE_NUM;
      cout << sz << " ";
      sz = log2(sz / ENTRIES_PER_BUCKET);
      cout << sz << " ";
      sz = pow(2, sz);
      for (size_t i = 0; i < TABLE_NUM; i++) ROOT->SS[i].SEGMENT_SIZE = sz;
      pmem_persist(ROOT->SS, sizeof(ROOT->SS) * TABLE_NUM);
      cout << sz * TABLE_NUM * ENTRIES_PER_BUCKET << endl;
      for (size_t i = 0; i < TABLE_NUM; i++) {
        clhts[i] = new CLHT(sz, i, 0, true);
      }
    }
    load_factor();
  }
  ~Halo() {
    for (auto &&i : reclaim_threads) i.join();
    memory_manager_Pool.shutdown(clhts);
  }

  bool Insert(Pair_t<KEY, VALUE> &p, int *r) {
    if (Unlikely(mmanager.ID == -1))
      memory_manager_Pool.get_PM_MemoryManager(&mmanager);
    auto hkey = hash_func(reinterpret_cast<void *>(p.key()), p.klen());
    auto addr = get_PM_addr(hkey);
    // check if the key exists
    if (addr != nullptr) {
      *r = EXIST;
      WRITE_PASS_COUNT++;
      if (WRITE_PASS_COUNT + WRITE_BUFFER_COUNTER > MAX_PAIR) {
        do_insert_now(nullptr);
        return true;
      }
      return false;
    } else {
      p.set_op(INSERT);
      INSERT_RESULT_POINTER[WRITE_BUFFER_COUNTER++] = r;
      auto sz = p.size();
      auto tsz = WRITE_BUFFER_SIZE + sz;
      auto threashold = MAX_BATCHING_SIZE;
      if (tsz >= MAX_BATCHING_SIZE && tsz <= MAX_WRITE_BUFFER_SIZE ||
          tsz < MAX_BATCHING_SIZE) {
        p.store(WRITE_BUFFER + WRITE_BUFFER_SIZE);
        WRITE_BUFFER_SIZE = tsz;
        if (tsz < threashold) return false;
      }
      void *ptr = nullptr;
      // current pair is a big pair that does not need to batch
      if (tsz > MAX_WRITE_BUFFER_SIZE) ptr = &p;
      // if ptr is not nullptr, the ptr point to a big pair.
      do_insert_now(ptr);
      return true;
    }
  }
  bool Update(Pair_t<KEY, VALUE> &p, int *r) {
    if (Unlikely(mmanager.ID == -1))
      memory_manager_Pool.get_PM_MemoryManager(&mmanager);
    p.set_op(OP_t::UPDATE);
    auto hkey = hash_func(reinterpret_cast<void *>(p.key()), p.klen());
    auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
    auto sz = clhts[n]->clht_put_replace(hkey, &p);
    READ_LOCK();
    if (!sz.first) {
      return false;
    }
    return true;
  }
  bool Get(Pair_t<KEY, VALUE> *p) {
    if (Unlikely(READ_BUFFER_SIZE == 1)) {
      auto hkey = hash_func(reinterpret_cast<void *>(p->key()), p->klen());
      auto addr = get_PM_addr(hkey);
      if (addr) {
        p->load(addr);
      }
      READ_LOCK();
      return true;
    }
    BUFFER_READ[BUFFER_READ_COUNTER++] = p;
    if (BUFFER_READ_COUNTER == READ_BUFFER_SIZE) {
      Gets();
      READ_LOCK();
      return true;
    }
    READ_LOCK();
    return false;
  }
  void get_all() { Gets(); }
  bool Delete(Pair_t<KEY, VALUE> &p) {
    auto hkey = hash_func(reinterpret_cast<void *>(p.key()), p.klen());
    auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
    auto offset = clhts[n]->clht_get(hkey).first;
    if (offset == INVALID) return false;
    if (Unlikely(mmanager.ID == -1))
      memory_manager_Pool.get_PM_MemoryManager(&mmanager);
    auto sz = clhts[n]->clht_remove(hkey, &p);
    if (!sz.first) return false;
    reclaim_ppage(sz.second, sz.first);
    READ_LOCK();
    return true;
  }
  void load_factor() {
    // size_t total_slot = 0;
    // size_t used_slot = 0;
    // size_t b_m = 0;
    // for (size_t i = 0; i < TABLE_NUM; i++) {
    //   auto c = clhts[i];
    //   auto t = c->table;
    //   auto n = t->num_buckets;
    //   b_m += n * sizeof(Bucket);
    //   for (size_t j = 0; j < n; j++) {
    //     auto b = t->buckets + j;
    //     while (b != nullptr) {
    //       for (size_t k = 0; k < ENTRIES_PER_BUCKET; k++) {
    //         if (b->key[k] != INVALID) used_slot++;
    //         total_slot++;
    //       }
    //       b = (Bucket *)get_DPage_addr(b->next);
    //     }
    //   }
    // }

    // cout << "total_slot: " << total_slot << " used_slot: " << used_slot << "
    // "
    //      << (float)used_slot * 100 / total_slot << "%\n";
    // printf("DRAM Allocated by malloc: %.1f GB.\n",
    //        float(b_m) / 1024 / 1024 / 1024);
    // memory_manager_Pool.info();
  }

  void wait_all() { do_insert_now(); }
  void reclaim_ppage(size_t page_id, size_t sz_freed) {
    if (!LOGCLEAN) return;

    if (sz_freed * 100 / PAGE_SIZE < CLEAN_THRESHOLD) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(reclaim_mtx);
      if (ppages_reclaiming.count(page_id))
        return;
      else
        ppages_reclaiming.insert(page_id);
    }
    if (memory_manager_Pool.is_in_allocating(page_id)) {
      return;
    }
    std::lock_guard<std::mutex> lock(thread_mutex);
    reclaim_threads.push_back(std::thread(
        [](size_t page_id, CLHT *clhts[TABLE_NUM], mutex &RECLAIM_MTX,
           mutex &reclaim_mtx, std::set<size_t> &ppages_reclaiming) {
          memory_manager_Pool.get_PM_MemoryManager(&mmanager, false);
          auto addr = PPage_table[page_id].load() + PRESERVE_SIZE_EACH_PAGE;
          auto offset = page_id * PAGE_SIZE + PRESERVE_SIZE_EACH_PAGE;
          auto end =
              PPage_table[page_id].load() +
              reinterpret_cast<PAGE_METADATA *>(PPage_table[page_id].load())
                  ->LOCAL_OFFSET;
          map<size_t, size_t> key_newoffset;
          size_t *reclaimed =
              &reinterpret_cast<PAGE_METADATA *>(addr)->RECLAIMED;
          while (addr < end) {
            auto p = reinterpret_cast<Pair_t<KEY, VALUE> *>(addr);
            if (p->get_op() != TRASH) {
              auto hkey =
                  hash_func(reinterpret_cast<void *>(p->key()), p->klen());
              auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
              clhts[n]->clht_put_move(hkey, mmanager, p, offset, reclaimed);
            }
            addr += p->size();
            offset += p->size();
          }
          {
            lock_guard<mutex> guard_reclaim(RECLAIM_MTX);
            RECLAIM.store(true);
            PPage_table[page_id].store(INVALID);
            WAIT_READ_LOCK();
            RECLAIM.store(false);
            RELEASE_READ_LOCK();
            mmanager.delete_pm_file(page_id);
            cout << page_id << " reallocated." << endl;
          }
          {
            std::lock_guard<std::mutex> lock(reclaim_mtx);
            ppages_reclaiming.erase(page_id);
          }
        },
        page_id, clhts, std::ref(RECLAIM_MTX), std::ref(reclaim_mtx),
        std::ref(ppages_reclaiming)));
  }

 private:
  void Gets() {
    // prefetch
    char *addrs[READ_BUFFER_SIZE];
    for (size_t i = 0; i < READ_BUFFER_SIZE; i++) {
      auto p = reinterpret_cast<Pair_t<KEY, VALUE> *>(BUFFER_READ[i]);
      auto hkey = hash_func(reinterpret_cast<void *>(p->key()), p->klen());
      addrs[i] = get_PM_addr(hkey);
      if (addrs[i])
        _mm_prefetch(reinterpret_cast<char *>(addrs[i]), _MM_HINT_NTA);
      else
        p->set_empty();
    }
    // load
    for (size_t i = 0; i < READ_BUFFER_SIZE; i++) {
      if (!addrs[i]) {
        continue;
      }
      auto r = reinterpret_cast<Pair_t<KEY, VALUE> *>(BUFFER_READ[i]);
      auto p = reinterpret_cast<Pair_t<KEY, VALUE> *>(addrs[i]);
      // cout << r->str_key() << "!" << p->str_key() << " " << p->value() <<
      // endl;
      if (r->str_key() != p->str_key()) cout << "ERROR!" << endl;
      r->load(addrs[i]);
      if (r->get_op() == OP_t::DELETED) r->set_empty();
    }
    BUFFER_READ_COUNTER = 0;
  }

  char *get_PM_addr(size_t hkey) {
    auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
    auto offset = clhts[n]->clht_get(hkey).first;
    if (offset == INVALID) return nullptr;
    auto page_index = offset / PAGE_SIZE;
    return reinterpret_cast<char *>(PPage_table[page_index].load() +
                                    offset % PAGE_SIZE);
  }
  void do_insert_now(void *ptr = nullptr) {
    if (!WRITE_BUFFER_COUNTER) return;
    auto len = WRITE_BUFFER_SIZE;
    auto &pm = mmanager;
    auto offset_and_addr = pm.halloc(len);
    auto offset = offset_and_addr.first;
    auto addr = offset_and_addr.second;
    auto add = reinterpret_cast<long long *>(WRITE_BUFFER);
    auto paddr = reinterpret_cast<long long *>(addr);
    auto end = len / 8;
    for (size_t i = 0; i < end; i++) {
      _mm_stream_si64(paddr + i, add[i]);
    }
    auto bigoffset = 0ULL;
    // write big pair to PM
    if (ptr) {
      auto bigpair = reinterpret_cast<Pair_t<KEY, VALUE> *>(ptr);
      auto add = reinterpret_cast<long long *>(ptr);
      auto o_and_a = pm.halloc(bigpair->size());
      bigoffset = o_and_a.first;
      auto paddr = reinterpret_cast<long long *>(o_and_a.second);
      pmem_memcpy_persist(paddr, add, bigpair->size());
    }
    pmem_drain();
    pm.update_metadata();
    pmem_drain();
    offset = offset_and_addr.first;
    bool resizing_flag_local = false;
    auto pair_addr = WRITE_BUFFER;
    // remove the count of big pair. The big pair is handled later.
    if (ptr) WRITE_BUFFER_COUNTER--;
    // insert key and offset into hash table
    for (size_t i = 0; i < WRITE_BUFFER_COUNTER; i++) {
      auto p = Pair_t<KEY, VALUE>(pair_addr);
      auto hkey = hash_func(reinterpret_cast<void *>(p.key()), p.klen());
      auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
      clhts[n]->clht_put(hkey, offset);
      offset += p.size();
      pair_addr += p.size();
    }
    // insert big pair into hash table.
    if (ptr) {
      auto p = reinterpret_cast<Pair_t<KEY, VALUE> *>(ptr);
      auto hkey = hash_func(reinterpret_cast<void *>(p->key()), p->klen());
      auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
      clhts[n]->clht_put(hkey, bigoffset);
    }
    if (ptr) WRITE_BUFFER_COUNTER++;
    // write back the result.
    for (size_t i = 0; i < WRITE_BUFFER_COUNTER; i++) {
      *INSERT_RESULT_POINTER[i] = DONE;
    }
    WRITE_BUFFER_SIZE = 0;
    WRITE_BUFFER_COUNTER = 0;
    WRITE_PASS_COUNT = 0;
    READ_LOCK();
  }
  void restore_to_table(Pair_t<KEY, VALUE> p, size_t offset) {
    auto hkey = hash_func(reinterpret_cast<void *>(p.key()), p.klen());
    auto op = p.op;
    if (op == INSERT) {
      auto n = GET_CLHT_INDEX(hkey, TABLE_NUM);
      clhts[n]->clht_put(hkey, offset);
    } else if (op == DELETED) {
      Pair_t<KEY, VALUE> *np = nullptr;
      clhts[GET_CLHT_INDEX(hkey, TABLE_NUM)]->clht_remove(hkey, np, nullptr);
    }
  }
  CLHT *clhts[TABLE_NUM];
  mutex reclaim_mtx;
  std::set<size_t> ppages_reclaiming;
  std::unordered_set<size_t> ppages_to_reclaiming;
};
}  // namespace HALO