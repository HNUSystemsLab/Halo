#include <inttypes.h>
#include <libvmem.h>
#include <stdio.h>
#include <stdlib.h>

#include <cstring>

#include "atomic_ops.h"
namespace PCLHT {

/* #define DEBUG */
extern VMEM *vmp;
#define CLHT_USE_RTM 0

#define CLHT_CHECK_STATUS(h)

#if CLHT_DO_GC == 1
#define CLHT_GC_HT_VERSION_USED(ht) \
  clht_gc_thread_version((clht_hashtable_t *)ht)
#else
#define CLHT_GC_HT_VERSION_USED(ht)
#endif

/* CLHT LINKED version specific parameters */
#define CLHT_LINKED_PERC_FULL_DOUBLE 75
#define CLHT_LINKED_MAX_AVG_EXPANSION 1
#define CLHT_LINKED_MAX_EXPANSIONS 7
#define CLHT_LINKED_MAX_EXPANSIONS_HARD 16
#define CLHT_LINKED_EMERGENCY_RESIZE \
  4 /* how many times to increase the size on emergency */
/* *************************************** */

#if defined(DEBUG)
#define DPP(x) x++
#else
#define DPP(x)
#endif

#define CACHE_LINE_SIZE 64
#define ENTRIES_PER_BUCKET 3

#ifndef ALIGNED
#if __GNUC__ && !SCC
#define ALIGNED(N) __attribute__((aligned(N)))
#else
#define ALIGNED(N)
#endif
#endif

#define CAS_U64_BOOL(a, b, c) (CAS_U64(a, b, c) == b)
typedef uintptr_t clht_addr_t;
typedef volatile uintptr_t clht_val_t;

typedef volatile uint8_t clht_lock_t;

static inline uint64_t pow2roundup(uint64_t x) {
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
typedef struct ALIGNED(CACHE_LINE_SIZE) bucket_s {
  clht_lock_t lock;
  volatile uint32_t hops;
  clht_addr_t key[ENTRIES_PER_BUCKET];
  clht_val_t val[ENTRIES_PER_BUCKET];
  volatile struct bucket_s *next;
} bucket_t;

typedef struct ALIGNED(CACHE_LINE_SIZE) clht {
  union {
    struct {
      struct clht_hashtable_s *ht;
      uint8_t next_cache_line[CACHE_LINE_SIZE - (sizeof(void *))];
      struct clht_hashtable_s *ht_oldest;
      struct ht_ts *version_list;
      size_t version_min;
      volatile clht_lock_t resize_lock;
      volatile clht_lock_t gc_lock;
      volatile clht_lock_t status_lock;
    };
    uint8_t padding[2 * CACHE_LINE_SIZE];
  };
} clht_t;

typedef struct ALIGNED(CACHE_LINE_SIZE) clht_hashtable_s {
  union {
    struct {
      size_t num_buckets;
      bucket_t *table;
      size_t hash;
      size_t version;
      uint8_t next_cache_line[CACHE_LINE_SIZE - (3 * sizeof(size_t)) -
                              (sizeof(void *))];
      struct clht_hashtable_s *table_tmp;
      struct clht_hashtable_s *table_prev;
      struct clht_hashtable_s *table_new;
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
} clht_hashtable_t;

typedef struct ALIGNED(CACHE_LINE_SIZE) ht_ts {
  union {
    struct {
      size_t version;
      clht_hashtable_t *versionp;
      int id;
      volatile struct ht_ts *next;
    };
    uint8_t padding[CACHE_LINE_SIZE];
  };
} ht_ts_t;

extern uint64_t __ac_Jenkins_hash_64(uint64_t key);

/* Hash a key for a particular hashtable. */
uint64_t clht_hash(clht_hashtable_t *hashtable, clht_addr_t key);

static inline void _mm_pause_rep(uint64_t w) {
  while (w--) {
    _mm_pause();
  }
}

#define TAS_RLS_MFENCE() _mm_sfence();

#define LOCK_FREE 0
#define LOCK_UPDATE 1
#define LOCK_RESIZE 2

#define LOCK_ACQ(lock, ht) lock_acq_chk_resize(lock, ht)

#define LOCK_RLS(lock) \
  TAS_RLS_MFENCE();    \
  *lock = 0;

#define LOCK_ACQ_RES(lock) lock_acq_resize(lock)

#define TRYLOCK_ACQ(lock) TAS_U8(lock)

#define TRYLOCK_RLS(lock) lock = LOCK_FREE

void ht_resize_help(clht_hashtable_t *h);

#if defined(DEBUG)
extern __thread uint32_t put_num_restarts;
#endif

static inline int lock_acq_chk_resize(clht_lock_t *lock, clht_hashtable_t *h) {
  char once = 1;
  clht_lock_t l;
  while ((l = CAS_U8(lock, LOCK_FREE, LOCK_UPDATE)) == LOCK_UPDATE) {
    if (once) {
      DPP(put_num_restarts);
      once = 0;
    }
    _mm_pause();
  }

  if (l == LOCK_RESIZE) {
    /* helping with the resize */
#if CLHT_HELP_RESIZE == 1
    ht_resize_help(h);
#endif

    while (h->table_new == NULL) {
      _mm_pause();
      _mm_mfence();
    }

    return 0;
  }

  return 1;
}

static inline int lock_acq_resize(clht_lock_t *lock) {
  clht_lock_t l;
  while ((l = CAS_U8(lock, LOCK_FREE, LOCK_RESIZE)) == LOCK_UPDATE) {
    _mm_pause();
  }

  if (l == LOCK_RESIZE) {
    return 0;
  }

  return 1;
}

/* ********************************************************************************
 */
/* intefance */
/* ********************************************************************************
 */

/* Create a new hashtable. */
clht_hashtable_t *clht_hashtable_create(uint64_t num_buckets);
clht_t *clht_create(uint64_t num_buckets);

/* Insert a key-value pair into a hashtable. */
int clht_put(clht_t *hashtable, clht_addr_t key, clht_val_t val);

/* Retrieve a key-value pair from a hashtable. */
clht_val_t clht_get(clht_t *h, clht_addr_t key);
/* Remove a key-value pair from a hashtable. */
clht_val_t clht_remove(clht_t *hashtable, clht_addr_t key);

size_t clht_size(clht_hashtable_t *hashtable);
size_t clht_size_mem(clht_hashtable_t *hashtable);
size_t clht_size_mem_garbage(clht_hashtable_t *hashtable);
bool clht_recovery();
void clht_print(clht_hashtable_t *hashtable);

size_t ht_status(clht_t *hashtable, int resize_increase, int just_print);

bucket_t *clht_bucket_create();
int ht_resize_pes(clht_t *hashtable, int is_increase, int by);

const char *clht_type_desc();

void clht_lock_initialization(clht_t *h);
}  // namespace PCLHT