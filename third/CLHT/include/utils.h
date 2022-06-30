#ifndef _UTILS_H_INCLUDED_
#define _UTILS_H_INCLUDED_
// some utility functions
//#define _GNU_SOURCE
//#define USE_MUTEX_LOCKS
//#define ADD_PADDING
//#define OPTERON
//#define OPTERON_OPTIMIZE

#include <errno.h>
#include <inttypes.h>
#include <malloc.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#ifdef __sparc__
#include <sys/processor.h>
#include <sys/procset.h>
#include <sys/types.h>
#elif defined(__tile__)
#include <arch/atomic.h>
#include <arch/cycle.h>
#include <sched.h>
#include <tmc/cpus.h>
#include <tmc/spin.h>
#include <tmc/task.h>
#else
#include <emmintrin.h>
#include <xmmintrin.h>
#ifdef NUMA
#include <numa.h>
#endif
#endif
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ALIGNED(N) __attribute__((aligned(N)))

#define PAUSE _mm_pause()

static inline void pause_rep(uint32_t num_reps) {
  uint32_t i;
  for (i = 0; i < num_reps; i++) {
    PAUSE;
    /* PAUSE; */
    /* asm volatile ("NOP"); */
  }
}

static inline void nop_rep(uint32_t num_reps) {
  uint32_t i;
  for (i = 0; i < num_reps; i++) {
    asm volatile("NOP");
  }
}

/// Round up to next higher power of 2 (return x if it's already a power
/// of 2) for 32-bit numbers
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

#ifdef __cplusplus
}

#endif

#endif
