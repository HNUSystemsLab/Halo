#include "key_generator.hpp"
thread_local std::default_random_engine key_generator_t::generator_;
thread_local uint32_t key_generator_t::seed_;
thread_local char key_generator_t::buf_[KEY_MAX];
thread_local uint64_t key_generator_t::current_id_ = -1ULL / 2;
/**
 * @brief Calculate multiplicative hash of integer in the same domain.
 *
 * This was adapted from:
 *
 * ""The Art of Computer Programming, Volume 3, Sorting and Searching",
 * D.E. Knuth, 6.4 p:516"
 *
 * The function should be used as a cheap way to scramble integers in the
 * domain [0,2^T] to integers in the same domain [0,2^T]. If the resulting
 * hash should be in a smaller domain [0,2^m], the result should be right
 * shifted by X bits where X = (32 | 64) - m.
 *
 * @tparam T type of input and output (uint32_t or uint64_t).
 * @param x integer to be hashed.
 * @return T hashed result.
 */
template <typename T>
static T multiplicative_hash(T x) {
  static_assert(
      std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
      "multiplicative hash only supports 32 bits and 64 bits variants.");

  static constexpr T A =
      std::is_same<T, uint32_t>::value ? 2654435761u : 11400714819323198393ul;
  return A * x;
}
/**
 * @brief Verify endianess during runtime.
 *
 * @return true
 * @return false
 */
static bool is_big_endian(void) {
  volatile union {
    uint32_t i;
    char c[4];
  } bint = {0x01020304};

  return bint.c[0] == 1;
}

key_generator_t::key_generator_t(size_t N, size_t size) : N_(N), size_(size) {
  memset(buf_, 0, KEY_MAX);
}

const char *key_generator_t::next(bool in_sequence) {
  char *ptr = buf_;

  uint64_t id = next_id();
  id = in_sequence ? current_id_ + id : id;
  uint64_t hashed_id = multiplicative_hash<uint64_t>(id);

  if (size_ < sizeof(hashed_id)) {
    // We want key smaller than 8 Bytes, so discard higher bits.
    auto bits_to_shift = (sizeof(hashed_id) - size_) << 3;

    // Discard high order bits
    if (is_big_endian()) {
      hashed_id >>= bits_to_shift;
      hashed_id <<= bits_to_shift;
    } else {
      hashed_id <<= bits_to_shift;
      hashed_id >>= bits_to_shift;
    }

    memcpy(ptr, &hashed_id,
           size_);  // TODO: check if must change to fit endianess
  } else {
    // TODO: change this, otherwise zeroes act as prefix
    // We want key of at least 8 Bytes, check if we must prepend zeroes
    auto bytes_to_prepend = size_ - sizeof(hashed_id);
    if (bytes_to_prepend > 0) {
      memset(ptr, 0, bytes_to_prepend);
      ptr += bytes_to_prepend;
    }
    memcpy(ptr, &hashed_id, sizeof(hashed_id));
  }
  return buf_;
}
