#pragma once
#include <iostream>
#include <string>
namespace HALO {
#define ROUND_UP(s, n) (((s) + (n)-1) & (~(n - 1)))
enum OP_t { TRASH, INSERT, DELETED, UPDATE };
const size_t INVALID = UINT64_MAX;
const size_t MAX_VALUE_LEN = 512;
const size_t MAX_KEY_LEN = 512;
using OP_VERSION = uint32_t;
constexpr int OP_BITS = 2;
constexpr int VERSION_BITS = sizeof(OP_VERSION) * 8 - OP_BITS;
#pragma pack(1)
template <typename KEY, typename VALUE>
class Pair_t {
 public:
  OP_VERSION op : OP_BITS;
  OP_VERSION version : VERSION_BITS;
  KEY _key;
  VALUE _value;
  Pair_t() {
    _key = 0;
    _value = 0;
  };
  Pair_t(char *p) {
    auto pt = reinterpret_cast<Pair_t<KEY, VALUE> *>(p);
    *this = *pt;
  }
  void load(char *p) {
    auto pt = reinterpret_cast<Pair_t<KEY, VALUE> *>(p);
    *this = *pt;
  }
  KEY *key() { return &_key; }
  KEY str_key() { return _key; }
  VALUE value() { return _value; }
  size_t klen() { return sizeof(KEY); }
  Pair_t(KEY k, VALUE v) {
    _key = k;
    _value = v;
  }
  void set_key(KEY k) { _key = k; }

  void store_persist(void *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = size();
    memcpy(addr, p, len);
    pmem_persist(addr, len);
  }
  void store_persist_update(char *addr) { store_persist(addr); }
  void store(void *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = size();
    memcpy(addr, p, len);
  }
  void set_empty() {
    _key = 0;
    _value = 0;
  }
  void set_version(uint64_t old_version) { version = old_version + 1; }
  void set_op(OP_t o) { op = static_cast<uint16_t>(o); }
  OP_t get_op() { return static_cast<OP_t>(op); }
  void set_op_persist(OP_t o) {
    op = static_cast<uint16_t>(o);
    pmem_persist(reinterpret_cast<char *>(this), 8);
  }
  // friend std::ostream &operator<<(std::ostream &out, Pair_t A);
  size_t size() {
    auto total_length = sizeof(OP_VERSION) + sizeof(KEY) + sizeof(VALUE);
    return total_length;
  }
};
template <typename KEY>
class Pair_t<KEY, std::string> {
 public:
  OP_VERSION op : OP_BITS;
  OP_VERSION version : VERSION_BITS;
  KEY _key;
  uint32_t _vlen;
  std::string svalue;
  Pair_t() {
    op = 0;
    version = 0;
    _vlen = 0;
    svalue.reserve(MAX_VALUE_LEN);
  };
  Pair_t(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    _key = pt->_key;
    op = pt->op;
    version = pt->version;
    _vlen = pt->_vlen;
    svalue.assign(p + sizeof(KEY) + sizeof(uint32_t) + sizeof(OP_VERSION),
                  _vlen);
  }
  void load(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    _key = pt->_key;
    op = pt->op;
    version = pt->version;
    _vlen = pt->_vlen;
    svalue.assign(p + sizeof(KEY) + sizeof(uint32_t) + sizeof(OP_VERSION),
                  _vlen);
  }
  size_t klen() { return sizeof(KEY); }
  KEY *key() { return &_key; }
  KEY str_key() { return _key; }
  std::string str_value() { return svalue; }

  Pair_t(KEY k, char *v_ptr, size_t vlen) : _vlen(vlen), version(0) {
    _key = k;
    svalue.assign(v_ptr, _vlen);
  }
  void set_key(KEY k) { _key = k; }
  void store_persist(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(KEY) + sizeof(uint32_t) + sizeof(OP_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &svalue[0], _vlen);
    pmem_persist(addr, len + _vlen);
  }
  void store_persist_update(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(KEY) + sizeof(uint32_t) + sizeof(OP_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &svalue[0], _vlen);
    reinterpret_cast<Pair_t<KEY, std::string> *>(addr)->set_version(version);
    pmem_persist(addr, len + _vlen);
  }
  void store(char *addr) {
    auto p = reinterpret_cast<char *>(this);
    auto len = sizeof(KEY) + sizeof(uint32_t) + sizeof(OP_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &svalue[0], _vlen);
  }
  void set_empty() { _vlen = 0; }
  void set_version(uint64_t old_version) { version = old_version + 1; }
  void set_op(OP_t o) { op = static_cast<uint16_t>(o); }
  OP_t get_op() { return op; }
  void set_op_persist(OP_t o) {
    op = static_cast<uint16_t>(o);
    pmem_persist(reinterpret_cast<char *>(this), 8);
  }
  size_t size() {
    auto total_length =
        sizeof(KEY) + sizeof(uint32_t) + sizeof(OP_VERSION) + _vlen;
    return total_length;
  }
};

template <>
class Pair_t<std::string, std::string> {
 public:
  OP_VERSION op : OP_BITS;
  OP_VERSION version : VERSION_BITS;
  uint32_t _klen;
  uint32_t _vlen;
  std::string skey;
  std::string svalue;
  Pair_t() {
    op = 0;
    version = 0;
    _klen = 0;
    _vlen = 0;
    skey.reserve(MAX_KEY_LEN);
    svalue.reserve(MAX_VALUE_LEN);
  };
  Pair_t(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    op = pt->op;
    version = pt->version;
    _klen = pt->_klen;
    _vlen = pt->_vlen;
    skey.assign(p + 6, _klen);
    svalue.assign(p + 6 + _klen, _vlen);
  }
  void load(char *p) {
    auto pt = reinterpret_cast<Pair_t *>(p);
    op = pt->op;
    version = pt->version;
    _klen = pt->_klen;
    _vlen = pt->_vlen;
    skey.assign(p + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(OP_VERSION),
                _klen);
    svalue.assign(
        p + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(OP_VERSION) + _klen,
        _vlen);
  }
  size_t klen() { return _klen; }
  char *key() { return &skey[0]; }
  std::string str_key() { return skey; }
  std::string value() { return svalue; }

  Pair_t(char *k_ptr, size_t klen, char *v_ptr, size_t vlen)
      : _klen(klen), _vlen(vlen), version(0) {
    skey.assign(k_ptr, _klen);
    svalue.assign(v_ptr, _vlen);
  }
  void set_key(char *k_ptr, size_t kl) {
    _klen = kl;
    skey.assign(k_ptr, _klen);
  }
  void store_persist(char *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(OP_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &skey[0], _klen);
    memcpy(addr + len + _klen, &svalue[0], _vlen);
    pmem_persist(addr, len + _klen + _vlen);
  }
  void store_persist_update(char *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(OP_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &skey[0], _klen);
    memcpy(addr + len + _klen, &svalue[0], _vlen);
    reinterpret_cast<Pair_t<std::string, std::string> *>(addr)->set_version(
        version);
    pmem_persist(addr, len + _klen + _vlen);
  }
  void store(char *addr) {
    auto p = reinterpret_cast<void *>(this);
    auto len = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(OP_VERSION);
    memcpy(addr, p, len);
    memcpy(addr + len, &skey[0], _klen);
    memcpy(addr + len + _klen, &svalue[0], _vlen);
  }
  void set_empty() {
    _klen = 0;
    _vlen = 0;
  }
  void set_version(uint64_t old_version) { version = old_version + 1; }
  void set_op(OP_t o) { op = static_cast<uint16_t>(o); }
  OP_t get_op() { return static_cast<OP_t>(op); }
  void set_op_persist(OP_t o) {
    op = static_cast<uint16_t>(o);
    pmem_persist(reinterpret_cast<char *>(this), 8);
  }

  size_t size() {
    auto total_length = sizeof(uint32_t) + sizeof(uint32_t) +
                        sizeof(OP_VERSION) + _klen + _vlen;
    return total_length;
  }
};
#pragma pack()
}  // namespace HALO