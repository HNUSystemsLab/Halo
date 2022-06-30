
#include <iostream>

#include "SOFTList.h"
#include "timer.h"
using namespace std;
constexpr size_t BUCKET_NUM = 16 * 1024 * 1024;
class SOFT {
 public:
  SOFT() {
    thread_ini(1);
    table = new SOFTList<size_t>[BUCKET_NUM];
  }
  std::string hash_name() { return "SOFT"; };
  virtual bool insert(size_t key) {
    SOFTList<size_t> &bucket = getBucket(key);
    return bucket.insert(key, key);
  }
  void thread_ini(int id) {
    init_alloc(id);
    init_volatileAlloc(id);
  }

  bool find(size_t key) {
    SOFTList<size_t> &bucket = getBucket(key);
    return bucket.contains(key);
  }

  bool erase(size_t key, int tid) {
    SOFTList<size_t> &bucket = getBucket(key);
    return bucket.remove(key);
  }
  void SOFTrecovery() {
    table = new SOFTList<size_t>[BUCKET_NUM];
    auto curr = alloc->mem_chunks;
    for (; curr != nullptr; curr = curr->next) {
      PNode<size_t> *currChunk = static_cast<PNode<size_t> *>(curr->obj);
      uint64_t numOfNodes = SSMEM_DEFAULT_MEM_SIZE / sizeof(PNode<size_t>);
      for (uint64_t i = 0; i < numOfNodes; i++) {
        PNode<size_t> *currNode = currChunk + i;
        // if (currNode->key == 0) continue;
        if (currNode->isDeleted()) {
          // currNode->validStart = currNode->validEnd.load();
          // ssmem_free(alloc, currNode, true);
        } else {
          bool pValid = currNode->recoveryValidity();
          uintptr_t key = currNode->key;
          SOFTList<size_t> &bucket = getBucket(key);
          bucket.quickInsert(currNode, pValid);
        }
      }
    }
  }

 private:
  static size_t h(size_t k, size_t _len = 8,
                  size_t _seed = static_cast<size_t>(0xc70f6907UL)) {
    auto _ptr = reinterpret_cast<void *>(&k);
    return std::_Hash_bytes(_ptr, _len, _seed);
  }
  SOFTList<size_t> &getBucket(uintptr_t k) { return table[h(k) % BUCKET_NUM]; }
  SOFTList<size_t> *table;
};
int main() {
  SOFT s;
  for (size_t i = 0; i < 1000000000ULL; i++) {
    s.insert(i);
    if (i % 10000000 == 0) cout << i << endl;
  }
  Timer t;
  t.start();
  s.SOFTrecovery();
  cout << t.elapsed<chrono::milliseconds>() << endl;
  return 0;
}