#include <inttypes.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <string>
#include <thread>

#include "Halo.hpp"
#define NONVAR 1
using namespace std;
using namespace HALO;
int n = 32;
int main(int argc, char *argv[]) {
  PM_PATH = "/mnt/pmem/Halo/";

#ifdef NONVAR
  Halo<size_t, size_t> halo(32);
#elif VARVALUE
  Halo<size_t, std::string> halo(1024);
#else
  Halo<std::string, std::string> halo(1024);
#endif
  std::cout << "Halo start." << std::endl;
  int *r = new int[n];
#ifdef NONVAR
  Pair_t<size_t, size_t> *rp = new Pair_t<size_t, size_t>[n];
  Pair_t<size_t, size_t> *rp2 = new Pair_t<size_t, size_t>[n];
#elif VARVALUE
  Pair_t<size_t, std::string> *rp = new Pair_t<size_t, std::string>[n];
#else
  Pair_t<std::string, std::string> *rp =
      new Pair_t<std::string, std::string>[n];
#endif
  for (size_t i = 0; i < n; i++) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p(i, i);
#elif VARVALUE
    Pair_t<size_t, std::string> p(i, reinterpret_cast<char *>(&i), 8);
#else
    Pair_t<std::string, std::string> p(reinterpret_cast<char *>(&i), 8,
                                       reinterpret_cast<char *>(&i), 8);
#endif
    halo.Insert(p, &r[i]);
  }
  halo.wait_all();
  cout << "inserted." << endl;
  halo.load_factor();
  for (size_t i = 0; i < n; i++) {
#ifdef NONVAR
    rp[i].set_key(i);
#elif VARVALUE
    rp[i].set_key(i);
#else
    rp[i].set_key(reinterpret_cast<char *>(&i), 8);
#endif

    halo.Get(&rp[i]);
  }
  halo.get_all();
  for (size_t i = 0; i < n; i++) {
    cout << *reinterpret_cast<size_t *>(rp[i].key()) << "-" << rp[i].value()
         << endl;
  }
  for (size_t i = 0; i < n; i++) {
#ifdef NONVAR
    Pair_t<size_t, size_t> p(i, rp[i].value() + 1);
#elif VARVALUE
    Pair_t<size_t, std::string> p(i, reinterpret_cast<char *>(&i), 8);
#else
    Pair_t<std::string, std::string> p(reinterpret_cast<char *>(&i), 8,
                                       reinterpret_cast<char *>(&i), 8);
#endif
    halo.Update(p, &r[i]);
  }
  for (size_t i = 0; i < n; i++) {
#ifdef NONVAR
    rp2[i].set_key(i);
#elif VARVALUE
    rp2[i].set_key(i);
#else
    rp2[i].set_key(reinterpret_cast<char *>(&i), 8);
#endif
    halo.Get(&rp2[i]);
  }
  halo.get_all();
  for (size_t i = 0; i < n; i++) {
    cout << *reinterpret_cast<size_t *>(rp2[i].key()) << "-" << rp2[i].value()
         << endl;
  }
  return 0;
}