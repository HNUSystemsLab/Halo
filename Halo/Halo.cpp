#include <immintrin.h>
#include <libpmem.h>
#include <sched.h>
#include <xmmintrin.h>

#include <algorithm>
#include <map>

#include "Halo.hpp"

namespace HALO {

#define ROUND_UP(s, n) (((s) + (n)-1) & (~(n - 1)))

MemoryManagerPool memory_manager_Pool;
string PM_PATH;
mutex PM_MemoryManager::mtx;
mutex DRAM_MemoryManager::mtx;
mutex MemoryManagerPool::mtx_pm_pool;
thread_local PM_MemoryManager mmanager;
thread_local int *INSERT_RESULT_POINTER[MAX_BUFFER_PAIR_SIZE];
thread_local size_t WRITE_BUFFER_COUNTER(0);
thread_local char WRITE_BUFFER[MAX_WRITE_BUFFER_SIZE];
thread_local size_t WRITE_BUFFER_SIZE(0);
thread_local size_t WRITE_PASS_COUNT(0);
thread_local void *BUFFER_READ[READ_BUFFER_SIZE];
thread_local size_t BUFFER_READ_COUNTER(0);

size_t PM_MemoryManager::PAGE_ID = 0;
size_t DRAM_MemoryManager::PAGE_ID = 0;

// ===========For reclaim=================
std::mutex RECLAIM_MTX;
atomic_bool RECLAIM(false);
bool RECLAIM_LOCK[CORE_NUM];
std::vector<thread> reclaim_threads;
std::mutex thread_mutex;

root *ROOT;
atomic_size_t PPage_table[MAX_PAGE_NUM];
char *DPage_table[MAX_PAGE_NUM];
enum PM_FILE_NAME {
  SEGMENT_SNAPSHOT = 's',
  DPAGE_SNAPSHOT = 'd',
  PPAGE = 'P',
};
std::mutex PM_TABLE_UPDATE_RECLAIM_MUTEX[MAX_PAGE_NUM];
std::string generate_filename(PM_FILE_NAME type, size_t snapshot_version,
                              size_t aid, size_t pid = -1ULL) {
  auto s = PM_PATH + std::string(1, type) + "_" + to_string(snapshot_version) +
           "_" + to_string(aid);
  if (pid != -1ULL)
    return s + "_" + to_string(pid);
  else
    return s;
}
std::vector<size_t> nphase() {
  std::vector<size_t> checkpoint(CORE_NUM);
  for (size_t i = 0; i < CORE_NUM; i++) {
    checkpoint[i] = ROOT->CURRENT_PPAGE_ID[i];
  }
  return checkpoint;
}
char *get_DPage_addr(size_t offset) {
  if (offset != INVALID) {
    auto addr = DPage_table[offset / PAGE_SIZE];
    if (Unlikely(addr == nullptr)) assert(addr == nullptr);
    return addr + offset % PAGE_SIZE;
  }
  return nullptr;
}
void READ_LOCK() {
  if (RECLAIM.load()) RECLAIM_LOCK[mmanager.ID] = true;
}
void WAIT_READ_LOCK() {
  while (true) {
    int count = 0;
    if (mmanager.ID != -1 && mmanager.workthread)
      RECLAIM_LOCK[mmanager.ID] = true;
    for (size_t i = 0; i < CORE_NUM; i++)
      if (RECLAIM_LOCK[i]) count++;
    if (count == memory_manager_Pool.thread_counter.load()) break;
  }
}
void RELEASE_READ_LOCK() {
  for (size_t i = 0; i < CORE_NUM; i++) RECLAIM_LOCK[i] = false;
}
std::vector<std::string> split(const std::string &str,
                               const std::string &delims = " ") {
  std::vector<std::string> output;
  auto first = std::cbegin(str);
  while (first != std::cend(str)) {
    const auto second = std::find_first_of(
        first, std::cend(str), std::cbegin(delims), std::cend(delims));
    if (first != second) output.emplace_back(first, second);
    if (second == std::cend(str)) break;
    first = std::next(second);
  }
  return output;
}
void MemoryManager::delete_pm_file(size_t page_id) {
  auto addr = reinterpret_cast<void *>(PPage_table[page_id].load());
  pmem_unmap(addr, PAGE_SIZE);
  filesystem::remove(PM_PATH + to_string(page_id));
}

pair<size_t, char *> DRAM_MemoryManager::halloc(size_t size) {
  lock_guard<mutex> guard(alloc_mtx);
  // std::cout << "Allocate Bucket.\n" << std::endl;
  while (true) {
    if (local_offset + size < PAGE_SIZE) {
      local_offset += size;
      return {local_offset + current_PAGE_ID * PAGE_SIZE - size,
              base_addr + local_offset - size};
    } else {
      creat_new_space();
    }
  }
}

void DRAM_MemoryManager::creat_new_space() {
  lock_guard<mutex> guard(DRAM_MemoryManager::mtx);
  if (base_addr)
    reinterpret_cast<PAGE_METADATA *>(base_addr)->LOCAL_OFFSET = local_offset;
  base_addr = static_cast<char *>(aligned_alloc(CACHE_LINE_SIZE, PAGE_SIZE));
  current_PAGE_ID = DRAM_MemoryManager::PAGE_ID++;
  DPage_table[current_PAGE_ID] = base_addr;
  pages.push_back(current_PAGE_ID);
  // std::cout << "Create DPage: " << current_PAGE_ID << std::endl;
  local_offset = PRESERVE_SIZE_EACH_PAGE;
  reinterpret_cast<PAGE_METADATA *>(base_addr)->ALLOCATOR_ID = ID;
}
void DRAM_MemoryManager::persist(Segment *ht_new,
                                 std::vector<size_t> checkpoints) {
  if (!SNAPSHOT) return;
  snapshot_version++;
  // thread t(
  //     [](Segment *ht_new, std::vector<size_t> &checkpoints,
  //        size_t snapshot_version, int ID, std::vector<size_t> &pages,
  //        DRAM_MemoryManager *dm) {
  // persist Segment
  auto size = ht_new->num_buckets * sizeof(Bucket);
  auto ptr = reinterpret_cast<char *>(ht_new->buckets);
  auto addr = static_cast<char *>(map_pm_file(
      size,
      generate_filename(PM_FILE_NAME::SEGMENT_SNAPSHOT, snapshot_version, ID)));
  for (size_t i = 0; i < size; i += 8) {
    _mm_stream_si64(reinterpret_cast<long long *>(addr + i),
                    *(reinterpret_cast<long long *>(ptr + i)));
  }
  pmem_drain();
  pmem_unmap(addr, size);

  // persist DPages
  for (auto &&p : pages) {
    auto aid = reinterpret_cast<PAGE_METADATA *>(DPage_table[p])->ALLOCATOR_ID;
    if (ID == aid) {
      auto addr = static_cast<char *>(
          map_pm_file(PAGE_SIZE, generate_filename(PM_FILE_NAME::DPAGE_SNAPSHOT,
                                                   snapshot_version, ID, p)));
      auto page_addr = DPage_table[p];
      for (size_t i = 0; i < PAGE_SIZE; i += 8) {
        _mm_stream_si64(reinterpret_cast<long long *>(addr + i),
                        *(reinterpret_cast<long long *>(page_addr + i)));
      }
      pmem_drain();
      pmem_unmap(addr, PAGE_SIZE);
    } else
      break;
  }

  // update ROOT info
  {
    // log-free consistency safe
    lock_guard<mutex> guard(ROOT->mtx);
    // if crash here, we use old snapshot verison.
    ROOT->SS[ID].SEGMENT_SIZE = ht_new->num_buckets;
    ROOT->CURRENT_DPAGE_ID[ID] = current_PAGE_ID;
    pmem_persist(&ROOT->CURRENT_DPAGE_ID[ID], sizeof(size_t));
    {
      lock_guard<mutex> guard(DRAM_MemoryManager::mtx);
      ROOT->DPAGE_ID = DRAM_MemoryManager::PAGE_ID;
    }
    pmem_persist(&ROOT->DPAGE_ID, sizeof(size_t));

    // after updateing SNAPSHOT_VERSION, we can use the new snapshot
    ROOT->SS[ID].SNAPSHOT_VERSION = snapshot_version;
    pmem_persist(&ROOT->SS[ID], sizeof(ROOT->SS[ID]));

    // if crash here, we use new snapshot verison but redo more logs(from last
    // checkpoint).
    for (size_t i = 0; i < CORE_NUM; i++) {
      ROOT->SEGMENT_CHECKPOINT[ID][i] = checkpoints[i];
    }
    pmem_persist(ROOT->SEGMENT_CHECKPOINT[ID], sizeof(size_t) * CORE_NUM);
  }

  // free old DPage
  filesystem::remove(generate_filename(PM_FILE_NAME::DPAGE_SNAPSHOT,
                                       snapshot_version - 1, ID));

  filesystem::remove(generate_filename(PM_FILE_NAME::SEGMENT_SNAPSHOT,
                                       snapshot_version - 1, ID));
  // dm->reclaim(ht_new);
  //     },
  //     ht_new, std::ref(checkpoints), snapshot_version, ID, std::ref(pages),
  //     this);
  // t.join();
}
void DRAM_MemoryManager::reclaim(Segment *ht_old) {
  thread t(
      [](Segment *seg, DRAM_MemoryManager *dm) {
        // ensure there is no access on the old DPages.
        {
          lock_guard<mutex> guard_reclaim(RECLAIM_MTX);
          RECLAIM.store(true);
          WAIT_READ_LOCK();
          RECLAIM.store(false);
          RELEASE_READ_LOCK();
        }
        for (auto &&p : dm->pages) {
          free(DPage_table[p]);
          // std::cout << "Reclaim DPage: " << p << std::endl;
          DPage_table[p] = nullptr;
        }
        free(seg->buckets);
        free(seg);
      },
      ht_old, this);
  t.detach();
}

void PM_MemoryManager::update_metadata() {
  _mm_stream_si64(
      reinterpret_cast<long long *>(
          &reinterpret_cast<PAGE_METADATA *>(base_addr)->LOCAL_OFFSET),
      *reinterpret_cast<long long *>(&local_offset));
  pmem_drain();
}
void PM_MemoryManager::creat_new_space() {
  lock_guard<mutex> guard(PM_MemoryManager::mtx);
  if (base_addr) {
    update_metadata();
  }
  current_PAGE_ID = PAGE_ID++;
  base_addr = static_cast<char *>(
      map_pm_file(PAGE_SIZE, PM_PATH + "P_" + to_string(current_PAGE_ID)));
  *(reinterpret_cast<size_t *>(base_addr) + 3) = ID;
  PPage_table[current_PAGE_ID] = reinterpret_cast<size_t>(base_addr);
  memory_manager_Pool.ppage_in_use[ID] = current_PAGE_ID;
  auto page_metadata = reinterpret_cast<PAGE_METADATA *>(base_addr);
  page_metadata->PAGEID = current_PAGE_ID;
  page_metadata->LOCAL_OFFSET = PRESERVE_SIZE_EACH_PAGE;
  page_metadata->FREED = 0;
  page_metadata->ALLOCATOR_ID = ID;
  // if crash here, this PPage will be deleted during recovery
  pmem_persist(base_addr, PRESERVE_SIZE_EACH_PAGE);
  ROOT->PPAGE_ID = PM_MemoryManager::PAGE_ID;
  // if crash here, we will get the largest PPAGE_ID as counter by scanning all
  // PPages.
  pmem_persist(&ROOT->PPAGE_ID, sizeof(size_t));
  ROOT->CURRENT_PPAGE_ID[ID] = current_PAGE_ID;
  // if crash here, we will get the largest PPAGE_ID as counter by scanning all
  // PPages.
  pmem_persist(&ROOT->CURRENT_PPAGE_ID[ID], sizeof(size_t));
  local_offset = PRESERVE_SIZE_EACH_PAGE;
}

PM_MemoryManager::~PM_MemoryManager() {
  RECLAIM_LOCK[ID] = false;
  if (ID != -1) memory_manager_Pool.log_out(this);
}
pair<size_t, char *> PM_MemoryManager::halloc(size_t size) {
  while (true) {
    if (local_offset + ROUND_UP(size, 8) < PAGE_SIZE) {
      local_offset += size;
      return {local_offset + current_PAGE_ID * PAGE_SIZE - size,
              base_addr + local_offset - size};
    } else {
      creat_new_space();
    }
  }
}
MemoryManagerPool::MemoryManagerPool() {
  thread_counter = 0;

  for (size_t i = 0; i < CORE_NUM; i++) {
    ppage_in_use[i] = INVALID;
    RECLAIM_LOCK[CORE_NUM] = false;
  }
  for (size_t i = 0; i < MAX_PAGE_NUM; i++) {
    PPage_table[i] = INVALID;
    DPage_table[i] = nullptr;
  }
}
void MemoryManagerPool::info() {}

void MemoryManagerPool::shutdown(CLHT *clhts[TABLE_NUM]) {
  if (!SNAPSHOT) return;
  vector<thread> t;
  for (size_t i = 0; i < TABLE_NUM; i++) {
    auto clht = clhts[i];
    auto &dm = clht->table->hallocD;
    auto verion = ++dm->snapshot_version;
    auto sz = clht->table->num_buckets;
    // store segment
    auto name =
        generate_filename(PM_FILE_NAME::SEGMENT_SNAPSHOT, verion, dm->ID);
    // printf("\nSnapshot segment:%s %lu\n", name.c_str(), sz * sizeof(Bucket));
    auto addr = MemoryManager::map_pm_file(sz * sizeof(Bucket), name);
    t.push_back(std::thread([addr, clht, sz]() {
      memcpy(addr, clht->table->buckets, sz * sizeof(Bucket));
      pmem_deep_persist(addr, sz * sizeof(Bucket));
      pmem_unmap(addr, sz * sizeof(Bucket));
    }));

    // store DPage
    for (auto &&p : dm->pages) {
      auto name2 =
          generate_filename(PM_FILE_NAME::DPAGE_SNAPSHOT, verion, dm->ID, p);
      auto addr = MemoryManager::map_pm_file(PAGE_SIZE, name2);
      // printf("\nSnapshot DPage: %lu,%s \n", p, name2.c_str());
      t.push_back(std::thread([addr, p]() {
        memcpy(addr, DPage_table[p], PAGE_SIZE);
        pmem_deep_persist(addr, PAGE_SIZE);
        pmem_unmap(addr, PAGE_SIZE);
      }));
    }
  }
  for (auto &&i : t) i.join();
  t.clear();

  for (size_t i = 0; i < TABLE_NUM; i++) {
    auto &dm = clhts[i]->table->hallocD;
    ROOT->SS[i].SEGMENT_SIZE = clhts[i]->table->num_buckets;
    ROOT->SS[i].SNAPSHOT_VERSION = dm->snapshot_version;
    ROOT->CURRENT_DPAGE_ID[i] = dm->current_PAGE_ID;
    ROOT->CURRENT_DPAGE_OFFSET[i] = dm->local_offset;
  }
  for (size_t i = 0; i < CORE_NUM; i++) {
    ROOT->CURRENT_PPAGE_ID[i] = pm[i].current_PAGE_ID;
    ROOT->CURRENT_PPAGE_OFFSET[i] = pm[i].local_offset;
  }
  ROOT->DPAGE_ID = DRAM_MemoryManager::PAGE_ID;
  ROOT->PPAGE_ID = PM_MemoryManager::PAGE_ID;
  pmem_persist(ROOT, sizeof(root));
  ROOT->clean = 1;
  pmem_persist(&ROOT->clean, sizeof(ROOT->clean));
  pmem_unmap(ROOT, METADATA_SIZE);
  for (size_t i = 0; i < TABLE_NUM; i++) {
    auto &dm = clhts[i]->table->hallocD;
    filesystem::remove_all(generate_filename(PM_FILE_NAME::DPAGE_SNAPSHOT,
                                             dm->snapshot_version - 1, dm->ID));
    filesystem::remove_all(generate_filename(PM_FILE_NAME::SEGMENT_SNAPSHOT,
                                             dm->snapshot_version - 1, dm->ID));
  }
}

void MemoryManagerPool::creat() {
  filesystem::create_directory(PM_PATH);
  ROOT = static_cast<root *>(
      MemoryManager::map_pm_file(METADATA_SIZE, PM_PATH + "ROOT"));
}

std::vector<size_t> MemoryManagerPool::startup(CLHT *clhts[TABLE_NUM]) {
  ROOT = static_cast<root *>(
      MemoryManager::map_pm_file(METADATA_SIZE, PM_PATH + "ROOT"));

  // Recover PPage_table
  for (auto &&entry : filesystem::recursive_directory_iterator(PM_PATH)) {
    string name = entry.path().filename();
    if (name[0] == PM_FILE_NAME::PPAGE) {
      auto s = split(name, "_");
      auto page_id = stoull(s[1]);
      if (PPage_table[page_id].load() == UINT64_MAX) {
        auto addr = static_cast<char *>(
            MemoryManager::map_pm_file(PAGE_SIZE, entry.path()));
        PPage_table[page_id] = reinterpret_cast<size_t>(addr);
      }
    }
  }

  PM_MemoryManager::PAGE_ID = ROOT->PPAGE_ID;

  vector<size_t> checkpoint(CORE_NUM);
  vector<thread> t;

  std::cout << "Boot from "
            << (ROOT->clean ? "Normal Shutdown." : "System Crash.") << endl;

  if (ROOT->clean) {
    // Recover Halloc-P
    {
      for (size_t i = 0; i < CORE_NUM; i++) {
        pm[i].current_PAGE_ID = ROOT->CURRENT_PPAGE_ID[i];
        pm[i].local_offset = ROOT->CURRENT_PPAGE_OFFSET[i];
        pm[i].base_addr =
            reinterpret_cast<char *>(PPage_table[pm[i].current_PAGE_ID].load());
      }
    }

    // Recover Segment
    {
      for (auto &&entry : filesystem::recursive_directory_iterator(PM_PATH)) {
        string name = entry.path().filename();
        if (name[0] == PM_FILE_NAME::SEGMENT_SNAPSHOT) {
          auto s = split(name, "_");
          auto version = stoull(s[1]);
          auto segmend_id = stoull(s[2]);
          auto table_size = ROOT->SS[segmend_id].SEGMENT_SIZE;
          if (version == ROOT->SS[segmend_id].SNAPSHOT_VERSION) {
            auto addr = static_cast<uint8_t *>(MemoryManager::map_pm_file(
                table_size * sizeof(Bucket), entry.path()));
            auto table = new CLHT(table_size, segmend_id, version, false);
            t.push_back(std::thread([addr, table, table_size]() {
              memcpy(table->table->buckets, addr, table_size * sizeof(Bucket));
              pmem_unmap(addr, table_size * sizeof(Bucket));
            }));

            clhts[segmend_id] = table;
          } else {
            std::filesystem::remove(entry);
          }
        }
      }
    }

    // Recover Halloc-D
    {
      DRAM_MemoryManager::PAGE_ID = ROOT->DPAGE_ID;
      for (size_t i = 0; i < TABLE_NUM; i++) {
        clhts[i]->table->hallocD->current_PAGE_ID = ROOT->CURRENT_DPAGE_ID[i];
        clhts[i]->table->hallocD->local_offset = ROOT->CURRENT_DPAGE_OFFSET[i];
      }
    }

    // Recover DPage_table and DPage
    {
      for (auto &&entry : filesystem::recursive_directory_iterator(PM_PATH)) {
        string name = entry.path().filename();
        if (name[0] == PM_FILE_NAME::DPAGE_SNAPSHOT) {
          auto s = split(name, "_");
          auto version = stoull(s[1]);
          auto aid = stoul(s[2]);
          auto page_id = stoull(s[3]);
          auto addr = static_cast<PAGE_METADATA *>(
              MemoryManager::map_pm_file(PAGE_SIZE, entry.path()));
          // printf("\nRecovery DPage:%s \n", name.c_str());
          if (ROOT->SS[addr->ALLOCATOR_ID].SNAPSHOT_VERSION != version) {
            pmem_unmap(addr, PAGE_SIZE);
            filesystem::remove(entry.path());
            continue;
          }
          clhts[aid]->table->hallocD->pages.push_back(page_id);
          DPage_table[page_id] =
              static_cast<char *>(aligned_alloc(CACHE_LINE_SIZE, PAGE_SIZE));
          t.push_back(thread([page_id, addr]() {
            memcpy(DPage_table[page_id], addr, PAGE_SIZE);
            pmem_unmap(addr, PAGE_SIZE);
          }));
        }
      }
    }
    for (auto &&i : t) i.join();
    t.clear();

  } else {
    // Recover Halloc-P
    {
      for (size_t i = 0; i < CORE_NUM; i++) {
        pm[i].current_PAGE_ID = ROOT->CURRENT_PPAGE_ID[i];
        auto p = PPage_table[pm[i].current_PAGE_ID].load();
        if (p != INVALID) {
          pm[i].local_offset =
              reinterpret_cast<PAGE_METADATA *>(p)->LOCAL_OFFSET;
          pm[i].base_addr = reinterpret_cast<char *>(p);
        }
      }
    }

    // Recover Segment table
    {
      for (auto &&entry : filesystem::recursive_directory_iterator(PM_PATH)) {
        string name = entry.path().filename();
        if (name[0] == PM_FILE_NAME::SEGMENT_SNAPSHOT) {
          auto s = split(name, "_");
          auto version = stoull(s[1]);
          auto segment_id = stoull(s[2]);
          auto table_size = ROOT->SS[segment_id].SEGMENT_SIZE;
          if (version == ROOT->SS[segment_id].SNAPSHOT_VERSION) {
            auto addr = static_cast<uint8_t *>(MemoryManager::map_pm_file(
                table_size * sizeof(Bucket), entry.path()));
            auto table = new CLHT(table_size, segment_id, version, false);
            auto cp = [&](void *dst, void *src, size_t sz) {
              memcpy(dst, src, sz);
              pmem_unmap(src, table_size * sizeof(Bucket));
            };
            t.push_back(thread(cp, table->table->buckets, addr,
                               table_size * sizeof(Bucket)));
            clhts[segment_id] = table;
          } else {
            filesystem::remove(entry.path());
          }
        }
      }
      for (auto &&i : t) i.join();
      t.clear();
      for (size_t i = 0; i < TABLE_NUM; i++) {
        if (clhts[i] == nullptr)
          clhts[i] = new CLHT(DEAFULT_SEGMENT_SIZE, i, 0, true);
      }
    }

    // Recover DPage_table and DPage
    {
      size_t next_DPage_id = 0;
      for (auto &&entry : filesystem::recursive_directory_iterator(PM_PATH)) {
        string name = entry.path().filename();
        if (name[0] == PM_FILE_NAME::DPAGE_SNAPSHOT) {
          auto s = split(name, "_");
          auto aid = stoul(s[1]);
          auto version = stoull(s[2]);
          auto page_id = stoull(s[3]);
          next_DPage_id = next_DPage_id < page_id ? page_id : next_DPage_id;
          auto addr = static_cast<PAGE_METADATA *>(
              MemoryManager::map_pm_file(PAGE_SIZE, entry.path()));
          if (ROOT->SS[addr->ALLOCATOR_ID].SNAPSHOT_VERSION != version) {
            pmem_unmap(addr, PAGE_SIZE);
            filesystem::remove(entry.path());
          } else {
            clhts[aid]->table->hallocD->pages.push_back(page_id);
            DPage_table[page_id] =
                static_cast<char *>(aligned_alloc(CACHE_LINE_SIZE, PAGE_SIZE));
            auto cp = [](void *dst, void *src) {
              memcpy(dst, src, PAGE_SIZE);
              pmem_unmap(dst, PAGE_SIZE);
            };
            t.push_back(thread(cp, DPage_table[page_id], addr));
          }
        }
      }
      DRAM_MemoryManager::PAGE_ID = next_DPage_id + 1;
    }
    for (auto &&i : t) i.join();
    t.clear();
    // Recover DPage_table and DPage
    {
      for (size_t i = 0; i < CORE_NUM; i++) {
        checkpoint[i] = ROOT->SEGMENT_CHECKPOINT[0][i];
        for (size_t j = 0; j < TABLE_NUM; j++) {
          checkpoint[i] = checkpoint[i] < ROOT->SEGMENT_CHECKPOINT[j][i]
                              ? checkpoint[i]
                              : ROOT->SEGMENT_CHECKPOINT[j][i];
        }
      }
    }
  }
  return checkpoint;
}
}  // namespace HALO