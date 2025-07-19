#include "Partition.hpp"

#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <sys/mman.h>

#include <cstring>
#include <unordered_set>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void* malloc_huge(size_t size)
{
   void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   memset(p, 0, size);
   return p;
}
// -------------------------------------------------------------------------------------
HashTable::Entry::Entry(PID key) : key(key) {}
// -------------------------------------------------------------------------------------
HashTable::HashTable(u64 sizeInBits)
{
   uint64_t size = (1ull << sizeInBits);
   mask = size - 1;
   entries = (Entry**)malloc_huge(size * sizeof(Entry*));
}
// -------------------------------------------------------------------------------------
u64 HashTable::hashKey(PID k)
{
   // MurmurHash64A
   const uint64_t m = 0xc6a4a7935bd1e995ull;
   const int r = 47;
   uint64_t h = 0x8445d61a4e774912ull ^ (8 * m);
   k *= m;
   k ^= k >> r;
   k *= m;
   h ^= k;
   h *= m;
   h ^= h >> r;
   h *= m;
   h ^= h >> r;
   return h;
}
// -------------------------------------------------------------------------------------
IOFrame& HashTable::insert(PID key)
{
   auto e = new Entry(key);
   uint64_t pos = hashKey(key) & mask;
   e->next = entries[pos];
   entries[pos] = e;
   return e->value;
}
// -------------------------------------------------------------------------------------
HashTable::Handler HashTable::lookup(PID key)
{
   uint64_t pos = hashKey(key) & mask;
   Entry** e_ptr = entries + pos;
   Entry* e = *e_ptr;  // e is only here for readability
   while (e) {
      if (e->key == key)
         return {e_ptr};
      e_ptr = &(e->next);
      e = e->next;
   }
   return {nullptr};
}
// -------------------------------------------------------------------------------------
void HashTable::remove(HashTable::Handler& handler)
{
   Entry* to_delete = *handler.holder;
   *handler.holder = (*handler.holder)->next;
   delete to_delete;
}
// -------------------------------------------------------------------------------------
void HashTable::remove(u64 key)
{
   auto handler = lookup(key);
   assert(handler);
   remove(handler);
}
// -------------------------------------------------------------------------------------
bool HashTable::has(u64 key)
{
   uint64_t pos = hashKey(key) & mask;
   auto e = entries[pos];
   while (e) {
      if (e->key == key)
         return true;
      e = e->next;
   }
   return false;
}
// -------------------------------------------------------------------------------------
Partition::Partition(u64 first_pid, u64 pid_distance, u64 free_bfs_limit)
    : io_ht(utils::getBitsNeeded(free_bfs_limit)), free_bfs_limit(free_bfs_limit), pid_distance(pid_distance)
{
   next_pid = first_pid;
   // initialize all the values so that we avoid concurrency bug 
   // from two worker threads trying to insert at the same time 
   // a new RUStatus. Now we are using std::unordered_map with 
   // std::mutex just to quickly iterate. Later we will change 
   // this with a static hash table.
   ru_status = std::make_unique<RUstatus[]>(2048);
}

void Partition::insert_frame_in_ru(BufferFrame *bf) {
  bool first = false;
  RUID ruid = bf->page.reclaim_unit;
  ensure(ruid < 2048);
  RUstatus &rus = ru_status[ruid];
  std::lock_guard<std::mutex> g_guard(rus.m);
/*
  if (rus.sets[rus.insertion_idx].empty()) {
    first = true;
  }
*/
  rus.sets[rus.insertion_idx].insert(bf);
/*
  if (first) {
    u64 n = ru_status_visible_size.fetch_add(1UL, std::memory_order_release); 
    printf("[INFO] new visible RU for this partition %lu\n", n + 1);
  }
*/
}

/** The last two methods are operated by the page provider
thread which is per partition so they are safe to execute without 
latches if inserts which are called by multiple workers threads are
handled separately in a lock-free queue for example.*/
void Partition::remove_frame_from_ru(BufferFrame *bf) {
  RUID ruid = bf->page.reclaim_unit;
  RUstatus &rus = ru_status[ruid];
  size_t exist = rus.sets[1-rus.insertion_idx].erase(bf);
  // For the moment not necessarly the value exists in the RU Stats
  // It could be chosen by random.
  // ensure(exist);
}

void Partition::get_n_frames_from_ru(u64 batch_size, std::vector<BufferFrame *> &bfs) {
  // start by looking at the current draining ru and then next and next, until getting 
  // the batch size.
  u64 count = 0;
  std::vector<u64> counts;
  // potential race condition : insert_frame_in_ru inserts a new RUID
  // quick fix, use ordered map and take a snapshot of the last iterator 
  // holding the lock
  // In the beginning the ru_status will be empty because we need to evict first
  // so that next time when we bring the pages back they will be put back in the RU
  /** u64 visible_size = ru_status_visible_size.load(std::memory_order_acquire);
  if (visible_size == 0) {
    // better return random
    // TODO(mfd) : WARN on this case.
    return;
  }*/
  // auto ru_status_it = ru_status.begin();
  u64 i = 0;
  while (i < 10 && count < batch_size) {
    // RUstatus &rus = ru_status[ruid];
    // auto &it = rus.it;
    RUstatus &rus = ru_status[i];
    // decltype(rus.sets[0]) draining_set;
    if (rus.it == rus.sets[1-rus.insertion_idx].end()) {
    {
      std::lock_guard<std::mutex> g_guard(rus.m);
      rus.insertion_idx = 1 - rus.insertion_idx;
      // reinitialize the iterator
      rus.it = rus.sets[1-rus.insertion_idx].begin();
      printf("Swap draining set of RUID %ld, new one have %lu frames\n", i, rus.sets[1-rus.insertion_idx].size());
      // XXX(mfd) : If after switching the new draining set was empty, flag
      // this so I can immeadiately skip it.
    }
    }
    auto &draining_set = rus.sets[1-rus.insertion_idx];
    // break upon first empty
    // if (draining_set.empty()) continue;
    u64 cnt = 0;
    while (rus.it != draining_set.end()) {
      if (count >= batch_size) break;
      bfs.push_back(*rus.it);
      rus.it++;
      ++cnt; ++count;
    }
    counts.push_back(cnt);
    // break;
    ++i;
  }
  // std::cout << "\nGet N frames : NRUH " << ru_status.size() << " "; 
  // std::cout << " ";
  // for (const auto &c : counts) std::cout << c << ",";
  // std::cout << std::endl;
}


void Partition::recycle_ru() {
  RUstatus &rus = ru_status[draining_ru];
  std::lock_guard<std::mutex> g_guard(rus.m);
  rus.insertion_idx = 1 - rus.insertion_idx;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
