#pragma once
#include "BufferFrame.hpp"
#include "FreeList.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <list>
#include <mutex>
#include <unordered_map>
#include <map>
#include <unordered_set>
// #include <deque>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct IOFrame {
   enum class STATE : u8 {
      READING = 0,
      READY = 1,
      TO_DELETE = 2,
      UNDEFINED = 3  // for debugging
   };
   std::mutex mutex;
   STATE state = STATE::UNDEFINED;
   BufferFrame* bf = nullptr;
   // -------------------------------------------------------------------------------------
   // Everything in CIOFrame is protected by partition lock
   // except the following counter which is decremented outside to determine
   // whether it is time to remove it
   atomic<s64> readers_counter = 0;
};
// -------------------------------------------------------------------------------------
struct HashTable {
   struct Entry {
      u64 key;
      Entry* next;
      IOFrame value;
      Entry(u64 key);
   };
   // -------------------------------------------------------------------------------------
   struct Handler {
      Entry** holder;
      operator bool() const { return holder != nullptr; }
      IOFrame& frame() const
      {
         assert(holder != nullptr);
         return *reinterpret_cast<IOFrame*>(&((*holder)->value));
      }
   };
   // -------------------------------------------------------------------------------------
   u64 mask;
   Entry** entries;
   // -------------------------------------------------------------------------------------
   u64 hashKey(u64 k);
   IOFrame& insert(u64 key);
   Handler lookup(u64 key);
   void remove(Handler& handler);
   void remove(u64 key);
   bool has(u64 key);  // for debugging
   HashTable(u64 size_in_bits);
};
// -------------------------------------------------------------------------------------
struct Partition {
   std::mutex ht_mutex;
   HashTable io_ht;
   // -------------------------------------------------------------------------------------
   const u64 free_bfs_limit;
   FreeList dram_free_list;
   // -------------------------------------------------------------------------------------
   // std::mutex ru_status;
   // std::unordered_map<RUID, std::unordered_set<PID>> ru_status_ht;
   struct RUstatus {
     /* Per ru mutex now to synchronize insertion done by multiple workers
     counter % 2 represents which of the queues insertion is on.*/ 
     std::mutex m;
     /** counter only incremented by the single page provider thread working on
     this partition. Worker threads will atomically read the counter on each acces,
     This value is rarely invalidated. But this is a temporary design. The real design 
     will use a lock free queue where this is not needed. */
     // this need not be an atomic counter. 
     int insertion_idx; // protected by mutex m
     struct BufferFrameD {
        BufferFrame* bf;
        /* Only for debugging purpouses to check that the frame is still
        holding the same page when inserted, when I am deleting it. */
        PID pid;
        BufferFrameD(BufferFrame *p) : bf(p), pid(p->header.pid) {}
     };
     // std::deque<BufferFrame*> deq[2];
     // sets[insertion_idx] is protected by m 
     // sets[1 -insertion_idx] is not proteceted because it is accessed by 
     //  a single thread. (page provider assigned to this partition)
     std::unordered_set<BufferFrame*> sets[2];
     // iterator to the sets[1-insertion_idx]
     // erasing from unordered_set does not invalidate iterator
     std::unordered_set<BufferFrame*>::iterator it;
     RUstatus() : insertion_idx(0) {}
     // RUstatus() = delete;
     RUstatus(const RUstatus&) = delete;
     RUstatus(RUstatus&&) = default;
   };
   // Maybe, look for workarounds to make this a vector.
   // std::map<RUID, RUstatus> ru_status;
   // std::vector<RUstatus> ru_status;
   std::unique_ptr<RUstatus[]> ru_status;
   std::atomic<u64> ru_status_visible_size = 0;
   RUID draining_ru = 0;
   void insert_frame_in_ru(BufferFrame *bf);
   void remove_frame_from_ru(BufferFrame *bf);
   void get_n_frames_from_ru(u64 batch_size, std::vector<BufferFrame *> &bfs);
   void recycle_ru();
   // void drain_new_ru();
   // -------------------------------------------------------------------------------------
   // SSD Pages
   const u64 pid_distance;
   std::mutex pids_mutex;  // protect free pids vector
   std::vector<PID> freed_pids;
   u64 next_pid;
   inline PID nextPID()
   {
      std::unique_lock<std::mutex> g_guard(pids_mutex);
      if (freed_pids.size()) {
         const u64 pid = freed_pids.back();
         freed_pids.pop_back();
         return pid;
      } else {
         const u64 pid = next_pid;
         next_pid += pid_distance;
         ensure((pid * PAGE_SIZE / 1024 / 1024 / 1024) <= FLAGS_ssd_gib);
         return pid;
      }
   }
   void freePage(PID pid)
   {
      std::unique_lock<std::mutex> g_guard(pids_mutex);
      freed_pids.push_back(pid);
   }
   u64 allocatedPages() { return next_pid / pid_distance; }
   u64 freedPages()
   {
      std::unique_lock<std::mutex> g_guard(pids_mutex);
      return freed_pids.size();
   }
   // -------------------------------------------------------------------------------------
   Partition(u64 first_pid, u64 pid_distance, u64 free_bfs_limit);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
