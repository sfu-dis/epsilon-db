#pragma once
#include "Swip.hpp"
#include "Units.hpp"
#include "leanstore/sync-primitives/Latch.hpp"
#include "leanstore/concurrency-recovery/WALEntry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cstring>
#include <optional>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
inline constexpr u64 MAX_PENDING_LSN_COUNT = 7;
namespace cr
{
struct Logging; // Forward Declaration
}
namespace storage
{
// -------------------------------------------------------------------------------------
const u64 PAGE_SIZE = 4 * 1024;
constexpr u64 PAGE_ALIGNEMENT = 1024;
// -------------------------------------------------------------------------------------
struct BufferFrame {
   enum class STATE : u8 { FREE = 0, HOT = 1, COOL = 2, LOADED = 3 };
   struct Header {
      WORKERID last_writer_worker_id = std::numeric_limits<u8>::max();  // for RFA
      LID last_written_plsn = 0;
      STATE state = STATE::FREE;  // INIT:
      std::atomic<bool> is_being_written_back = false;
      bool keep_in_memory = false;
      PID pid = 9999;         // INIT:
      HybridLatch latch = 0;  // INIT: // ATTENTION: NEVER DECREMENT
      // -------------------------------------------------------------------------------------
      BufferFrame* next_free_bf = nullptr;
      // -------------------------------------------------------------------------------------
      cr::Logging *logging = nullptr;
      bool flush_sink_log = false;
      LID fixed_at_plsn = 0; // TODO(mfd) : jsut for debugging, remove later
      u8 pending_lsn_count = 0;
      LID pending_lsn[MAX_PENDING_LSN_COUNT];
      // Any page is by default discardable unless :
      // 1. It is an inner node. (Simplicity)
      // 2. It has been splitted. (Simplicity)
      // 3. It is a newly allocated page. (Necessary)
      // 4. It belongs to an already reclaimed RU epoch. (Simplicity)
      std::atomic<bool> discardable = false;
      // -------------------------------------------------------------------------------------
      // Contention Split data structure
      struct ContentionTracker {
         u32 restarts_counter = 0;
         u32 access_counter = 0;
         s32 last_modified_pos = -1;
         void reset()
         {
            restarts_counter = 0;
            access_counter = 0;
            last_modified_pos = -1;
         }
      };
      ContentionTracker contention_tracker;
      // -------------------------------------------------------------------------------------
      struct OptimisticParentPointer {
         BufferFrame* parent_bf = nullptr;
         PID parent_pid;
         LID parent_plsn = 0;
         BufferFrame** swip_ptr = nullptr;
         s64 pos_in_parent = -1;
         void update(BufferFrame* new_parent_bf, PID new_parent_pid, LID new_parent_gsn, BufferFrame** new_swip_ptr, s64 new_pos_in_parent)
         {
            if (parent_bf != new_parent_bf || parent_pid != new_parent_pid || parent_plsn != new_parent_gsn || swip_ptr != new_swip_ptr ||
                pos_in_parent != new_pos_in_parent) {
               parent_bf = new_parent_bf;
               parent_pid = new_parent_pid;
               parent_plsn = new_parent_gsn;
               swip_ptr = new_swip_ptr;
               pos_in_parent = new_pos_in_parent;
            }
         }
      };
      OptimisticParentPointer optimistic_parent_pointer;
      // -------------------------------------------------------------------------------------
      u64 crc = 0;
      // -------------------------------------------------------------------------------------
      void dump();
   };
   // -------------------------------------------------------------------------------------
   struct PPL {
      cr::WALEntry wal_entry;
      struct PerPageLogEntryHeader {
         // Same as WALDTEntry
         LID gsn;
         DTID dt_id;
         PID pid;
      };
      PerPageLogEntryHeader header;
      u8 nb_log_records;
      static constexpr u32 space_for_log_records = PAGE_ALIGNEMENT - sizeof(Header) - sizeof(wal_entry) - sizeof(header) - 1;
      u8 log_records[space_for_log_records];
      void init()
      {
         wal_entry.type = cr::WALEntry::TYPE::PER_PAGE_DT_SPECIFIC;
         wal_entry.magic_debugging_number = 99;
         wal_entry.size = offsetof(PPL, log_records);
         nb_log_records = 0;
      }
      void reset()
      {
         nb_log_records = 0;
         wal_entry.size = offsetof(PPL, log_records);
      }
   };
   static_assert(offsetof(PPL, PPL::log_records) == 57, "");
   static constexpr u32 log_records_offset = offsetof(PPL, log_records);
   // -------------------------------------------------------------------------------------
   struct alignas(PAGE_ALIGNEMENT) Page {
      LID PLSN = 0;
      LID GSN = 0;
      DTID dt_id = 9999;                                                                               // INIT: datastructure id
      u64 magic_debugging_number;                                                                      // ATTENTION
      u32 fdp_plid = -1; // TODO(mfd) : Obsolete, remove
      u32 nbfixed = 0; // TODO(mfd) : Used just for debugging, remove later
      ru_epoch_t prev_ru_epoch = UNMAPPED_RU_EPOCH; // TODO(mfd) : Used just for debugging, remove later
      ru_epoch_t ru_epoch = UNMAPPED_RU_EPOCH;
      LID last_written_lsn = INVALID_LSN;
      s32 prev_log_id = -1; // TODO(mfd) : Used just for debugging, remove later
      s32 log_id = -1; // TODO(mfd) : Used just for debugging, remove later
      u8 dt[PAGE_SIZE - sizeof(PLSN) - sizeof(GSN) - sizeof(dt_id) - sizeof(magic_debugging_number) 
             - sizeof(fdp_plid) - sizeof(nbfixed) - 2 * sizeof(ru_epoch) - sizeof(last_written_lsn) - 2*sizeof(log_id)];  // Datastruture BE CAREFUL HERE !!!!!
      // -------------------------------------------------------------------------------------
      operator u8*() { return reinterpret_cast<u8*>(this); }
      // -------------------------------------------------------------------------------------
      void reset()
      {
          PLSN = 0;
          GSN = 0;
          ru_epoch = UNMAPPED_RU_EPOCH;
          prev_ru_epoch = UNMAPPED_RU_EPOCH;
          last_written_lsn = INVALID_LSN;
      }
      void dump();
   };
   // -------------------------------------------------------------------------------------
   struct Header header;
   // -------------------------------------------------------------------------------------
   // Per page logs will occupy the spare space between header and page.
   struct PPL ppl;
   // -------------------------------------------------------------------------------------
   struct Page page;  // The persisted part
   // -------------------------------------------------------------------------------------
   bool operator==(const BufferFrame& other) { return this == &other; }
   // -------------------------------------------------------------------------------------
   inline bool isDirty() const { return page.PLSN != header.last_written_plsn; }
   inline bool isFree() const { return header.state == STATE::FREE; }
   inline bool isDiscardable() const { return header.discardable.load(std::memory_order_acquire); }
   inline void markUnDiscardable() { return header.discardable.store(false, std::memory_order_release); }
   // -------------------------------------------------------------------------------------
   // Pre: bf is exclusively locked
   void reset()
   {
      header.crc = 0;
      // -------------------------------------------------------------------------------------
      assert(!header.is_being_written_back);
      header.latch.assertExclusivelyLatched();
      header.last_writer_worker_id = std::numeric_limits<u8>::max();
      header.last_written_plsn = 0;
      header.state = STATE::FREE;  // INIT:
      header.is_being_written_back.store(false, std::memory_order_release);
      header.pid = 9999;
      header.next_free_bf = nullptr;
      header.logging = nullptr;
      header.fixed_at_plsn = 0;
      header.discardable = false;
      header.flush_sink_log = false;
      header.contention_tracker.reset();
      header.keep_in_memory = false;
      header.pending_lsn_count = 0;
      ppl.reset();
      // std::memset(reinterpret_cast<u8*>(&page), 0, PAGE_SIZE);
   }
   // -------------------------------------------------------------------------------------
   BufferFrame()
   {
      header.latch->store(0ul);
      // set up the header per page log entry.
      ppl.init();
   }
   // -------------------------------------------------------------------------------------
   template <typename WT>
   std::optional<WT*> reservePPLEntry(u32 payload_size)
   {
      ensure(FLAGS_per_page_logging);
      if (!isDiscardable()) return std::nullopt;
      u32 total_entry_size = sizeof(WT) + payload_size;
      if (ppl.wal_entry.size + total_entry_size > sizeof(PPL)) {
         this->markUnDiscardable();
         return std::nullopt;
      }
      const u32 offset = ppl.wal_entry.size - log_records_offset;
      ppl.wal_entry.size += total_entry_size;
      ppl.nb_log_records++;
      return reinterpret_cast<WT*>(&ppl.log_records[offset]);
   }
   // -------------------------------------------------------------------------------------
   bool submitPPLEntry();
   // -------------------------------------------------------------------------------------
   void dump();
};
// -------------------------------------------------------------------------------------
static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(BufferFrame::Page::dt);
// -------------------------------------------------------------------------------------
static_assert(sizeof(BufferFrame::Page) == PAGE_SIZE, "");
static_assert(sizeof(BufferFrame) == PAGE_SIZE + PAGE_ALIGNEMENT, "");
// -------------------------------------------------------------------------------------
static_assert((sizeof(BufferFrame) - sizeof(BufferFrame::Page)) == PAGE_ALIGNEMENT, "");
// TODO(mfd) : put exact lower bound on PAGE_ALIGNEMENT - Header
static_assert(sizeof(BufferFrame::Header) < PAGE_ALIGNEMENT, "");
static_assert((PAGE_ALIGNEMENT - sizeof(BufferFrame::Header)) == sizeof(BufferFrame::PPL), "");
static_assert(sizeof(BufferFrame) == (sizeof(BufferFrame::Page) + sizeof(BufferFrame::PPL) + sizeof(BufferFrame::Header)), "");
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
