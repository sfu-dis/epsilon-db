#pragma once
#include "BufferFrame.hpp"
#include "Macros.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
class BufferManager;  // Forward declaration
// -------------------------------------------------------------------------------------
// On disk block of an LBA chain: a list of the LBAs written to an RU epoch, linked to the previous block of the same RU epoch.
// The LBAs are PIDs, or with compress_pages the (4 KiB) blocks the pages are packed into. The chain block itself is
// stored at the LBA pid.
struct alignas(PAGE_SIZE) LBAChainBlock {
   static constexpr u64 MAGIC = 0x6E69616843414C42;  // "LBAChain"
   static constexpr u64 HEADER_SIZE = 48;
   static constexpr u32 CAPACITY = (PAGE_SIZE - HEADER_SIZE) / sizeof(PID);
   // -------------------------------------------------------------------------------------
   u64 magic;
   PID pid;  // where the block itself is stored
   PID prev;  // previous block of the same RU epoch, INVALID_PID for the first one
   ru_epoch_t ru_epoch;  // the RU epoch whose LBAs are listed
   ru_epoch_t written_ru_epoch;  // the RU epoch the block itself got written to, ru_epoch or a newer one
   u32 count;  // entries reserved by the writers, the others are INVALID_PID
   u32 pad;
   // A PID may appear several times, INVALID_PID entries are to be skipped.
   PID entries[CAPACITY];
};
static_assert(sizeof(LBAChainBlock) == PAGE_SIZE, "");
static_assert(offsetof(LBAChainBlock, entries) == LBAChainBlock::HEADER_SIZE, "");
// -------------------------------------------------------------------------------------
// FLAGS_bg_page_fixer_variant 2, DEFAULT_RUH only: one LBA chain per RU epoch, its head is kept in the PersistantRUState.
//
// The writers (page providers, bg page fixers and the shutdown flush) record the PID of every page they write in a ring of
// FLAGS_lba_chain_buffer_blocks blocks. One block is active at a time and is tagged with an RU epoch, that RU epoch is the
// one the page is written to. A writer reserves an entry with a fetch_add on the block's word, fills it, then increments
// the block's filled counter. The writer reserving the entry just past the end of the block activates the next one.
//
// The ru_epoch_mgr closes the active block when it opens a new RU epoch and seals the closed blocks once filled: it
// allocates a PID for the block, links it to the head of its RU epoch's chain, writes it to the DEFAULT_RUH and persists
// the new head. The blocks live as long as the RU epoch they list, their PIDs are recycled once it is reclaimed.
class LBAChain
{
  public:
   // open_ru_epoch : the open RU epoch of the DEFAULT_RUH, its chain (if recovered) is continued.
   LBAChain(BufferManager& bm, ru_epoch_t open_ru_epoch);
   DO_NOT_COPY_AND_MOVE(LBAChain);
   ~LBAChain();
   // -------------------------------------------------------------------------------------
   // Writers: records the PID of a page (the block with compression) about to be written to the DEFAULT_RUH and returns the
   // RU epoch it belongs to.
   ru_epoch_t record(PID pid);
   // -------------------------------------------------------------------------------------
   // ru_epoch_mgr only. Each returns the number of blocks it wrote, they are accounted to the open RU epoch by the caller.
   // Called when the RU epoch new_ru_epoch is opened, the PIDs recorded from now on belong to it.
   u64 openRUEpoch(ru_epoch_t new_ru_epoch);
   // Seals the closed blocks, then persists the heads that changed.
   u64 sealClosedBlocks();
   // Makes the LBAs recorded so far for the RU epochs <= ru_epoch durable.
   u64 sealUpTo(ru_epoch_t ru_epoch);
   u64 stalls() const { return stalls_counter.load(std::memory_order_relaxed); }
   // Writers currently waiting for a free slot of the ring.
   s64 stalledWriters() const { return stalled_writers.load(std::memory_order_relaxed); }
   // Writers currently spinning on a full or closed block, waiting for the next one to get activated.
   s64 closedWaiters() const { return closed_waiters.load(std::memory_order_relaxed); }
   // True while the ru_epoch_mgr is closing the active block (waiting for its activation or a free slot).
   bool closerWaiting() const { return closer_waiting.load(std::memory_order_relaxed); }
   // -------------------------------------------------------------------------------------
   // Bg page fixers: reads the chain of the RU epoch. Appends its entries to entries (when not null) and its blocks
   // (PID, RU epoch it was written to) to blocks.
   static void readChain(int fd, PID head, ru_epoch_t ru_epoch, std::vector<PID>* entries, std::vector<std::pair<PID, ru_epoch_t>>& blocks);
   // Bg page fixers: frees a block of a reclaimed chain, as listed by readChain().
   static void recycleBlock(BufferManager& bm, PID pid, ru_epoch_t written_ru_epoch);

  private:
   // Not part of the block, so that the block is written as is.
   struct alignas(CACHE_LINE_SIZE) Control {
      // (seq << 32) | count. seq : the sequence number of the block currently in the slot.
      // count : the entries reserved, CLOSED is added once closed by the ru_epoch_mgr.
      std::atomic<u64> word{0};
      std::atomic<u32> filled{0};
      // The entries reserved before the block got closed, written by the closer before activating the next block.
      u32 reserved = 0;
   };
   static constexpr u64 CLOSED = u64(1) << 30;
   static constexpr u64 COUNT_MASK = (u64(1) << 32) - 1;
   static u64 makeWord(u64 seq) { return seq << 32; }
   static u32 wordSeq(u64 word) { return static_cast<u32>(word >> 32); }
   static u64 wordCount(u64 word) { return word & COUNT_MASK; }
   // -------------------------------------------------------------------------------------
   BufferManager& bm;
   const u64 n_blocks;
   // ru_epoch_mgr only: the RU epoch the next block is tagged with when the active one gets closed.
   ru_epoch_t open_ru_epoch;
   std::unique_ptr<Control[]> controls;
   LBAChainBlock* blocks;
   // Sequence number of the active block, the block seq lives in the slot seq % n_blocks.
   alignas(CACHE_LINE_SIZE) std::atomic<u64> active_seq{0};
   alignas(CACHE_LINE_SIZE) std::atomic<u64> stalls_counter{0};
   std::atomic<s64> stalled_writers{0};
   std::atomic<s64> closed_waiters{0};
   std::atomic<bool> closer_waiting{false};
   // ru_epoch_mgr only: the blocks in [sealed_seq, active_seq) are closed but not sealed yet.
   u64 sealed_seq = 0;
   std::vector<ru_epoch_t> dirty_heads;
   // -------------------------------------------------------------------------------------
   Control& control(u64 seq) { return controls[seq % n_blocks]; }
   LBAChainBlock& block(u64 seq) { return blocks[seq % n_blocks]; }
   // ru_epoch_mgr only: closes the active block, the next one is tagged with next_ru_epoch.
   u64 closeActiveBlock(ru_epoch_t next_ru_epoch);
   // The block seq got closed with reserved entries, activates the next one tagged with ru_epoch.
   void activateNext(u64 seq, u32 reserved, ru_epoch_t ru_epoch);
   // Returns true if the block got written, an empty block is only recycled.
   bool seal(u64 seq);
   // Where a block gets written to: a fresh PID, or a raw block with compression.
   PID allocateBlock();
   void persistDirtyHeads();
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
