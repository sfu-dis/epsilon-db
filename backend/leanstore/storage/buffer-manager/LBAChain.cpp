#include "LBAChain.hpp"

#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <immintrin.h>
#include <liburing.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
#include "leanstore/storage/NvmeDevice.hpp"

#include <algorithm>
#include <cstdlib>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
LBAChain::LBAChain(BufferManager& bm, ru_epoch_t open_ru_epoch) : bm(bm), n_blocks(FLAGS_lba_chain_buffer_blocks), open_ru_epoch(open_ru_epoch)
{
   ensure_lte(2, n_blocks);
   controls = std::make_unique<Control[]>(n_blocks);
   blocks = static_cast<LBAChainBlock*>(std::aligned_alloc(PAGE_SIZE, n_blocks * PAGE_SIZE));
   ensure(blocks != nullptr);
   for (u64 seq = 0; seq < n_blocks; ++seq) {
      control(seq).word.store(makeWord(seq), std::memory_order_relaxed);
   }
   block(0).ru_epoch = open_ru_epoch;
   active_seq.store(0, std::memory_order_release);
}
// -------------------------------------------------------------------------------------
LBAChain::~LBAChain()
{
   std::free(blocks);
}
// -------------------------------------------------------------------------------------
ru_epoch_t LBAChain::record(PID pid)
{
   ensure(pid != INVALID_PID);
   while (true) {
      const u64 seq = active_seq.load(std::memory_order_acquire);
      Control& c = control(seq);
      LBAChainBlock& b = block(seq);
      const u64 word = c.word.fetch_add(1, std::memory_order_acq_rel);
      const u64 slot = wordCount(word);
      if (slot < LBAChainBlock::CAPACITY) {
         if (wordSeq(word) == static_cast<u32>(seq)) {
            // The block can't be sealed before we fill the entry, so its RU epoch stays.
            b.entries[slot] = pid;
            const ru_epoch_t ru_epoch = b.ru_epoch;
            c.filled.fetch_add(1, std::memory_order_release);
            return ru_epoch;
         }
         // The slot got recycled for a newer block since we read active_seq, the entry we reserved there becomes a
         // tombstone so that its filled counter still catches up with the reserved entries.
         b.entries[slot] = INVALID_PID;
         c.filled.fetch_add(1, std::memory_order_release);
         continue;
      }
      if (slot == LBAChainBlock::CAPACITY && wordSeq(word) == static_cast<u32>(seq)) {
         // We reserved just past the end, the block is closed by us.
         activateNext(seq, LBAChainBlock::CAPACITY, b.ru_epoch);
         continue;
      }
      // Full or closed, the next block is about to get activated.
      _mm_pause();
   }
}
// -------------------------------------------------------------------------------------
void LBAChain::activateNext(u64 seq, u32 reserved, ru_epoch_t ru_epoch)
{
   control(seq).reserved = reserved;
   const u64 next = seq + 1;
   // The ring is full, wait for the ru_epoch_mgr to seal the block occupying the slot.
   if (wordSeq(control(next).word.load(std::memory_order_acquire)) != static_cast<u32>(next)) {
      stalls_counter.fetch_add(1, std::memory_order_relaxed);
      while (wordSeq(control(next).word.load(std::memory_order_acquire)) != static_cast<u32>(next)) {
         _mm_pause();
      }
   }
   block(next).ru_epoch = ru_epoch;
   // Publishes reserved and the RU epoch of the next block.
   active_seq.store(next, std::memory_order_release);
}
// -------------------------------------------------------------------------------------
u64 LBAChain::openRUEpoch(ru_epoch_t new_ru_epoch)
{
   ensure_lt(open_ru_epoch, new_ru_epoch);
   const u64 written = closeActiveBlock(new_ru_epoch);
   open_ru_epoch = new_ru_epoch;
   return written;
}
// -------------------------------------------------------------------------------------
u64 LBAChain::closeActiveBlock(ru_epoch_t next_ru_epoch)
{
   u64 written = 0;
   while (true) {
      const u64 seq = active_seq.load(std::memory_order_acquire);
      // From now on the writers reserve past CLOSED, which is never CAPACITY, so none of them activates the next block.
      const u64 word = control(seq).word.fetch_add(CLOSED, std::memory_order_acq_rel);
      ensure_equal(wordSeq(word), static_cast<u32>(seq));
      const u64 reserved = wordCount(word);
      if (reserved >= LBAChainBlock::CAPACITY) {
         // A writer filled it up and activates the next block (of the old RU epoch), close that one instead. The writer
         // may wait for a free slot, which only we free.
         while (active_seq.load(std::memory_order_acquire) == seq) {
            written += sealClosedBlocks();
            _mm_pause();
         }
         continue;
      }
      while (wordSeq(control(seq + 1).word.load(std::memory_order_acquire)) != static_cast<u32>(seq + 1)) {
         written += sealClosedBlocks();
      }
      activateNext(seq, reserved, next_ru_epoch);
      return written;
   }
}
// -------------------------------------------------------------------------------------
u64 LBAChain::sealClosedBlocks()
{
   u64 written = 0;
   const u64 active = active_seq.load(std::memory_order_acquire);
   while (sealed_seq < active) {
      written += seal(sealed_seq);
      ++sealed_seq;
   }
   persistDirtyHeads();
   return written;
}
// -------------------------------------------------------------------------------------
u64 LBAChain::sealUpTo(ru_epoch_t ru_epoch)
{
   u64 written = 0;
   if (ru_epoch >= open_ru_epoch) {
      // The RU epoch is still open, the next block continues it.
      written += closeActiveBlock(open_ru_epoch);
   }
   written += sealClosedBlocks();
   return written;
}
// -------------------------------------------------------------------------------------
bool LBAChain::seal(u64 seq)
{
   Control& c = control(seq);
   LBAChainBlock& b = block(seq);
   const u32 reserved = c.reserved;
   // The writers fill their entry right after reserving it.
   while (c.filled.load(std::memory_order_acquire) != reserved) {
      _mm_pause();
   }
   bool written = false;
   if (reserved > 0) {
      std::fill(b.entries + reserved, b.entries + LBAChainBlock::CAPACITY, INVALID_PID);
      const auto [pid, prev_ru_epoch, prev_ruh] = bm.randomPartition().nextPID();
      if (pid == INVALID_PID) {
         bm.outOfSpace();
      }
      if (FLAGS_enable_discarding) {
         ensure(bm.discard_state[pid].isFree());
      }
      // A recycled PID, its previous copy gets invalidated, like in pageWriteBackPrologue().
      if (prev_ru_epoch != UNMAPPED_RU_EPOCH && prev_ru_epoch > bm.reclaimed_ru_epoch[prev_ruh].load(std::memory_order_acquire)) {
         bm.ru_discard_set[prev_ruh].at(prev_ru_epoch).invalid.fetch_add(1);
      }
      PID& head = bm.persistant_ru_state->chainHead(b.ru_epoch);
      b.magic = LBAChainBlock::MAGIC;
      b.pid = pid;
      b.prev = head;
      b.written_ru_epoch = bm.ru_epoch[DEFAULT_RUH].load(std::memory_order_acquire);
      b.count = reserved;
      b.pad = 0;
      const s64 ret = nvme_pwrite(bm.ssd_fd, &b, PAGE_SIZE, pid * PAGE_SIZE, DEFAULT_RUH);
      ensure_equal(ret, s64(PAGE_SIZE));
      bm.ru_discard_set[DEFAULT_RUH][b.written_ru_epoch].total.fetch_add(1, std::memory_order_relaxed);
      head = pid;
      if (dirty_heads.empty() || dirty_heads.back() != b.ru_epoch) {
         dirty_heads.push_back(b.ru_epoch);
      }
      written = true;
   }
   // Recycles the slot for the block seq + n_blocks. The stale writers fill a tombstone as soon as the word is reset,
   // so filled is reset first.
   c.filled.store(0, std::memory_order_relaxed);
   c.reserved = 0;
   c.word.store(makeWord(seq + n_blocks), std::memory_order_release);
   return written;
}
// -------------------------------------------------------------------------------------
void LBAChain::persistDirtyHeads()
{
   for (const ru_epoch_t ru_epoch : dirty_heads) {
      bm.persistant_ru_state->writeChainHeadToPersistantStorage(bm.ssd_fd, bm.persistant_ru_state_offset, ru_epoch);
   }
   dirty_heads.clear();
}
// -------------------------------------------------------------------------------------
void LBAChain::readChain(int fd, PID head, ru_epoch_t ru_epoch, std::vector<PID>* entries, std::vector<std::pair<PID, ru_epoch_t>>& blocks)
{
   std::unique_ptr<LBAChainBlock, decltype(&std::free)> b(static_cast<LBAChainBlock*>(std::aligned_alloc(PAGE_SIZE, PAGE_SIZE)), &std::free);
   ensure(b != nullptr);
   for (PID pid = head; pid != INVALID_PID; pid = b->prev) {
      const s64 ret = pread(fd, b.get(), PAGE_SIZE, pid * PAGE_SIZE);
      ensure_equal(ret, s64(PAGE_SIZE));
      ensure_equal(b->magic, LBAChainBlock::MAGIC);
      ensure_equal(b->pid, pid);
      ensure_equal(b->ru_epoch, ru_epoch);
      ensure_lte(b->count, LBAChainBlock::CAPACITY);
      blocks.emplace_back(pid, b->written_ru_epoch);
      if (entries) {
         entries->insert(entries->end(), b->entries, b->entries + b->count);
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
