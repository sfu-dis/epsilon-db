#include "CompressedSpace.hpp"

#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "LBAChain.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include <zstd.h>
#include <zstd_errors.h>
// -------------------------------------------------------------------------------------
#include <sys/mman.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
namespace compression
{
namespace
{
struct CCtxDeleter {
   void operator()(ZSTD_CCtx* cctx) { ZSTD_freeCCtx(cctx); }
};
struct DCtxDeleter {
   void operator()(ZSTD_DCtx* dctx) { ZSTD_freeDCtx(dctx); }
};
ZSTD_CCtx* threadLocalCCtx()
{
   thread_local std::unique_ptr<ZSTD_CCtx, CCtxDeleter> cctx;
   if (!cctx) {
      cctx.reset(ZSTD_createCCtx());
      ensure(cctx != nullptr);
      ensure(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_compressionLevel, FLAGS_compression_level)));
      // Catches reads of a wrong address, it costs 4 bytes per page.
      ensure(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_checksumFlag, 1)));
      ensure(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_contentSizeFlag, 0)));
   }
   return cctx.get();
}
ZSTD_DCtx* threadLocalDCtx()
{
   thread_local std::unique_ptr<ZSTD_DCtx, DCtxDeleter> dctx;
   if (!dctx) {
      dctx.reset(ZSTD_createDCtx());
      ensure(dctx != nullptr);
   }
   return dctx.get();
}
}  // namespace
// -------------------------------------------------------------------------------------
u32 compressPage(const u8* page, u8* dst)
{
   // A compressed page must be strictly smaller than PAGE_SIZE, the length PAGE_SIZE means uncompressed.
   constexpr u32 header_size = sizeof(CompressedPageHeader);
   const size_t ret = ZSTD_compress2(threadLocalCCtx(), dst + header_size, PAGE_SIZE - 1 - header_size, page, PAGE_SIZE);
   if (ZSTD_isError(ret)) {
      ensure(ZSTD_getErrorCode(ret) == ZSTD_error_dstSize_tooSmall);
      std::memcpy(dst, page, PAGE_SIZE);
      return PAGE_SIZE;
   }
   setRUEpoch(dst, reinterpret_cast<const BufferFrame::Page*>(page)->ru_epoch);
   return header_size + ret;
}
// -------------------------------------------------------------------------------------
void decompressPage(const u8* src, u32 length, u8* page)
{
   ensure(length > 0 && length <= PAGE_SIZE);
   if (length == PAGE_SIZE) {
      std::memcpy(page, src, PAGE_SIZE);
      return;
   }
   constexpr u32 header_size = sizeof(CompressedPageHeader);
   ensure(length > header_size);
   const size_t ret = ZSTD_decompressDCtx(threadLocalDCtx(), page, PAGE_SIZE, src + header_size, length - header_size);
   if (ZSTD_isError(ret) || ret != PAGE_SIZE) {
      std::cerr << "Failed to decompress a page of " << length << " bytes : " << (ZSTD_isError(ret) ? ZSTD_getErrorName(ret) : "wrong size")
                << std::endl;
      ensure(false);
   }
   // The page may have been copied to a block of a newer RU epoch without being recompressed.
   reinterpret_cast<BufferFrame::Page*>(page)->ru_epoch = ruEpoch(src);
}
// -------------------------------------------------------------------------------------
u8* threadLocalBlockBuffer()
{
   struct Deleter {
      void operator()(u8* ptr) { std::free(ptr); }
   };
   thread_local std::unique_ptr<u8, Deleter> buffer(static_cast<u8*>(std::aligned_alloc(PAGE_SIZE, PAGE_SIZE)));
   ensure(buffer != nullptr);
   return buffer.get();
}
}  // namespace compression
// -------------------------------------------------------------------------------------
CompressedSpace::CompressedSpace(u64 blocks_count, u64 logical_pages, u32 writers_count)
    : blocks_count(blocks_count),
      logical_pages(logical_pages),
      writers_count(writers_count),
      live_bytes(std::make_unique<std::atomic<s32>[]>(blocks_count)),
      block_states(std::make_unique<std::atomic<u8>[]>(blocks_count)),
      writer_free_lists(std::make_unique<BlockFreeList[]>(writers_count))
{
   for (u64 b = 0; b < blocks_count; b++) {
      live_bytes[b].store(0, std::memory_order_relaxed);
      block_states[b].store(FREE, std::memory_order_relaxed);
   }
   // Anonymous memory reads as 0 (never written) until it is touched.
   void* map = mmap(nullptr, logical_pages * sizeof(std::atomic<u64>), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
   if (map == MAP_FAILED) {
      perror("mmap");
      SetupFailed("Could not reserve the page map of the compressed pages");
   }
   page_map = static_cast<std::atomic<u64>*>(map);
}
// -------------------------------------------------------------------------------------
CompressedSpace::~CompressedSpace()
{
   munmap(page_map, logical_pages * sizeof(std::atomic<u64>));
}
// -------------------------------------------------------------------------------------
void CompressedSpace::transition(u64 block, BlockState from, BlockState to)
{
   u8 expected = from;
   if (!block_states[block].compare_exchange_strong(expected, to)) {
      std::cerr << "[FATAL] Compressed block " << block << " is in state " << int(expected) << " instead of " << int(from)
                << " (0 free, 1 open, 2 sealed), live bytes " << live_bytes[block].load() << std::endl;
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
u64 CompressedSpace::allocateBlock()
{
   ensure(tls_writer_thread_id >= 0 && static_cast<u32>(tls_writer_thread_id) < writers_count);
   auto& mine = writer_free_lists[tls_writer_thread_id];
   {
      std::unique_lock guard(mine.mutex);
      if (mine.blocks.empty()) {
         // Refill from the blocks freed by the workers.
         std::unique_lock shared_guard(shared_free_list.mutex);
         const u64 n = std::min<u64>(shared_free_list.blocks.size(), 64);
         mine.blocks.insert(mine.blocks.end(), shared_free_list.blocks.end() - n, shared_free_list.blocks.end());
         shared_free_list.blocks.resize(shared_free_list.blocks.size() - n);
      }
      if (!mine.blocks.empty()) {
         const u64 block = mine.blocks.back();
         mine.blocks.pop_back();
         free_blocks.fetch_sub(1, std::memory_order_relaxed);
         ensure_equal(live_bytes[block].load(), 0);
         transition(block, FREE, OPEN);
         return block;
      }
   }
   const u64 block = next_unused_block.fetch_add(1);
   if (block >= blocks_count) {
      // The free blocks may all sit in the free lists of the other writers, we do not steal for now.
      std::cerr << "[FATAL] No free compressed block left: " << free_blocks.load() << " free blocks in the free lists, "
                << written_pages.load() << " pages written into " << written_blocks.load() << " blocks ("
                << written_compressed_bytes.load() << " compressed bytes)" << std::endl;
      BMC::global_bf->outOfSpace();
   }
   ensure_equal(live_bytes[block].load(), 0);
   transition(block, FREE, OPEN);
   return block;
}
// -------------------------------------------------------------------------------------
u64 CompressedSpace::allocateRawBlock()
{
   u64 block;
   {
      std::unique_lock guard(shared_free_list.mutex);
      if (!shared_free_list.blocks.empty()) {
         block = shared_free_list.blocks.back();
         shared_free_list.blocks.pop_back();
         free_blocks.fetch_sub(1, std::memory_order_relaxed);
      } else {
         block = next_unused_block.fetch_add(1);
         if (block >= blocks_count) {
            std::cerr << "[FATAL] No free compressed block left for an LBA chain block, " << free_blocks.load()
                      << " free blocks in the free lists of the writers" << std::endl;
            BMC::global_bf->outOfSpace();
         }
      }
   }
   ensure_equal(live_bytes[block].load(), 0);
   transition(block, FREE, OPEN);
   sealBlock(block, PAGE_SIZE);
   return block;
}
// -------------------------------------------------------------------------------------
void CompressedSpace::sealBlock(u64 block, u32 packed_bytes)
{
   ensure(block < blocks_count);
   ensure(packed_bytes > 0 && packed_bytes <= PAGE_SIZE);
   // No page of an open block can be freed: a page that is being written back is not freed when deleted.
   ensure_equal(live_bytes[block].load(), 0);
   transition(block, OPEN, SEALED);
   live_bytes[block].store(packed_bytes);
}
// -------------------------------------------------------------------------------------
void CompressedSpace::free(PID pid)
{
   if (!cpid::onSSD(pid)) {
      return;
   }
   const u64 block = cpid::block(pid);
   const s32 length = cpid::length(pid);
   ensure(block < blocks_count);
   ensure(cpid::offsetInBlock(pid) + length <= PAGE_SIZE);
   if (block_states[block].load() != SEALED) {
      std::cerr << "[FATAL] Freeing the page " << pid << " of the compressed block " << block << " in state "
                << int(block_states[block].load()) << ", double free ?" << std::endl;
      ensure(false);
   }
   const s32 left = live_bytes[block].fetch_sub(length) - length;
   if (left < 0) {
      std::cerr << "[FATAL] Freeing the page " << pid << " made the live bytes of block " << block << " negative (" << left
                << "), double free ?" << std::endl;
      ensure(false);
   }
   if (left == 0) {
      releaseBlock(block);
   }
}
// -------------------------------------------------------------------------------------
void CompressedSpace::releaseBlock(u64 block)
{
   transition(block, SEALED, FREE);
   auto& list = (tls_writer_thread_id >= 0) ? writer_free_lists[tls_writer_thread_id] : shared_free_list;
   std::unique_lock guard(list.mutex);
   list.blocks.push_back(block);
   free_blocks.fetch_add(1, std::memory_order_relaxed);
}
// -------------------------------------------------------------------------------------
u64 CompressedSpace::usedBlocks()
{
   return std::min(next_unused_block.load(), blocks_count) - free_blocks.load();
}
// -------------------------------------------------------------------------------------
namespace
{
// 0002: the page map replaced the live bytes, the PIDs are logical.
// 0003: the compressed pages start with a CompressedPageHeader.
constexpr u64 SPACE_MAP_MAGIC = 0xC0DEC0DE5A5A0003;
}
// Format: magic, blocks_count, logical_pages, next_unused_block, pids_end, page_map[0, pids_end)
void CompressedSpace::persist(const std::string& path, PID pids_end)
{
   ensure(pids_end <= logical_pages);
   std::ofstream file(path, std::ios::binary | std::ios::trunc);
   ensure(file.is_open());
   const u64 header[5] = {SPACE_MAP_MAGIC, blocks_count, logical_pages, std::min(next_unused_block.load(), blocks_count), pids_end};
   file.write(reinterpret_cast<const char*>(header), sizeof(header));
   static_assert(sizeof(std::atomic<u64>) == sizeof(u64));
   file.write(reinterpret_cast<const char*>(page_map), pids_end * sizeof(u64));
   ensure(file.good());
}
// -------------------------------------------------------------------------------------
void CompressedSpace::recover(const std::string& path, const std::vector<u64>& raw_blocks)
{
   std::ifstream file(path, std::ios::binary);
   if (!file.is_open()) {
      SetupFailed("Could not open the compression space map " + path + ", was the database loaded with --compress_pages ?");
   }
   u64 header[5];
   file.read(reinterpret_cast<char*>(header), sizeof(header));
   ensure(file.good());
   if (header[0] != SPACE_MAP_MAGIC) {
      SetupFailed("The compression space map " + path + " was written by an older build, please load the database again");
   }
   if (header[1] != blocks_count || header[2] != logical_pages) {
      SetupFailed("The compression space map was written for a database of a different size");
   }
   const u64 next = header[3];
   const PID pids_end = header[4];
   ensure(next <= blocks_count && pids_end <= logical_pages);
   file.read(reinterpret_cast<char*>(page_map), pids_end * sizeof(u64));
   ensure(file.good());
   // The live bytes of a block are the lengths of the pages it holds.
   for (PID pid = 0; pid < pids_end; pid++) {
      const PID loc = page_map[pid].load(std::memory_order_relaxed);
      if (!cpid::onSSD(loc)) {
         continue;
      }
      const u64 block = cpid::block(loc);
      ensure(block < next);
      ensure(cpid::offsetInBlock(loc) + cpid::length(loc) <= PAGE_SIZE);
      const s32 live = live_bytes[block].fetch_add(cpid::length(loc), std::memory_order_relaxed) + cpid::length(loc);
      ensure(live <= static_cast<s32>(PAGE_SIZE));
   }
   for (const u64 block : raw_blocks) {
      ensure(block < next);
      if (live_bytes[block].load(std::memory_order_relaxed) != 0) {
         std::cerr << "[FATAL] The LBA chain block " << block << " also holds pages of the page map" << std::endl;
         ensure(false);
      }
      live_bytes[block].store(PAGE_SIZE, std::memory_order_relaxed);
   }
   std::unique_lock guard(shared_free_list.mutex);
   for (u64 b = 0; b < next; b++) {
      const bool free = live_bytes[b].load(std::memory_order_relaxed) == 0;
      block_states[b].store(free ? FREE : SEALED);
      if (free) {
         shared_free_list.blocks.push_back(b);
      }
   }
   free_blocks.store(shared_free_list.blocks.size());
   next_unused_block.store(next);
}
// -------------------------------------------------------------------------------------
BlockPacker::BlockPacker(CompressedSpace& space, u8* blocks, u64 max_blocks, LBAChain* lba_chain)
    : space(space),
      lba_chain(lba_chain),
      blocks(blocks),
      max_blocks(max_blocks),
      block_ids(max_blocks),
      used(max_blocks),
      in_place(max_blocks),
      block_ru_epochs(max_blocks),
      scratch(std::make_unique<u8[]>(PAGE_SIZE))
{
   ensure(max_blocks > 0);
   ensure(reinterpret_cast<u64>(blocks) % 512 == 0);
}
// -------------------------------------------------------------------------------------
void BlockPacker::recordBlock(u64 slot)
{
   if (lba_chain) {
      block_ru_epochs[slot] = lba_chain->record(block_ids[slot]);
   }
}
// -------------------------------------------------------------------------------------
PID BlockPacker::add(const u8* page, bool compress, PID current_location, ru_epoch_t& ru_epoch)
{
   ensure(!sealed);
   ru_epoch = reinterpret_cast<const BufferFrame::Page*>(page)->ru_epoch;
   if (!compress && cpid::length(current_location) == PAGE_SIZE) {
      // Uncompressed pages own their block, rewrite it in place.
      ensure(!full());
      const u64 slot = blocks_count++;
      block_ids[slot] = cpid::block(current_location);
      used[slot] = PAGE_SIZE;
      in_place[slot] = true;
      recordBlock(slot);
      std::memcpy(block(slot), page, PAGE_SIZE);
      if (lba_chain) {
         ru_epoch = reinterpret_cast<BufferFrame::Page*>(block(slot))->ru_epoch = block_ru_epochs[slot];
      }
      space.written_pages.fetch_add(1, std::memory_order_relaxed);
      space.written_compressed_bytes.fetch_add(PAGE_SIZE, std::memory_order_relaxed);
      return current_location;
   }
   u32 length = PAGE_SIZE;
   if (compress) {
      length = compression::compressPage(page, scratch.get());
   } else {
      std::memcpy(scratch.get(), page, PAGE_SIZE);
   }
   u64 slot = blocks_count;
   for (u64 b_i = 0; b_i < blocks_count; b_i++) {
      if (used[b_i] + length <= PAGE_SIZE && (slot == blocks_count || used[b_i] > used[slot])) {
         slot = b_i;
      }
   }
   if (slot == blocks_count) {
      ensure(!full());
      block_ids[slot] = space.allocateBlock();
      used[slot] = 0;
      in_place[slot] = false;
      recordBlock(slot);
      blocks_count++;
   }
   if (lba_chain) {
      // The page is written to the RU epoch of its block, the header (or the uncompressed page) says so.
      ru_epoch = block_ru_epochs[slot];
      if (length == PAGE_SIZE) {
         reinterpret_cast<BufferFrame::Page*>(scratch.get())->ru_epoch = ru_epoch;
      } else {
         compression::setRUEpoch(scratch.get(), ru_epoch);
      }
   }
   std::memcpy(block(slot) + used[slot], scratch.get(), length);
   const PID pid = cpid::make(blockOffset(slot) + used[slot], length);
   used[slot] += length;
   space.written_pages.fetch_add(1, std::memory_order_relaxed);
   space.written_compressed_bytes.fetch_add(length, std::memory_order_relaxed);
   return pid;
}
// -------------------------------------------------------------------------------------
void BlockPacker::seal()
{
   if (sealed) {
      return;
   }
   for (u64 b_i = 0; b_i < blocks_count; b_i++) {
      // Keep the tail of the block deterministic.
      std::memset(block(b_i) + used[b_i], 0, PAGE_SIZE - used[b_i]);
      if (!in_place[b_i]) {
         space.sealBlock(block_ids[b_i], used[b_i]);
      }
   }
   sealed = true;
}
// -------------------------------------------------------------------------------------
void BlockPacker::reset()
{
   ensure(sealed || blocks_count == 0);
   space.written_blocks.fetch_add(blocks_count, std::memory_order_relaxed);
   blocks_count = 0;
   sealed = false;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
