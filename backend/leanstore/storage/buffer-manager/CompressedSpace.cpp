#include "CompressedSpace.hpp"

#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include <zstd.h>
#include <zstd_errors.h>
// -------------------------------------------------------------------------------------
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
   const size_t ret = ZSTD_compress2(threadLocalCCtx(), dst, PAGE_SIZE - 1, page, PAGE_SIZE);
   if (ZSTD_isError(ret)) {
      ensure(ZSTD_getErrorCode(ret) == ZSTD_error_dstSize_tooSmall);
      std::memcpy(dst, page, PAGE_SIZE);
      return PAGE_SIZE;
   }
   return ret;
}
// -------------------------------------------------------------------------------------
void decompressPage(const u8* src, u32 length, u8* page)
{
   ensure(length > 0 && length <= PAGE_SIZE);
   if (length == PAGE_SIZE) {
      std::memcpy(page, src, PAGE_SIZE);
      return;
   }
   const size_t ret = ZSTD_decompressDCtx(threadLocalDCtx(), page, PAGE_SIZE, src, length);
   if (ZSTD_isError(ret) || ret != PAGE_SIZE) {
      std::cerr << "Failed to decompress a page of " << length << " bytes : " << (ZSTD_isError(ret) ? ZSTD_getErrorName(ret) : "wrong size")
                << std::endl;
      ensure(false);
   }
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
CompressedSpace::CompressedSpace(u64 blocks_count, u32 writers_count)
    : blocks_count(blocks_count),
      writers_count(writers_count),
      live_bytes(std::make_unique<std::atomic<s32>[]>(blocks_count)),
      writer_free_lists(std::make_unique<BlockFreeList[]>(writers_count))
{
   for (u64 b = 0; b < blocks_count; b++) {
      live_bytes[b].store(0, std::memory_order_relaxed);
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
         paranoid(live_bytes[block].load() == 0);
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
   return block;
}
// -------------------------------------------------------------------------------------
void CompressedSpace::sealBlock(u64 block, u32 packed_bytes)
{
   ensure(block < blocks_count);
   // A page packed into this block may already have been deleted, which made the counter negative.
   if (live_bytes[block].fetch_add(packed_bytes) + static_cast<s32>(packed_bytes) == 0) {
      releaseBlock(block);
   }
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
   if (live_bytes[block].fetch_sub(length) == length) {
      releaseBlock(block);
   }
}
// -------------------------------------------------------------------------------------
void CompressedSpace::releaseBlock(u64 block)
{
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
constexpr u64 SPACE_MAP_MAGIC = 0xC0DEC0DE5A5A0001;
}
// Format: magic, blocks_count, next_unused_block, live_bytes[0, next_unused_block)
void CompressedSpace::persist(const std::string& path)
{
   std::ofstream file(path, std::ios::binary | std::ios::trunc);
   ensure(file.is_open());
   const u64 next = std::min(next_unused_block.load(), blocks_count);
   const u64 header[3] = {SPACE_MAP_MAGIC, blocks_count, next};
   file.write(reinterpret_cast<const char*>(header), sizeof(header));
   std::vector<s32> live(next);
   for (u64 b = 0; b < next; b++) {
      live[b] = live_bytes[b].load();
      ensure(live[b] >= 0 && live[b] <= static_cast<s32>(PAGE_SIZE));
   }
   file.write(reinterpret_cast<const char*>(live.data()), live.size() * sizeof(s32));
   ensure(file.good());
}
// -------------------------------------------------------------------------------------
void CompressedSpace::recover(const std::string& path)
{
   std::ifstream file(path, std::ios::binary);
   if (!file.is_open()) {
      SetupFailed("Could not open the compression space map " + path + ", was the database loaded with --compress_pages ?");
   }
   u64 header[3];
   file.read(reinterpret_cast<char*>(header), sizeof(header));
   ensure(file.good());
   ensure_equal(header[0], SPACE_MAP_MAGIC);
   if (header[1] != blocks_count) {
      SetupFailed("The compression space map was written for a database of a different size");
   }
   const u64 next = header[2];
   std::vector<s32> live(next);
   file.read(reinterpret_cast<char*>(live.data()), live.size() * sizeof(s32));
   ensure(file.good());
   std::unique_lock guard(shared_free_list.mutex);
   for (u64 b = 0; b < next; b++) {
      live_bytes[b].store(live[b]);
      if (live[b] == 0) {
         shared_free_list.blocks.push_back(b);
      }
   }
   free_blocks.store(shared_free_list.blocks.size());
   next_unused_block.store(next);
}
// -------------------------------------------------------------------------------------
BlockPacker::BlockPacker(CompressedSpace& space, u8* blocks, u64 max_blocks)
    : space(space),
      blocks(blocks),
      max_blocks(max_blocks),
      block_ids(max_blocks),
      used(max_blocks),
      scratch(std::make_unique<u8[]>(PAGE_SIZE))
{
   ensure(max_blocks > 0);
   ensure(reinterpret_cast<u64>(blocks) % 512 == 0);
}
// -------------------------------------------------------------------------------------
PID BlockPacker::add(const u8* page, bool compress)
{
   ensure(!sealed);
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
      blocks_count++;
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
      space.sealBlock(block_ids[b_i], used[b_i]);
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
