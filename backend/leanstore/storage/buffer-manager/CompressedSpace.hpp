#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
// Experimental page compression (--compress_pages), in the spirit of ZLeanStore.
// Pages are compressed upon write back and packed out-of-place into 4 KiB blocks, a compressed page never
// straddles two blocks so that a buffer miss is still a single 4 KiB read.
// The PIDs are logical, the page map gives the location (cpid) of each page on the SSD. Every write back of a
// compressed page goes to a new location and frees the previous one, the uncompressed pages (inner and meta nodes)
// are rewritten in place. There is no crash recovery, only a clean shutdown is persisted.
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Location (cpid) of a page on the SSD, as stored in the page map:
//   [63..61] always 0
//   [60..48] compressed length in (0, PAGE_SIZE], PAGE_SIZE means the page is stored uncompressed
//   [47..0]  byte address on ssd_fd
// 0 (length 0) means the page has never been written.
namespace cpid
{
constexpr u64 ADDRESS_BITS = 48;
constexpr u64 ADDRESS_MASK = (u64(1) << ADDRESS_BITS) - 1;
constexpr u64 LENGTH_MASK = 0x1FFF;
static_assert(((LENGTH_MASK << ADDRESS_BITS) >> 61) == 0, "the length must not overlap the swip status bits");
static_assert(PAGE_SIZE <= LENGTH_MASK, "");
inline PID make(u64 address, u32 length)
{
   return (u64(length) << ADDRESS_BITS) | address;
}
inline u64 address(PID pid)
{
   return pid & ADDRESS_MASK;
}
inline u32 length(PID pid)
{
   return (pid >> ADDRESS_BITS) & LENGTH_MASK;
}
inline bool onSSD(PID pid)
{
   return length(pid) != 0;
}
inline u64 block(PID pid)
{
   return address(pid) / PAGE_SIZE;
}
inline u64 offsetInBlock(PID pid)
{
   return address(pid) % PAGE_SIZE;
}
}  // namespace cpid
// -------------------------------------------------------------------------------------
namespace compression
{
// A compressed page is stored as a CompressedPageHeader followed by its zstd frame, the length in its cpid covers both.
// The header is not compressed so that the page can be copied to another block (of a newer RU epoch) as is, only
// its header gets updated. The RU epoch inside the compressed page may then be stale, decompressPage() brings it
// up to date from the header. An uncompressed page (length PAGE_SIZE) has no header, its RU epoch is in the page.
struct CompressedPageHeader {
   ru_epoch_t ru_epoch;
};
// The compressed pages are packed at any byte offset of a block, so the header is accessed through memcpy.
// Pre: src points to a compressed page (length < PAGE_SIZE).
inline ru_epoch_t ruEpoch(const u8* src)
{
   CompressedPageHeader header;
   std::memcpy(&header, src, sizeof(header));
   return header.ru_epoch;
}
inline void setRUEpoch(u8* src, ru_epoch_t ru_epoch)
{
   CompressedPageHeader header{ru_epoch};
   std::memcpy(src, &header, sizeof(header));
}
// Compresses a PAGE_SIZE page into dst (PAGE_SIZE bytes), header included, returns the length.
// A page that does not compress is copied as is and PAGE_SIZE is returned.
u32 compressPage(const u8* page, u8* dst);
// Inverse of compressPage, ensures the page decompresses to exactly PAGE_SIZE bytes.
// The RU epoch of a compressed page is taken from its header.
void decompressPage(const u8* src, u32 length, u8* page);
// PAGE_SIZE aligned buffer of the calling thread to read a block into.
u8* threadLocalBlockBuffer();
}  // namespace compression
// -------------------------------------------------------------------------------------
// Free space of the compressed pages, in 4 KiB blocks of the database region.
// Each block keeps the number of bytes of its live pages, once it drops to 0 the block is free again and
// goes to the free list of the writer that freed it. Writers allocate blocks from their own free list.
class CompressedSpace
{
  public:
   // The logical pages may outnumber the physical blocks, we assume a compression ratio of up to 2.
   static constexpr u64 LOGICAL_PAGES_FACTOR = 2;
   CompressedSpace(u64 blocks_count, u64 logical_pages, u32 writers_count);
   ~CompressedSpace();
   // -------------------------------------------------------------------------------------
   // Page map: logical PID -> cpid, 0 if the page has never been written.
   PID location(PID pid) const
   {
      ensure(pid < logical_pages);
      return page_map[pid].load(std::memory_order_acquire);
   }
   // Returns the previous location, the caller frees it (unless the page got rewritten in place).
   PID exchangeLocation(PID pid, PID location)
   {
      ensure(pid < logical_pages);
      return page_map[pid].exchange(location, std::memory_order_acq_rel);
   }
   // -------------------------------------------------------------------------------------
   // Pre: called by a writer (tls_writer_thread_id != -1).
   u64 allocateBlock();
   // Called by the writer once it is done packing pages into the block, before writing it.
   void sealBlock(u64 block, u32 packed_bytes);
   // Frees the copy of a page that got rewritten elsewhere or deleted, no-op if the page is not on the SSD.
   void free(PID pid);
   // -------------------------------------------------------------------------------------
   // Persists the page map of the PIDs [0, pids_end) and the bump pointer, the live bytes are recomputed on recovery.
   void persist(const std::string& path, PID pids_end);
   void recover(const std::string& path);
   // -------------------------------------------------------------------------------------
   // Stats, written blocks are counted by the writers.
   std::atomic<u64> written_pages = 0;
   std::atomic<u64> written_compressed_bytes = 0;
   std::atomic<u64> written_blocks = 0;
   u64 usedBlocks();

  private:
   void releaseBlock(u64 block);
   // -------------------------------------------------------------------------------------
   struct alignas(CACHE_LINE_SIZE) BlockFreeList {
      // Only contended at shutdown, where more threads than writers flush the buffer pool.
      std::mutex mutex;
      std::vector<u64> blocks;
   };
   const u64 blocks_count;
   const u64 logical_pages;
   const u32 writers_count;
   // mmap'ed without prefaulting, the pages of the map get backed on first touch.
   std::atomic<u64>* page_map;
   std::unique_ptr<std::atomic<s32>[]> live_bytes;
   // Life cycle of a block, checked on every transition to catch a double free before it corrupts data:
   // FREE -allocateBlock-> OPEN (being packed) -sealBlock-> SEALED -last free-> FREE
   enum BlockState : u8 { FREE = 0, OPEN = 1, SEALED = 2 };
   std::unique_ptr<std::atomic<u8>[]> block_states;
   void transition(u64 block, BlockState from, BlockState to);
   // Blocks above have never been used.
   std::atomic<u64> next_unused_block = 0;
   std::atomic<u64> free_blocks = 0;
   std::unique_ptr<BlockFreeList[]> writer_free_lists;
   // Blocks freed by non writer threads (workers deleting pages) and the free blocks found on recovery.
   BlockFreeList shared_free_list;
};
// -------------------------------------------------------------------------------------
// Packs the compressed pages of a writer into blocks, the caller writes them out after seal().
// All the blocks of a batch stay open until seal(), a page goes to the fullest block it fits in (best fit).
class BlockPacker
{
  public:
   // blocks: max_blocks * PAGE_SIZE bytes, aligned for O_DIRECT.
   BlockPacker(CompressedSpace& space, u8* blocks, u64 max_blocks);
   // Returns the new location of the page. Pre: !full()
   // compress = false stores the page as is in a block of its own, for the special nodes (inner and meta nodes).
   // Such a page is rewritten in place when its current location is already an uncompressed block.
   PID add(const u8* page, bool compress, PID current_location);
   // There may be no room left for another page.
   bool full() const { return blocks_count >= max_blocks; }
   // Pre: the batch is sealed and written.
   void seal();
   void reset();
   // -------------------------------------------------------------------------------------
   u64 blocksCount() const { return blocks_count; }
   u8* block(u64 slot) { return blocks + slot * PAGE_SIZE; }
   u64 blockOffset(u64 slot) const { return block_ids[slot] * PAGE_SIZE; }
   u64 slotOf(const void* block_ptr) const { return (reinterpret_cast<const u8*>(block_ptr) - blocks) / PAGE_SIZE; }

  private:
   CompressedSpace& space;
   u8* const blocks;
   const u64 max_blocks;
   u64 blocks_count = 0;
   bool sealed = false;
   std::vector<u64> block_ids;
   std::vector<u32> used;
   // The block is rewritten in place, it is already sealed.
   std::vector<bool> in_place;
   std::unique_ptr<u8[]> scratch;
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
