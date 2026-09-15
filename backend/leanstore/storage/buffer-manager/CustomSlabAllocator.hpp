#pragma once
#include "Units.hpp"
#include "BufferManager.hpp"
#include "Macros.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
/**
Custom Slab-like allocator used to allocate small LSN array of discarded pages.
*/
template <typename T>
class CustomSlabAllocator
{
   static constexpr u32 CHUNK_OBJS = 1024;

   DO_NOT_COPY(CustomSlabAllocator);

   struct Chunks {
      T* ptr;
      explicit Chunks(T* p) : ptr(p) {}
   };

   class FreeList
   {
      u32 n;
      std::mutex m;
      T* next_free = nullptr;
      std::vector<Chunks> chunks;

      void allocate_new_chunk()
      {
         const u64 chunk_bytes = CHUNK_OBJS * n * sizeof(T);
         T* base = static_cast<T*>(std::malloc(chunk_bytes));
         ensure(base != nullptr);
         chunks.emplace_back(base);

         for (u32 i = 0; i < CHUNK_OBJS; i++) {
            T* addr = base + i * n;
            *reinterpret_cast<T**>(addr) = next_free;
            next_free = addr;
         }

         COUNTERS_BLOCK(discard_state_peak_mem_usage)
         {
            BMC::global_bf->bm_stats.discard_state_peak_mem_usage.fetch_add(chunk_bytes);
         }
      }

      T* try_pop()
      {
         if (!next_free)
            return nullptr;

         T* addr = next_free;
         next_free = *reinterpret_cast<T**>(addr);
         return addr;
      }

     public:
      explicit FreeList(u32 n) : n(n) { allocate_new_chunk(); }

      void free(T* addr)
      {
         std::lock_guard _l(m);
         *reinterpret_cast<T**>(addr) = next_free;
         next_free = addr;
      }

      T* allocate()
      {
         std::lock_guard _l(m);
         T* addr = nullptr;
         if ((addr = try_pop()) != nullptr)
            return addr;

         allocate_new_chunk();
         addr = try_pop();
         ensure(addr != nullptr);
         return addr;
      }
   };

   const u32 slabs_count;
   FreeList* lists;

  public:
   CustomSlabAllocator() : slabs_count(BMC::global_bf->max_pending_lsn)
   {
      lists = static_cast<FreeList*>(std::malloc((slabs_count - 1) * sizeof(FreeList)));
      for (u32 i = 2; i <= slabs_count ; ++i) {
         new (&lists[i - 2]) FreeList(i);
      }
   }

   T* allocate(u32 n)
   {
      ensure(n <= slabs_count);
      ensure(n >= 2);
      return lists[n - 2].allocate();
   }

   void free(T* addr, u32 n)
   {
      ensure(n <= slabs_count);
      ensure(n >= 2);
      lists[n - 2].free(addr);
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
