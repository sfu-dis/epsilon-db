#pragma once
// -------------------------------------------------------------------------------------
#include "BufferFrame.hpp"
#include "Units.hpp"
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct BufferFrame;  // Forward declaration
// -------------------------------------------------------------------------------------
template <typename T>
class Swip
{
   // -------------------------------------------------------------------------------------
   // 1xxxxxxxxxxxx evicted, 01xxxxxxxxxxx cooling, 00xxxxxxxxxxx hot
   static const u64 evicted_bit = u64(1) << 63;
   static const u64 evicted_mask = ~(u64(1) << 63);
   static const u64 cool_bit = u64(1) << 62;
   static const u64 cool_mask = ~(u64(1) << 62);
   static const u64 hot_mask = ~(u64(3) << 62);
   static const u64 dirty_bit = u64(1) << 61;
   static const u64 dirty_mask = ~(u64(1) << 61);
   static_assert(evicted_bit == 0x8000000000000000, "");
   static_assert(dirty_bit == 0x2000000000000000, "");
   static_assert(evicted_mask == 0x7FFFFFFFFFFFFFFF, "");
   static_assert(hot_mask == 0x3FFFFFFFFFFFFFFF, "");

   struct PageID
   {
      u32 page_id; // 32 bits enough for 16 TiB databases with 4K pages.
      u32 meta;    // 3 status bits + RU epoch
   };

   static_assert(sizeof(PageID) == sizeof(u64), "");

   union {
      u64 pid;
      PageID pid2;
      BufferFrame* bf;
   };
  public:
   // -------------------------------------------------------------------------------------
   Swip() = default;
   Swip(BufferFrame* bf) : bf(bf) {}
   template <typename T2>
   Swip(Swip<T2>& other) : pid(other.pid)
   {
   }
   // -------------------------------------------------------------------------------------
   bool operator==(const Swip& other) const { return (raw() == other.raw()); }
   // -------------------------------------------------------------------------------------
   bool isHOT() { return (pid & (evicted_bit | cool_bit)) == 0; }
   bool isCOOL() { return pid & cool_bit; }
   bool isEVICTED() { return pid & evicted_bit; }
   bool isDIRTY() { return pid & dirty_bit; }
   // -------------------------------------------------------------------------------------
   u64 asPageID() { 
      return pid2.page_id;
   }
   BufferFrame& asBufferFrame() { return *bf; }
   BufferFrame& asBufferFrameMasked() { return *reinterpret_cast<BufferFrame*>(pid & hot_mask); }
   u64 raw() const { return pid; }
   u32 ru_epoch()
   {
      ensure((pid2.meta & 0x80000000) == 0x80000000);
      return pid2.meta & 0x1FFFFFFF;
   }
   // -------------------------------------------------------------------------------------
   template <typename T2>
   void warm(T2* bf)
   {
      assert(isEVICTED());
      this->bf = bf;
   }
   void warm()
   {
      assert(isCOOL());
      this->pid = pid & ~cool_bit;
   }
   // -------------------------------------------------------------------------------------
   void cool() { this->pid = pid | cool_bit; }
   // -------------------------------------------------------------------------------------
   void evict(PID pid, u64 ru_epoch)
   { 
      // this->pid = pid | evicted_bit;
      ensure((pid & ~(0xFFFFFFFF)) == 0);
      ensure((ru_epoch & ~(0x1FFFFFFF)) == 0);
      this->pid2.page_id = pid;
      this->pid2.meta = (ru_epoch | 0x80000000);
   }
   void evictAndMarkDirty(PID pid, u64 ru_epoch)
   { 
      evict(pid, ru_epoch);
      this->pid2.meta |= 0x20000000;
      // this->pid = (pid | evicted_bit | dirty_bit);
   }
   // -------------------------------------------------------------------------------------
   template <typename T2>
   Swip<T2>& cast()
   {
      return *reinterpret_cast<Swip<T2>*>(this);
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
