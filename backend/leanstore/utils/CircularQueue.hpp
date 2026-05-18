#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
template <typename T, u64 N>
struct CircularQueue {
   T arr[N];
   u64 head = 0;
   u64 tail = 0;

   bool full() const { return tail - head >= N; }
   bool empty() const { return head == tail; }
   u64 size() const { return tail - head; }

   template <typename... Args>
   T* emplace_back(Args&&... args) {
       if (full()) return nullptr;
       T* slot = &arr[tail++ % N];
       new (slot) T(std::forward<Args>(args)...);
       return slot;
   }

   // Caller must ensure non-empty.
   T& pop_front()
   {
      assert(!empty());
      return arr[head++ % N];
   }

#if 0
   template <typename Pred>
   T* erase_front_while(Pred pred) {
       u64 last_erased_idx = -1;
       while (!empty() && pred(arr[head % N])) {
           last_erased_idx = head++;
       }
       if (last_erased_idx != -1) {
          return &arr[last_erased_idx % N];
       }
       return nullptr;
   }
#endif

   struct iterator {
      CircularQueue* q;
      u64 idx;

      T& operator*() const { return q->arr[idx % N]; }
      T* operator->() const { return &q->arr[idx % N]; }

      iterator& operator++()
      {
         ++idx;
         return *this;
      }
      iterator operator++(int)
      {
         auto tmp = *this;
         ++idx;
         return tmp;
      }

      bool operator==(const iterator& o) const { return idx == o.idx; }
      bool operator!=(const iterator& o) const { return idx != o.idx; }

      void erase()
      {
         assert(idx == q->head && "CircularQueue only supports erasing from the front");
         q->head++;
      }
   };

   iterator begin() { return {this, head}; }
   iterator end() { return {this, tail}; }
};
}  // namespace utils
}  // namespace leanstore
