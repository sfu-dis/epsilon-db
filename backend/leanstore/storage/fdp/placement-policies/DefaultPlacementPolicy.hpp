#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace fdp
{
// -------------------------------------------------------------------------------------
class DefaultPlacementPolicy
{
  public:
   DefaultPlacementPolicy() = default;

   void onAllocateNewPage(BufferFrame& bf)
   {
      bf.header.next_ruh = DEFAULT_RUH;
   }

   void onWriteBackDirtyPage(BufferFrame& bf)
   {
      bf.header.next_ruh = DEFAULT_RUH;
   }

   void onFixDiscardedPage(BufferFrame& bf)
   {
      bf.header.next_ruh = DEFAULT_RUH;
   }

};
}  // namespace fdp
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
