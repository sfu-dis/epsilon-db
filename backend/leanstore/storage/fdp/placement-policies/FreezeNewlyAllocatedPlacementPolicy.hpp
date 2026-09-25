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
class FreezeNewlyAllocatedPlacementPolicy
{
  public:
   FreezeNewlyAllocatedPlacementPolicy() = default;

   ruh_id_t AssignPlacementID(BufferFrame::Page& page, BufferFrame* bf)
   {
      return DEFAULT_RUH;
   }

   void onAllocateNewPage(BufferFrame& bf)
   {
      bf.header.next_ruh = freezing_ruh;
   }

   void onWriteBackDirtyPage(BufferFrame& bf)
   {
      if (bf.header.next_ruh == UNMAPPED_RUH) {
         bf.header.next_ruh = DEFAULT_RUH;
      }
   }

   void onFixDiscardedPage(BufferFrame& bf)
   {
      ensure_equal(bf.header.next_ruh, UNMAPPED_RUH);
      bf.header.next_ruh = DEFAULT_RUH;
   }

  private:
   ruh_id_t freezing_ruh = ruh_id_t{1};
};
}  // namespace fdp
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
