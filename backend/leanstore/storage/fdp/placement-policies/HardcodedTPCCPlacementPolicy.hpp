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
// TPC-C specific placement:
//  - history and item are frozen from their first write, in freezing_ruh.
//  - neworder pages are self invalidating, all their writes go to hot_ruh.
//  - order and orderline pages get updated once after being inserted (by the delivery
//    transaction), their first write goes to hot_ruh and they are frozen in freezing_ruh
//    from their second write on.
//  - everything else goes to DEFAULT_RUH.
// hot_ruh is reclaimed inline by the RU epoch manager thread, without page fixing.
class HardcodedTPCCPlacementPolicy
{
  public:
   // HACKY(mfd) : TPC-C tables get their dt_id from the order in which they are created
   //  (see frontend/tpc-c/tpcc.cpp).
   static constexpr DTID TPCC_HISTORY_DT_ID = 4;
   static constexpr DTID TPCC_NEWORDER_DT_ID = 5;
   static constexpr DTID TPCC_ORDER_DT_ID = 6;
   static constexpr DTID TPCC_ITEM_DT_ID = 8;
   static constexpr DTID TPCC_ORDERLINE_DT_ID = 10;

   HardcodedTPCCPlacementPolicy()
   {
      freezing_ruh = allocateRUH();
      hot_ruh = allocateRUH();
   }

   ruh_id_t AssignPlacementID(BufferFrame::Page& page, BufferFrame*)
   {
      return ruhOf(page);
   }

   void onAllocateNewPage(BufferFrame& bf)
   {
      // The dt_id is not known yet, the RUH is decided upon write back.
      bf.header.next_ruh = UNMAPPED_RUH;
   }

   void onWriteBackDirtyPage(BufferFrame& bf)
   {
      bf.header.next_ruh = ruhOf(bf.page);
   }

   void onFixDiscardedPage(BufferFrame& bf)
   {
      bf.header.next_ruh = ruhOf(bf.page);
   }

  private:
   // Must be called before the write back increments page.write_back_count.
   static ruh_id_t ruhOf(const BufferFrame::Page& page)
   {
      switch (page.dt_id) {
         case TPCC_HISTORY_DT_ID:
         case TPCC_ITEM_DT_ID:
            return freezing_ruh;
         case TPCC_NEWORDER_DT_ID:
            return hot_ruh;
         case TPCC_ORDER_DT_ID:
         case TPCC_ORDERLINE_DT_ID:
            // write_back_count survives evictions since it is persisted with the page and is
            // only reset upon allocation.
            return page.write_back_count == 0 ? hot_ruh : freezing_ruh;
         default:
            return DEFAULT_RUH;
      }
   }
};
}  // namespace fdp
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
