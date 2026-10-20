#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <array>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace fdp
{
// -------------------------------------------------------------------------------------
// Places the pages of the frozen data structures (see freeze_dt()) in freezing_ruh and
// everything else in DEFAULT_RUH.
class FreezeTablesPlacementPolicy
{
  public:
   // HACKY(mfd) : TPC-C tables get their dt_id from the order in which they are created
   //  (see frontend/tpc-c/tpcc.cpp).
   static constexpr DTID TPCC_HISTORY_DT_ID = 4;
   static constexpr DTID TPCC_ORDER_DT_ID = 6;
   static constexpr DTID TPCC_ITEM_DT_ID = 8;
   static constexpr DTID TPCC_ORDERLINE_DT_ID = 10;

   FreezeTablesPlacementPolicy()
   {
      freezing_ruh = allocateRUH();
      freeze_dt(TPCC_HISTORY_DT_ID);
      freeze_dt(TPCC_ORDER_DT_ID);
      freeze_dt(TPCC_ITEM_DT_ID);
      freeze_dt(TPCC_ORDERLINE_DT_ID);
   }

   // Not synchronized, must be called before pages of dt_id get written back.
   void freeze_dt(DTID dt_id)
   {
      ensure(dt_id >= 0 && dt_id < MAX_DT_ID);
      frozen[dt_id] = true;
   }

   void onAllocateNewPage(BufferFrame& bf)
   {
      bf.header.next_ruh = UNMAPPED_RUH;
   }

   void onWriteBackDirtyPage(BufferFrame& bf)
   {
      bf.header.next_ruh = ruhOf(bf.page.dt_id);
   }

   void onFixDiscardedPage(BufferFrame& bf)
   {
      bf.header.next_ruh = ruhOf(bf.page.dt_id);
   }

  private:
   std::array<bool, MAX_DT_ID> frozen{};

   ruh_id_t ruhOf(DTID dt_id) const
   {
      ensure(dt_id >= 0 && dt_id < MAX_DT_ID);
      return frozen[dt_id] ? freezing_ruh : DEFAULT_RUH;
   }
};
}  // namespace fdp
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
