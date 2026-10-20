#pragma once
#include "HardcodedTPCCPlacementPolicy.hpp"
#include "Units.hpp"
#include "leanstore/storage/btree/core/BTreeNode.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace fdp
{
// -------------------------------------------------------------------------------------
// Record level freezing: the workload flags a record as frozen (the freeze argument of the KVInterface's
// insert and updateSameSizeInPlace) when it will not be updated anymore. The frozen flag lives in the
// record's slot, so it follows the record through splits, merges and redo.
//  - a leaf whose records are all frozen goes to freezing_ruh.
//  - with fdp_frozen_records_tpcc_hot, the TPC-C neworder pages (self invalidating) and the order and orderline
//    pages that are not all frozen yet (they still wait for the delivery update) go to hot_ruh.
//  - everything else (inner nodes, empty leaves, leaves with at least one unfrozen record) goes to DEFAULT_RUH.
// Without fdp_frozen_records_tpcc_hot the policy does not look at the dt_id, it relies on the workload to freeze
// the right records (see frontend/tpc-c/TPCCWorkload.hpp).
// hot_ruh is reclaimed inline by the RU epoch manager thread, without page fixing.
constexpr bool fdp_frozen_records_tpcc_hot = true;
class FrozenRecordsPlacementPolicy
{
  public:
   FrozenRecordsPlacementPolicy()
   {
      freezing_ruh = allocateRUH();
      if constexpr (fdp_frozen_records_tpcc_hot) {
         hot_ruh = allocateRUH();
      }
   }

   void onAllocateNewPage(BufferFrame& bf)
   {
      // The page is empty at this point, the RUH is decided upon write back.
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
   using TPCC = HardcodedTPCCPlacementPolicy;

   static ruh_id_t ruhOf(const BufferFrame::Page& page)
   {
      // Assumes every data structure is a B-Tree (see LeanStore::LeanStore).
      const auto& node = *reinterpret_cast<const btree::BTreeNode*>(page.dt);
      if (node.allFrozen()) {
         return freezing_ruh;
      }
      if constexpr (fdp_frozen_records_tpcc_hot) {
         switch (page.dt_id) {
            case TPCC::TPCC_NEWORDER_DT_ID:
               return hot_ruh;
            case TPCC::TPCC_ORDER_DT_ID:
            case TPCC::TPCC_ORDERLINE_DT_ID:
               return hot_ruh;
            default:
               break;
         }
      }
      return DEFAULT_RUH;
   }
};
}  // namespace fdp
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
