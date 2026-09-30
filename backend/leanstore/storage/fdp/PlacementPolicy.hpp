#pragma once
#include "Macros.hpp"
#include "Units.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
// -------------------------------------------------------------------------------------
#include <memory>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// TODO(mfd) : I can remove the header include later.
struct BufferFrame;  // Forward declaration
namespace fdp
{
// -------------------------------------------------------------------------------------
// RUH IDs, assigned once by the PlacementPolicy constructor (i.e. before any page gets written):
// DEFAULT_RUH first, then the RUHs the placement policy asks for through allocateRUH(), then the
// WAL RUH if the WAL shares the device (FLAGS_log_same_namespace).
// An RUH that is not used by the placement policy stays UNMAPPED_RUH.
extern u32 num_ruhs;  // Data RUHs in use, the WAL RUH excluded.
extern ruh_id_t freezing_ruh;
extern ruh_id_t hot_ruh;
extern ruh_id_t wal_ruh;
// Only for the placement policies' constructors.
ruh_id_t allocateRUH();
// -------------------------------------------------------------------------------------
// Decides which Flexible Data Placement (FDP) placement ID a BufferFrame's page should
// be written to.
// -------------------------------------------------------------------------------------
class PlacementPolicy
{
  public:
   PlacementPolicy();
   ~PlacementPolicy();
   // -------------------------------------------------------------------------------------
   // Not copyable: Impl may end up owning state that must not be silently duplicated.
   DO_NOT_COPY(PlacementPolicy);
   PlacementPolicy(PlacementPolicy&&) noexcept;
   PlacementPolicy& operator=(PlacementPolicy&&) noexcept;
   // -------------------------------------------------------------------------------------
   // Returns the placement ID to use for bf.
   ruh_id_t AssignPlacementID(BufferFrame::Page& page, BufferFrame* bf = nullptr);

   void onAllocateNewPage(BufferFrame& bf);
   void onWriteBackDirtyPage(BufferFrame& bf);
   void onFixDiscardedPage(BufferFrame& bf);
   // -------------------------------------------------------------------------------------
   class Impl;

  private:
   std::unique_ptr<Impl> impl;
};
// -------------------------------------------------------------------------------------
}  // namespace fdp
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
