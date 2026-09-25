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
