#include "PlacementPolicy.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "placement-policies/DefaultPlacementPolicy.hpp"
#include "placement-policies/FreezeNewlyAllocatedPlacementPolicy.hpp"
#include "placement-policies/FreezeTablesPlacementPolicy.hpp"
#include "placement-policies/HardcodedTPCCPlacementPolicy.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace fdp
{
// -------------------------------------------------------------------------------------
u32 num_ruhs = 0;
ruh_id_t freezing_ruh = UNMAPPED_RUH;
ruh_id_t hot_ruh = UNMAPPED_RUH;
ruh_id_t wal_ruh = UNMAPPED_RUH;
// -------------------------------------------------------------------------------------
ruh_id_t allocateRUH()
{
   ensure_lt(num_ruhs, MAX_RUHS);
   return static_cast<ruh_id_t>(num_ruhs++);
}
// -------------------------------------------------------------------------------------
class PlacementPolicy::Impl
{
  public:
   virtual ~Impl() = default;
   virtual ruh_id_t AssignPlacementID(BufferFrame::Page& page, BufferFrame* bf = nullptr) = 0;
   // Different Policies have diffirent times to decide the placement ID.
   virtual void onAllocateNewPage(BufferFrame& bf) = 0;
   virtual void onWriteBackDirtyPage(BufferFrame& bf) = 0;
   virtual void onFixDiscardedPage(BufferFrame& bf) = 0;
};
// -------------------------------------------------------------------------------------
namespace
{
template <typename ConcretePolicy>
class PolicyAdapter : public PlacementPolicy::Impl
{
  public:
   ruh_id_t AssignPlacementID(BufferFrame::Page& page, BufferFrame* bf = nullptr) override
   {
      return policy.AssignPlacementID(page, bf);
   }

   void onAllocateNewPage(BufferFrame& bf) override
   {
      policy.onAllocateNewPage(bf);
   }

   void onWriteBackDirtyPage(BufferFrame& bf) override
   {
      policy.onWriteBackDirtyPage(bf);
   }

   void onFixDiscardedPage(BufferFrame& bf) override
   {
      policy.onFixDiscardedPage(bf);
   }

  private:
   ConcretePolicy policy;
};
// -------------------------------------------------------------------------------------
std::unique_ptr<PlacementPolicy::Impl> makeImpl()
{
   if(FLAGS_fdp_placement_policy == "Default") {
      return std::make_unique<PolicyAdapter<DefaultPlacementPolicy>>();
   } else if (FLAGS_fdp_placement_policy == "FreezeNewlyAllocated") {
      return std::make_unique<PolicyAdapter<FreezeNewlyAllocatedPlacementPolicy>>();
   } else if (FLAGS_fdp_placement_policy == "FreezeTables") {
      return std::make_unique<PolicyAdapter<FreezeTablesPlacementPolicy>>();
   } else if (FLAGS_fdp_placement_policy == "HardcodedTPCC") {
      return std::make_unique<PolicyAdapter<HardcodedTPCCPlacementPolicy>>();
   } else {
      throw std::invalid_argument("FLAGS_fdp_placememt_policy");
   }
   UNREACHABLE();
}
// -------------------------------------------------------------------------------------
std::unique_ptr<PlacementPolicy::Impl> assignRUHsAndMakeImpl()
{
   num_ruhs = 0;
   freezing_ruh = hot_ruh = wal_ruh = UNMAPPED_RUH;
   ensure_equal(allocateRUH(), DEFAULT_RUH);
   auto impl = makeImpl();
   // The WAL is not accounted in num_ruhs, it just takes the next RUH ID.
   if (FLAGS_log_same_namespace) {
      wal_ruh = static_cast<ruh_id_t>(num_ruhs);
   }
   return impl;
}
}  // namespace
// -------------------------------------------------------------------------------------
PlacementPolicy::PlacementPolicy() : impl(assignRUHsAndMakeImpl()) {}
PlacementPolicy::~PlacementPolicy() = default;
PlacementPolicy::PlacementPolicy(PlacementPolicy&&) noexcept = default;
PlacementPolicy& PlacementPolicy::operator=(PlacementPolicy&&) noexcept = default;
// -------------------------------------------------------------------------------------
ruh_id_t PlacementPolicy::AssignPlacementID(BufferFrame::Page& page, BufferFrame* bf)
{
   return impl->AssignPlacementID(page, bf);
}
// -------------------------------------------------------------------------------------
void PlacementPolicy::onAllocateNewPage(BufferFrame& bf)
{
   return impl->onAllocateNewPage(bf);
}
// -------------------------------------------------------------------------------------
void PlacementPolicy::onWriteBackDirtyPage(BufferFrame& bf)
{
   return impl->onWriteBackDirtyPage(bf);
}
// -------------------------------------------------------------------------------------
void PlacementPolicy::onFixDiscardedPage(BufferFrame& bf)
{
   return impl->onFixDiscardedPage(bf);
}
// -------------------------------------------------------------------------------------
}  // namespace fdp
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------

