#include "PlacementPolicy.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "placement-policies/DefaultPlacementPolicy.hpp"
#include "placement-policies/FreezeNewlyAllocatedPlacementPolicy.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace fdp
{
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
   } else {
      throw std::invalid_argument("FLAGS_fdp_placememt_policy");
   }
   UNREACHABLE();
}
}  // namespace
// -------------------------------------------------------------------------------------
PlacementPolicy::PlacementPolicy() : impl(makeImpl()) {}
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

