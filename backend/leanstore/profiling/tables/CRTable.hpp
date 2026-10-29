#pragma once
#include "ProfilingTable.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
class CRTable : public ProfilingTable
{
  private:
   u64 wal_hits, wal_miss;
   double p1, p2, total, write, wal_total, wal_hit_pct, wal_miss_pct;
   u64 gct_blocks, gct_blocks_full;

  public:
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
