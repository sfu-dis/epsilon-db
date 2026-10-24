#include "SSDLayout.hpp"

#include "Exceptions.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "leanstore/storage/buffer-manager/RUEpochDiscardSet.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <sstream>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
static constexpr double GIB = 1073741824.0;
// -------------------------------------------------------------------------------------
u64 SSDLayout::dbPages() const
{
   return db_size / PAGE_SIZE;
}
// -------------------------------------------------------------------------------------
std::string SSDLayout::toString() const
{
   std::stringstream ss;
   ss << "db = [" << db_start << ", " << db_start + db_size << ")"
      << ", persistant_ru_state_offset = " << persistant_ru_state_offset << ", log = [" << log_offset << ", " << log_offset + log_size
      << ")" << (log_same_namespace ? " on ssd_path" : " on redo_log_file");
   return ss.str();
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> SSDLayout::serialize() const
{
   std::unordered_map<std::string, std::string> map;
   map["db_start"] = std::to_string(db_start);
   map["db_size"] = std::to_string(db_size);
   map["persistant_ru_state_offset"] = std::to_string(persistant_ru_state_offset);
   map["log_offset"] = std::to_string(log_offset);
   map["log_size"] = std::to_string(log_size);
   map["log_same_namespace"] = log_same_namespace ? "1" : "0";
   return map;
}
// -------------------------------------------------------------------------------------
SSDLayout SSDLayout::deserialize(std::unordered_map<std::string, std::string> map)
{
   SSDLayout layout;
   layout.db_start = std::stoull(map.at("db_start"));
   layout.db_size = std::stoull(map.at("db_size"));
   layout.persistant_ru_state_offset = std::stoull(map.at("persistant_ru_state_offset"));
   layout.log_offset = std::stoull(map.at("log_offset"));
   layout.log_size = std::stoull(map.at("log_size"));
   layout.log_same_namespace = map.at("log_same_namespace") == "1";
   return layout;
}
// -------------------------------------------------------------------------------------
std::string SSDLayout::incompatibilityWith(const SSDLayout& persisted) const
{
   std::stringstream ss;
   auto cmp = [&](const char* name, u64 now, u64 then) {
      if (now != then) {
         ss << name << ": persisted " << then << ", now " << now << "; ";
      }
   };
   cmp("log_same_namespace", log_same_namespace, persisted.log_same_namespace);
   cmp("db_start", db_start, persisted.db_start);
   cmp("db_size", db_size, persisted.db_size);
   cmp("persistant_ru_state_offset", persistant_ru_state_offset, persisted.persistant_ru_state_offset);
   if (log_same_namespace && persisted.log_same_namespace) {
      cmp("log_offset", log_offset, persisted.log_offset);
      cmp("log_size", log_size, persisted.log_size);
   }
   return ss.str();
}
// -------------------------------------------------------------------------------------
SSDLayoutPlan computeSSDLayout(const SSDLayoutConfig& config)
{
   SSDLayoutPlan plan;
   SSDLayout& layout = plan.layout;
   const u64 ssd_bytes = utils::downAlign(config.ssd_bytes, PAGE_SIZE);
   layout.log_same_namespace = config.log_same_namespace;
   // -------------------------------------------------------------------------------------
   // 1. Carve the log out of the tail of the device.
   u64 db_region_end = ssd_bytes;  // everything below is for pages + persistant RU state
   if (config.log_same_namespace) {
      if (ssd_bytes == 0) {
         SetupFailed("log_same_namespace requires the size of ssd_path, is it a block device? ");
      }
      if (config.log_space_pct <= 0 || config.log_space_pct >= 100) {
         SetupFailed("log_space_pct must be in (0, 100), 10%% is good");
      }
      layout.log_size = utils::downAlign(static_cast<u64>(ssd_bytes * (config.log_space_pct / 100.0)), PAGE_SIZE);
      if (layout.log_size == 0) {
         SetupFailed("log_space_pct leaves no space for the log ");
      }
      layout.log_offset = ssd_bytes - layout.log_size;
      db_region_end = layout.log_offset;
      if (config.ssd_gib * GIB > db_region_end) {
         SetupFailed("ssd_gib overlaps with the log region, lower ssd_gib/log_space_pct or pass ssd_gib=0 ");
      }
   } else {
      layout.log_offset = 0;
      layout.log_size = utils::downAlign(config.log_device_bytes, PAGE_SIZE);
   }
   // -------------------------------------------------------------------------------------
   // 2. Database region, the persistant RU state is taken from its end.
   double ssd_gib = config.ssd_gib;
   if (ssd_gib == 0) {
      ssd_gib = config.log_same_namespace ? db_region_end / GIB : static_cast<double>(ssd_bytes / 1073741824);
   }
   if (ssd_gib <= 0) {
      SetupFailed("Could not determine the database size, pass ssd_gib ");
   }
   if (ssd_bytes != 0 && ssd_gib * GIB > ssd_bytes) {
      SetupFailed("ssd_gib is larger than ssd_path ");
   }
   if (config.ru_size == 0) {
      SetupFailed("Please specify an RU size in units of number of 4KiB pages ");
   }
   u64 total_blocks = static_cast<u64>((ssd_gib * GIB) / PAGE_SIZE);
   u64 max_open_ru_epochs = total_blocks / config.ru_size;
   plan.overprovisioning_ru_epochs = config.overprovisioning_ru_epochs;
   if (plan.overprovisioning_ru_epochs == 0) {
      plan.overprovisioning_ru_epochs = static_cast<u32>(max_open_ru_epochs * config.overprovisioning_ratio);
   }
   max_open_ru_epochs += plan.overprovisioning_ru_epochs;
   plan.max_open_ru_epochs = max_open_ru_epochs;
   const u64 persistant_state_blocks = utils::upAlign(PersistantRUState::sizeFor(max_open_ru_epochs, config.lba_chain_heads), PAGE_SIZE) / PAGE_SIZE;
   if (total_blocks <= persistant_state_blocks * 100) {
      SetupFailed("Database region too small for the persistant RU state ");
   }
   total_blocks -= persistant_state_blocks;
   plan.ssd_gib = (total_blocks * PAGE_SIZE) / GIB;
   // -------------------------------------------------------------------------------------
   layout.db_start = 0;
   layout.db_size = total_blocks * PAGE_SIZE;
   layout.persistant_ru_state_offset = layout.db_start + layout.db_size;
   const u64 persistant_state_end = layout.persistant_ru_state_offset + persistant_state_blocks * PAGE_SIZE;
   if (config.log_same_namespace && persistant_state_end > layout.log_offset) {
      SetupFailed("Persistant RU state overlaps with the log region ");
   }
   return plan;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
