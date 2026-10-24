#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <string>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Physical layout of the persistent storage, all offsets and sizes are in bytes.
//
//  ssd_fd : [ database pages | persistant RU state | (log region, iff log_same_namespace) ]
//             ^ db_start       ^ persistant_ru_state_offset   ^ log_offset
//
// When the log lives on a separate device (--redo_log_file), log_offset is relative to
// that device and is 0, log_size is the size of that device (0 if WAL is disabled).
struct SSDLayout {
   u64 db_start = 0;
   u64 db_size = 0;  // database pages only, [db_start, db_start + db_size)
   u64 persistant_ru_state_offset = 0;
   u64 log_offset = 0;
   u64 log_size = 0;
   bool log_same_namespace = false;
   // -------------------------------------------------------------------------------------
   bool operator==(const SSDLayout&) const = default;
   u64 dbPages() const;
   std::string toString() const;
   // -------------------------------------------------------------------------------------
   std::unordered_map<std::string, std::string> serialize() const;
   static SSDLayout deserialize(std::unordered_map<std::string, std::string> map);
   // Returns an empty string if a database persisted with `persisted` can be reopened with this
   // layout, otherwise a description of the mismatch. With a separate log device the log fields
   // are not compared: the log is not part of ssd_fd and loading usually runs with --nowal.
   std::string incompatibilityWith(const SSDLayout& persisted) const;
};
// -------------------------------------------------------------------------------------
struct SSDLayoutConfig {
   u64 ssd_bytes = 0;
   double ssd_gib = 0;  // requested database size, 0 = everything available
   bool log_same_namespace = false;
   double log_space_pct = 10.0;
   u64 log_device_bytes = 0;  // separate log device only, 0 = unknown or WAL disabled
   u64 ru_size = 0;  // in 4KiB pages
   u64 overprovisioning_ru_epochs = 0;  // 0 = derive from overprovisioning_ratio
   double overprovisioning_ratio = 0;
   bool lba_chain_heads = false;  // the persistant RU state holds the LBA chain heads, see PersistantRUState
};
// -------------------------------------------------------------------------------------
struct SSDLayoutPlan {
   SSDLayout layout;
   u64 max_open_ru_epochs = 0;
   u64 overprovisioning_ru_epochs = 0;
   double ssd_gib = 0;
};
// -------------------------------------------------------------------------------------
// Throws leanstore::ex::GenericException on an impossible configuration.
SSDLayoutPlan computeSSDLayout(const SSDLayoutConfig& config);
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore

