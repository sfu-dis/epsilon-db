#include "LeanStore.hpp"

#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
// -------------------------------------------------------------------------------------
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <termios.h>
#include <unistd.h>

#include <locale>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace rs = rapidjson;
namespace leanstore
{
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
   // LeanStore::addStringFlag("ssd_path", &FLAGS_ssd_path);
   if (FLAGS_recover_file != "./leanstore.json") {
      FLAGS_recover = true;
   }
   if (FLAGS_persist_file != "./leanstore.json") {
      FLAGS_persist = true;
   }
   if (FLAGS_recover) {
      deserializeFlags();
   }
   // -------------------------------------------------------------------------------------
   // Check if configurations make sense
   if ((FLAGS_vi) && !FLAGS_wal) {
      SetupFailed("You have to enable WAL");
   }
   if (FLAGS_isolation_level == "si" && (!FLAGS_mv | !FLAGS_vi)) {
      SetupFailed("You have to enable mv an vi (multi-versioning)");
   }
   if (!FLAGS_wal && FLAGS_wal_pwrite) {
      SetupFailed("You have to enable wal or turn of wal_pwrite");
   }
   FLAGS_wal_buffer_size = utils::upAlign(FLAGS_wal_buffer_size, 4096);
   if (FLAGS_enable_discarding && !(FLAGS_wal_pwrite || FLAGS_fake_log_reapply)) {
      SetupFailed("You have to enable wal_pwrite if you want to enable discarding.");
   }
   if (FLAGS_enable_discarding) {
      if (FLAGS_ru_gc_threads == 0) {
         SetupFailed("You have to create at least one RU GC thread to enable discarding");
      }
      if (FLAGS_wal_partition_by != "ru_epoch") {
         SetupFailed("You have to partition the log by ru_epoch to enable discarding");
      }
      if (FLAGS_contention_split) {
         SetupFailed("Contention Split is not tested with discarding enabled, please turn it off!");
      }
   }
   if (FLAGS_ru_size == 0) {
      SetupFailed("Please specify an RU size in units of number of 4KiB pages");
   }
   if (FLAGS_opportunistic_log_compaction && !FLAGS_per_page_logging) {
      SetupFailed("Opportunistic Log Compaction is only implemented when PPL is enabled");
   }
   if (FLAGS_per_page_logging && !FLAGS_enable_discarding) {
      // per page logging is only relevant when discarding is enabled, silently turn it off.
      FLAGS_per_page_logging = false;
   }
   // -------------------------------------------------------------------------------------
   logger = std::make_unique<utils::Logger>("leanstore_log.txt", true);
   // -------------------------------------------------------------------------------------
   // Set the default logger to file logger
   // Init SSD pool
   int flags = O_RDWR | O_DIRECT;
   if (FLAGS_trunc) {
      flags |= O_TRUNC | O_CREAT;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   if (ssd_fd == -1) {
      perror("posix error");
      std::cout << "path: " << FLAGS_ssd_path << std::endl;
      SetupFailed("Could not open the file or the SSD block device");
   }
   ensure(fcntl(ssd_fd, F_GETFL) != -1);
   // -------------------------------------------------------------------------------------
   u64 ssd_size_in_bytes;
   if (ioctl(ssd_fd, BLKGETSIZE64, &ssd_size_in_bytes) == 0) {
      LOG_INFO(logger, "SSD size: %.2f GiB", ssd_size_in_bytes / 1073741824.0);
   } else {
      perror("ioctl");
   }
   if (FLAGS_ssd_gib == 0) {
      FLAGS_ssd_gib = ssd_size_in_bytes / 1073741824;
   }
   u64 total_blocks_in_ssd = (FLAGS_ssd_gib * 1048576) / 4;
   BufferManager::RU_SIZE = FLAGS_ru_size;
   u64 max_open_ru_epochs = total_blocks_in_ssd / BufferManager::RU_SIZE;
   if (FLAGS_overprovisioning_ru_epochs == 0) {
      FLAGS_overprovisioning_ru_epochs = max_open_ru_epochs * FLAGS_overprovisioning_ratio;
   }
   max_open_ru_epochs += FLAGS_overprovisioning_ru_epochs;
   LOG_INFO(logger, "Number of Over Provisioning Reclaim Units : %lu", FLAGS_overprovisioning_ru_epochs);
   LOG_INFO(logger, "Number of Physical Reclaim Units : %lu", max_open_ru_epochs);
   if (FLAGS_background_page_fixer_variant == 2) {
      const u64 ru_epoch_pids_size = utils::upAlign(sizeof(BufferManager::ru_epoch_pids) + 2 * FLAGS_ru_size * sizeof(PID), PAGE_SIZE);
      const u64 space_for_ru_epoch_pids = ru_epoch_pids_size * max_open_ru_epochs;
      total_blocks_in_ssd -= (space_for_ru_epoch_pids/4096);
      // store the offset to pids array.
      LOG_INFO(logger, "Per RU pids list size %.2f MiB, Total : %.2f GiB", ru_epoch_pids_size / 1048576.0, space_for_ru_epoch_pids / 1073741824.0);
   }
   u64 persistant_state_blocks = utils::upAlign(sizeof(BufferManager::PersistantRUState) + max_open_ru_epochs * sizeof(u32), 4096) / 4096;
   total_blocks_in_ssd -= persistant_state_blocks;
   // Adjust ssd_gib
   FLAGS_ssd_gib = (total_blocks_in_ssd * 4) / 1048576;
   LOG_INFO(logger, "Space reserved for database pages: %.2f GiB", FLAGS_ssd_gib);
   // -------------------------------------------------------------------------------------
   buffer_manager = make_unique<storage::BufferManager>(ssd_fd, total_blocks_in_ssd, max_open_ru_epochs);
   ensure_equal(BMC::global_bf, buffer_manager.get());
   BMC::global_bf = buffer_manager.get();
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal_partition_by == "ru_epoch") {
      FLAGS_wal_partitions_count = max_open_ru_epochs + FLAGS_wal_sink_logs;
      if (FLAGS_wal_partitions_count > 1023) {
         SetupFailed("Too many RUs, please verify the size of the RU epoch");
      }
      LOG_INFO(logger, "Number of Log partitions : %lu", FLAGS_wal_partitions_count);
   }
   // -------------------------------------------------------------------------------------
   DTRegistry::global_dt_registry.registerDatastructureType(0, storage::btree::BTreeLL::getMeta());
   DTRegistry::global_dt_registry.registerDatastructureType(2, storage::btree::BTreeVI::getMeta());
   // -------------------------------------------------------------------------------------
   if (FLAGS_recover) {
      deserializeState();
   }
   // -------------------------------------------------------------------------------------
   history_tree = std::make_unique<cr::HistoryTree>();
   u64 log_device_size;
   if (FLAGS_wal) {
      ensure(FLAGS_redo_log_file != "");
      ensure(FLAGS_redo_log_file != FLAGS_ssd_path);
      log_dev_fd = open(FLAGS_redo_log_file.c_str(), O_RDWR | O_DIRECT);
      ensure(log_dev_fd > 0);

      if (FLAGS_log_dev_size_gib == 0) {
         if (ioctl(log_dev_fd, BLKGETSIZE64, &log_device_size) == 0) {
            log_device_size = utils::downAlign(log_device_size, 4096);
            LOG_INFO(logger, "Log device size: %lu bytes", log_device_size);
            ensure((log_device_size % 4096) == 0);
         } else {
            perror("ioctl");
         }
      } else {
         log_device_size = FLAGS_log_dev_size_gib * 1073741824ul;
      }
      if (FLAGS_log_same_device_fdp) {
         // ATTENTION : Not Portable, requires kernel patch with FDP support.
         u64 hint = 1;
         int ret = fcntl(log_dev_fd, F_SET_RW_HINT, &hint);
         ensure_equal(ret, 0);
         LOG_INFO(logger, "set the placement-id (%ld) to log file\n", hint);
      }
   }
   cr_manager = make_unique<cr::CRManager>(*history_tree.get(), ssd_fd, log_dev_fd, log_device_size);
   cr::CRManager::global = cr_manager.get();
   if (FLAGS_vi) {
      cr_manager->scheduleJobSync(0, [&]() {
         history_tree->update_btrees = std::make_unique<leanstore::storage::btree::BTreeLL*[]>(FLAGS_worker_threads);
         history_tree->remove_btrees = std::make_unique<leanstore::storage::btree::BTreeLL*[]>(FLAGS_worker_threads);
         for (u64 w_i = 0; w_i < FLAGS_worker_threads; w_i++) {
            std::string name = "_history_tree_" + std::to_string(w_i);
            history_tree->update_btrees[w_i] = &registerBTreeLL(name + "_updates", {.enable_wal = false, .use_bulk_insert = true});
            history_tree->remove_btrees[w_i] = &registerBTreeLL(name + "_removes", {.enable_wal = false, .use_bulk_insert = true});
         }
      });
   }
   // -------------------------------------------------------------------------------------
   buffer_manager->startBackgroundThreads();
}
// -------------------------------------------------------------------------------------
void LeanStore::startProfilingThread()
{
   std::thread profiling_thread([this]() { this->profilingThread(); });
   bg_threads_counter++;
   profiling_thread.detach();
}
// -------------------------------------------------------------------------------------
storage::btree::BTreeLL& LeanStore::registerBTreeLL(string name, storage::btree::BTreeGeneric::Config config)
{
   assert(btrees_ll.find(name) == btrees_ll.end());
   auto& btree = btrees_ll[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(0, reinterpret_cast<void*>(&btree), name);
   btree.create(dtid, config);
   return btree;
}

// -------------------------------------------------------------------------------------
storage::btree::BTreeVI& LeanStore::registerBTreeVI(string name, storage::btree::BTreeLL::Config config)
{
   assert(btrees_vi.find(name) == btrees_vi.end());
   auto& btree = btrees_vi[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(2, reinterpret_cast<void*>(&btree), name);
   auto& graveyard_btree = registerBTreeLL("_" + name + "_graveyard", {.enable_wal = false, .use_bulk_insert = false});
   btree.create(dtid, config, &graveyard_btree);
   return btree;
}
// -------------------------------------------------------------------------------------
u64 LeanStore::getConfigHash()
{
   return config_hash;
}
// -------------------------------------------------------------------------------------
LeanStore::GlobalStats LeanStore::getGlobalStats()
{
   return global_stats;
}
// -------------------------------------------------------------------------------------
void LeanStore::serializeState()
{
   // Serialize data structure instances
   std::ofstream json_file;
   json_file.open(FLAGS_persist_file, ios::trunc);
   rs::Document d;
   rs::Document::AllocatorType& allocator = d.GetAllocator();
   d.SetObject();
   // -------------------------------------------------------------------------------------
   std::unordered_map<std::string, std::string> serialized_cr_map = cr_manager->serialize();
   rs::Value cr_serialized(rs::kObjectType);
   for (const auto& [key, value] : serialized_cr_map) {
      rs::Value k, v;
      k.SetString(key.c_str(), key.length(), allocator);
      v.SetString(value.c_str(), value.length(), allocator);
      cr_serialized.AddMember(k, v, allocator);
   }
   d.AddMember("cr_manager", cr_serialized, allocator);
   // -------------------------------------------------------------------------------------
   std::unordered_map<std::string, std::string> serialized_bm_map = buffer_manager->serialize();
   rs::Value bm_serialized(rs::kObjectType);
   for (const auto& [key, value] : serialized_bm_map) {
      rs::Value k, v;
      k.SetString(key.c_str(), key.length(), allocator);
      v.SetString(value.c_str(), value.length(), allocator);
      bm_serialized.AddMember(k, v, allocator);
   }
   d.AddMember("buffer_manager", bm_serialized, allocator);
   // -------------------------------------------------------------------------------------
   rs::Value dts(rs::kArrayType);
   for (auto& dt : DTRegistry::global_dt_registry.dt_instances_ht) {
      if (std::get<2>(dt.second).substr(0, 1) == "_") {
         continue;
      }
      rs::Value dt_json_object(rs::kObjectType);
      const DTID dt_id = dt.first;
      rs::Value name;
      name.SetString(std::get<2>(dt.second).c_str(), std::get<2>(dt.second).length(), allocator);
      dt_json_object.AddMember("name", name, allocator);
      dt_json_object.AddMember("type", rs::Value(std::get<0>(dt.second)), allocator);
      dt_json_object.AddMember("id", rs::Value(dt_id), allocator);
      // -------------------------------------------------------------------------------------
      std::unordered_map<std::string, std::string> serialized_dt_map = DTRegistry::global_dt_registry.serialize(dt_id);
      rs::Value dt_serialized(rs::kObjectType);
      for (const auto& [key, value] : serialized_dt_map) {
         rs::Value k, v;
         k.SetString(key.c_str(), key.length(), allocator);
         v.SetString(value.c_str(), value.length(), allocator);
         dt_serialized.AddMember(k, v, allocator);
      }
      dt_json_object.AddMember("serialized", dt_serialized, allocator);
      // -------------------------------------------------------------------------------------
      dts.PushBack(dt_json_object, allocator);
   }
   d.AddMember("registered_datastructures", dts, allocator);
   // -------------------------------------------------------------------------------------
   serializeFlags(d);
   rs::StringBuffer sb;
   rs::PrettyWriter<rs::StringBuffer> writer(sb);
   d.Accept(writer);
   json_file << sb.GetString();
}
// -------------------------------------------------------------------------------------
void LeanStore::serializeFlags(rs::Document& d)
{
   rs::Value flags_serialized(rs::kObjectType);
   rs::Document::AllocatorType& allocator = d.GetAllocator();
   for (auto flags : persisted_string_flags) {
      rs::Value name(std::get<0>(flags).c_str(), std::get<0>(flags).length(), allocator);
      rs::Value value;
      value.SetString((*std::get<1>(flags)).c_str(), (*std::get<1>(flags)).length(), allocator);
      flags_serialized.AddMember(name, value, allocator);
   }
   for (auto flags : persisted_s64_flags) {
      rs::Value name(std::get<0>(flags).c_str(), std::get<0>(flags).length(), allocator);
      string value_string = std::to_string(*std::get<1>(flags));
      rs::Value value;
      value.SetString(value_string.c_str(), value_string.length(), d.GetAllocator());
      flags_serialized.AddMember(name, value, allocator);
   }
   d.AddMember("flags", flags_serialized, allocator);
}
// -------------------------------------------------------------------------------------
void LeanStore::deserializeState()
{
   std::ifstream json_file;
   json_file.open(FLAGS_recover_file);
   rs::IStreamWrapper isw(json_file);
   rs::Document d;
   d.ParseStream(isw);
   // -------------------------------------------------------------------------------------
   const rs::Value& cr = d["cr_manager"];
   std::unordered_map<std::string, std::string> serialized_cr_map;
   for (rs::Value::ConstMemberIterator itr = cr.MemberBegin(); itr != cr.MemberEnd(); ++itr) {
      serialized_cr_map[itr->name.GetString()] = itr->value.GetString();
   }
   cr_manager->deserialize(serialized_cr_map);
   // -------------------------------------------------------------------------------------
   const rs::Value& bm = d["buffer_manager"];
   std::unordered_map<std::string, std::string> serialized_bm_map;
   for (rs::Value::ConstMemberIterator itr = bm.MemberBegin(); itr != bm.MemberEnd(); ++itr) {
      serialized_bm_map[itr->name.GetString()] = itr->value.GetString();
   }
   buffer_manager->deserialize(serialized_bm_map);
   // -------------------------------------------------------------------------------------
   const rs::Value& dts = d["registered_datastructures"];
   assert(dts.IsArray());
   for (auto& dt : dts.GetArray()) {
      assert(dt.IsObject());
      const DTID dt_id = dt["id"].GetInt();
      const DTType dt_type = dt["type"].GetInt();
      const std::string dt_name = dt["name"].GetString();
      std::unordered_map<std::string, std::string> serialized_dt_map;
      const rs::Value& serialized_object = dt["serialized"];
      for (rs::Value::ConstMemberIterator itr = serialized_object.MemberBegin(); itr != serialized_object.MemberEnd(); ++itr) {
         serialized_dt_map[itr->name.GetString()] = itr->value.GetString();
      }
      // -------------------------------------------------------------------------------------
      if (dt_type == 0) {
         auto& btree = btrees_ll[dt_name];
         DTRegistry::global_dt_registry.registerDatastructureInstance(0, reinterpret_cast<void*>(&btree), dt_name, dt_id);
      } else if (dt_type == 2) {
         auto& btree = btrees_vi[dt_name];
         DTRegistry::global_dt_registry.registerDatastructureInstance(2, reinterpret_cast<void*>(&btree), dt_name, dt_id);
      } else {
         UNREACHABLE();
      }
      DTRegistry::global_dt_registry.deserialize(dt_id, serialized_dt_map);
   }
}
// -------------------------------------------------------------------------------------
void LeanStore::deserializeFlags()
{
   std::ifstream json_file;
   json_file.open(FLAGS_recover_file);
   rs::IStreamWrapper isw(json_file);
   rs::Document d;
   d.ParseStream(isw);
   // -------------------------------------------------------------------------------------
   const rs::Value& flags = d["flags"];
   std::unordered_map<std::string, std::string> flags_serialized;
   for (rs::Value::ConstMemberIterator itr = flags.MemberBegin(); itr != flags.MemberEnd(); ++itr) {
      flags_serialized[itr->name.GetString()] = itr->value.GetString();
   }
   for (auto flags : persisted_string_flags) {
      *std::get<1>(flags) = flags_serialized[std::get<0>(flags)];
   }
   for (auto flags : persisted_s64_flags) {
      *std::get<1>(flags) = atoi(flags_serialized[std::get<0>(flags)].c_str());
   }
}
// -------------------------------------------------------------------------------------
LeanStore::~LeanStore()
{
   if (FLAGS_btree_print_height || FLAGS_btree_print_tuples_count) {
      cr_manager->joinAll();
      for (auto& iter : btrees_ll) {
         if (iter.first.substr(0, 1) == "_") {
            continue;
         }
         cout << "BTreeLL: " << iter.first << ", dt_id= " << iter.second.dt_id << ", height= " << iter.second.height;
         if (FLAGS_btree_print_tuples_count) {
            cr_manager->scheduleJobSync(0, [&]() { cout << ", #tuples= " << iter.second.countEntries() << endl; });
         } else {
            cout << endl;
         }
      }
      for (auto& iter : btrees_vi) {
         cout << "BTreeVI: " << iter.first << ", dt_id= " << iter.second.dt_id << ", height= " << iter.second.height;
         if (FLAGS_btree_print_tuples_count) {
            cr_manager->scheduleJobSync(0, [&]() { cout << ", #tuples= " << iter.second.countEntries() << endl; });
         } else {
            cout << endl;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   bg_threads_keep_running = false;
   buffer_manager->stopBackgroundThreads();
   while (bg_threads_counter) {
   }
   if (FLAGS_persist) {
      serializeState();
      buffer_manager->writeAllBufferFrames();
   }
}
// -------------------------------------------------------------------------------------
// Static members
std::list<std::tuple<string, fLS::clstring*>> LeanStore::persisted_string_flags = {};
std::list<std::tuple<string, s64*>> LeanStore::persisted_s64_flags = {};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
