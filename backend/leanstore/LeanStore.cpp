#include "LeanStore.hpp"

#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/profiling/tables/LatencyTable.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "tabulate/table.hpp"
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
#include <fdp.h>
// -------------------------------------------------------------------------------------
using namespace tabulate;
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
   // -------------------------------------------------------------------------------------
   // Set the default logger to file logger
   // Init SSD pool
   int flags = O_RDWR | O_DIRECT;
   if (FLAGS_trunc) {
      flags |= O_TRUNC | O_CREAT;
   }
   ssd_fd = fdp_open(FLAGS_ssd_path.c_str(), flags, 0666);
   if (ssd_fd == -1) {
      perror("posix error");
      std::cout << "path: " << FLAGS_ssd_path << std::endl;
      SetupFailed("Could not open the file or the SSD block device");
   }
   if (FLAGS_falloc > 0) {
      const u64 gib_size = 1024ull * 1024ull * 1024ull;
      auto dummy_data = (u8*)aligned_alloc(512, gib_size);
      for (u64 i = 0; i < FLAGS_falloc; i++) {
         const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
         posix_check(ret == gib_size);
      }
      free(dummy_data);
      fsync(ssd_fd);
   }
   ensure(fcntl(ssd_fd, F_GETFL) != -1);
   // -------------------------------------------------------------------------------------
   u64 total_blocks_in_ssd = 0; // depends on how the namespace is formatted
   if (FLAGS_ssd_gib == 0) {
      u64 ssd_size; // in bytes
      if (ioctl(ssd_fd, BLKGETSIZE64, &ssd_size) == 0) {
         std::cout << "[INFO] SSD size: " << ssd_size << " bytes" << std::endl;
         total_blocks_in_ssd = ssd_size / 4096;
      } else {
         perror("ioctl");
      }
   } else {
      total_blocks_in_ssd = (FLAGS_ssd_gib * 1048576) / 4;
   }
   u64 max_open_ru_epochs = total_blocks_in_ssd / BufferManager::RU_SIZE; // Hardcoded ru size
   // -------------------------------------------------------------------------------------
   buffer_manager = make_unique<storage::BufferManager>(ssd_fd, total_blocks_in_ssd);
   ensure_equal(BMC::global_bf, buffer_manager.get());
   BMC::global_bf = buffer_manager.get();
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal_partition_by == "ru_epoch") {
      FLAGS_wal_partitions_count = max_open_ru_epochs + 1;
      cout << "[INFO] number of Log partitions : " << FLAGS_wal_partitions_count << endl;
   }
   // -------------------------------------------------------------------------------------
   DTRegistry::global_dt_registry.registerDatastructureType(0, storage::btree::BTreeLL::getMeta());
   DTRegistry::global_dt_registry.registerDatastructureType(2, storage::btree::BTreeVI::getMeta());
   // -------------------------------------------------------------------------------------
   if (FLAGS_recover) {
      deserializeState();
   }
   // -------------------------------------------------------------------------------------
   u64 end_of_block_device;
   if (FLAGS_wal_offset_gib == 0) {
      ioctl(ssd_fd, BLKGETSIZE64, &end_of_block_device);
   } else {
      end_of_block_device = FLAGS_wal_offset_gib * 1024 * 1024 * 1024;
   }
   // -------------------------------------------------------------------------------------
   history_tree = std::make_unique<cr::HistoryTree>();
   u64 log_device_size;
   if (FLAGS_wal) {
      ensure(FLAGS_redo_log_file != "");
      ensure(FLAGS_redo_log_file != FLAGS_ssd_path);
      log_dev_fd = open(FLAGS_redo_log_file.c_str(), O_RDWR | O_DIRECT);
      ensure(log_dev_fd > 0);

      if (ioctl(log_dev_fd, BLKGETSIZE64, &log_device_size) == 0) {
         std::cout << "[INFO] Log device size: " << log_device_size << " bytes" << std::endl;
         ensure((log_device_size % 4096) == 0);
         // log_device_size = log_device_size / 4096;
      } else {
         perror("ioctl");
      }
   }
   cr_manager = make_unique<cr::CRManager>(*history_tree.get(), ssd_fd, log_dev_fd, log_device_size);
   cr::CRManager::global = cr_manager.get();
   cr_manager->scheduleJobSync(0, [&]() {
      history_tree->update_btrees = std::make_unique<leanstore::storage::btree::BTreeLL*[]>(FLAGS_worker_threads);
      history_tree->remove_btrees = std::make_unique<leanstore::storage::btree::BTreeLL*[]>(FLAGS_worker_threads);
      for (u64 w_i = 0; w_i < FLAGS_worker_threads; w_i++) {
         std::string name = "_history_tree_" + std::to_string(w_i);
         history_tree->update_btrees[w_i] = &registerBTreeLL(name + "_updates", {.enable_wal = false, .use_bulk_insert = true});
         history_tree->remove_btrees[w_i] = &registerBTreeLL(name + "_removes", {.enable_wal = false, .use_bulk_insert = true});
      }
   });
   // -------------------------------------------------------------------------------------
   buffer_manager->startBackgroundThreads();
}
// -------------------------------------------------------------------------------------
std::string to_hhmmss(uint64_t total_seconds) {
    uint64_t hours   = total_seconds / 3600;
    uint64_t minutes = (total_seconds % 3600) / 60;
    uint64_t seconds = total_seconds % 60;

    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%02lu:%02lu:%02lu",
                  hours, minutes, seconds);
    return std::string(buffer);
}
void LeanStore::startProfilingThread()
{
   std::thread profiling_thread([&]() {
      utils::pinThisThread(((FLAGS_pin_threads) ? FLAGS_worker_threads : 0) + FLAGS_wal + FLAGS_pp_threads);
      if (FLAGS_root) {
         posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
      }
      // -------------------------------------------------------------------------------------
      profiling::BMTable bm_table(*buffer_manager.get());
      profiling::DTTable dt_table(*buffer_manager.get());
      profiling::CPUTable cpu_table;
      profiling::CRTable cr_table;
      profiling::LatencyTable latency_table;
      std::vector<profiling::ProfilingTable*> tables = {&configs_table, &bm_table, &dt_table, &cpu_table, &cr_table};
      // std::vector<profiling::ProfilingTable*> tables = {&bm_table, &cr_table};
      if (FLAGS_profile_latency) {
         tables.push_back(&latency_table);
      }
      // -------------------------------------------------------------------------------------
      std::vector<std::ofstream> csvs;
      std::ofstream::openmode open_flags = FLAGS_csv_truncate ? ios::trunc : ios::app;
      for (u64 t_i = 0; t_i < tables.size(); t_i++) {
         tables[t_i]->open();
         // -------------------------------------------------------------------------------------
         csvs.emplace_back();
         auto& csv = csvs.back();
         csv.open(FLAGS_csv_path + "_" + tables[t_i]->getName() + ".csv", open_flags);
         csv.seekp(0, ios::end);
         csv << std::setprecision(2) << std::fixed;
         if (csv.tellp() == 0) {
            csv << "t,c_hash";
            for (auto& c : tables[t_i]->getColumns()) {
               csv << "," << c.first;
            }
            csv << endl;
         }
      }
      // -------------------------------------------------------------------------------------
      config_hash = configs_table.hash();
      // config_hash = 0;
      // -------------------------------------------------------------------------------------
      u64 seconds = 0;
      while (bg_threads_keep_running) {
         for (u64 t_i = 0; t_i < tables.size(); t_i++) {
            tables[t_i]->next();
            if (tables[t_i]->size() == 0)
               continue;
            // -------------------------------------------------------------------------------------
            // CSV
            auto& csv = csvs[t_i];
            for (u64 r_i = 0; r_i < tables[t_i]->size(); r_i++) {
               csv << to_hhmmss(seconds) << "," << config_hash;
               for (auto& c : tables[t_i]->getColumns()) {
                  csv << "," << c.second.values[r_i];
               }
               csv << endl;
            }
            // -------------------------------------------------------------------------------------
            // TODO: Websocket, CLI
         }
         // -------------------------------------------------------------------------------------
         // Global Stats
         global_stats.accumulated_tx_counter += std::stoi(cr_table.get("0", "tx"));
         // -------------------------------------------------------------------------------------
         // Console
         // -------------------------------------------------------------------------------------
         if (FLAGS_print_tx_console) {
            tabulate::Table table;

            table.add_row({"t", "pre TX", "dur TX", "W MiB", "R MiB", "Discard MiB", "Dirty Read %" , "WAL GiB/s", "WALmtx %", 
                           "gct_p1%", "gct_p2%", "gct_w%", "failed trypop", "GC Fix Mib/s", "False dirty", "GC Clean", "GC hot", "GSPeak GiB"});
            table.add_row({to_hhmmss(seconds),
                            cr_table.get("0", "tx"), 
                            cr_table.get("0", "gct_committed_tx"),
                            bm_table.get("0", "w_mib"),
                            bm_table.get("0", "r_mib"),
                            bm_table.get("0", "discarded_mib"),
                            bm_table.get("0", "dirty_pct"),
                            cr_table.get("0", "gct_write_gib"),
                            cr_table.get("0", "log_buf"),
                            cr_table.get("0", "gct_phase_1_pct"),
                            cr_table.get("0", "gct_phase_2_pct"),
                            cr_table.get("0", "gct_write_pct"),
                            bm_table.get("0", "failed_try_pop"),
                            bm_table.get("0", "gc_fixed_mib"),
                            bm_table.get("0", "false_dirty"),
                            bm_table.get("0", "gc_clean"),
                            bm_table.get("0", "gc_hot"),
                            bm_table.get("0", "discard_state_peak_mem_usage"),
                           });
            // -------------------------------------------------------------------------------------
            table.format().width(8);
            table.column(0).format().width(10);
            table.column(1).format().width(10);
            table.column(2).format().width(10);
            // table.column(13).format().width(22);
            // -------------------------------------------------------------------------------------
            auto print_table = [](tabulate::Table& table, std::function<bool(u64)> predicate) {
               std::stringstream ss;
               table.print(ss);
               string str = ss.str();
               u64 line_n = 0;
               for (u64 i = 0; i < str.size(); i++) {
                  if (str[i] == '\n') {
                     line_n++;
                  }
                  if (predicate(line_n)) {
                     cout << str[i];
                  }
               }
            };
            if ((seconds % 30 ) == 0) {
               cout << endl;
               print_table(table, [](u64 line_n) { return (line_n < 3) || (line_n == 4); });
            } else {
               print_table(table, [](u64 line_n) { return line_n == 4; });
            }
         }
         // -------------------------------------------------------------------------------------
         std::this_thread::sleep_for(std::chrono::milliseconds(1000));
         seconds += 1;
         std::locale::global(std::locale::classic());
      }
      bg_threads_counter--;
   });
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
