#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/Schema.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
#include "leanstore/utils/RejectionInversionZipf.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/parallel_for.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <set>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_insert_threads, 0, "");
DEFINE_uint32(ycsb_threads, 0, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
DEFINE_bool(ycsb_warmup, true, "");
DEFINE_uint32(ycsb_sleepy_thread, 0, "");
DEFINE_uint32(ycsb_ops_per_tx, 1, "");
// DEFINE_uint32(ycsb_nb_tables, 1, "Use multiple tables");
DEFINE_bool(ycsb_worker_per_table, false, "Each worker is assigned a table to work on, This will turn off CC");
DEFINE_bool(ycsb_profiler_thread, true, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<8>;
using KVTable = Relation<YCSBKey, YCSBPayload>;
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // Always init with the maximum number of threads (FLAGS_worker_threads)
   LeanStore db;
   auto& crm = db.getCRManager();
   std::vector<LeanStoreAdapter<KVTable>> tables;
   const u64 ycsb_n_tables = FLAGS_ycsb_worker_per_table ? FLAGS_worker_threads : 1UL;
   tables.reserve(ycsb_n_tables);
   crm.scheduleJobSync(0, [&]() {
      for (u64 t_id = 0; t_id < ycsb_n_tables; t_id++) {
         std::string table_name = "YCSB" + std::to_string(t_id);
         tables.emplace_back(db, table_name);
      }
   });
   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   db.registerConfigEntry("ycsb_threads", FLAGS_ycsb_threads);
   db.registerConfigEntry("ycsb_ops_per_tx", FLAGS_ycsb_ops_per_tx);
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   if (FLAGS_ycsb_worker_per_table) isolation_level = TX_ISOLATION_LEVEL::READ_UNCOMMITTED;
   const TX_MODE tx_type = TX_MODE::OLTP;
   // -------------------------------------------------------------------------------------
   u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   // Insert values
   if (FLAGS_ycsb_worker_per_table && !FLAGS_ycsb_tuple_count) ycsb_tuple_count = ycsb_tuple_count / FLAGS_worker_threads;
   const u64 n = ycsb_tuple_count;
   if (FLAGS_ycsb_profiler_thread) {
      db.startProfilingThread();
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_tmp4) {
      // -------------------------------------------------------------------------------------
      std::ofstream csv;
      csv.open("zipf.csv", ios::trunc);
      csv.seekp(0, ios::end);
      csv << std::setprecision(2) << std::fixed;
      std::unordered_map<u64, u64> ht;
      auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
      for (u64 t_i = 0; t_i < (FLAGS_tmp4 ? FLAGS_tmp4 : 1e6); t_i++) {
         u64 key = zipf_random->rand();
         if (ht.find(key) == ht.end()) {
            ht[key] = 0;
         } else {
            ht[key]++;
         }
      }
      csv << "key,count" << endl;
      for (auto& [key, value] : ht) {
         csv << key << "," << value << endl;
      }
      cout << ht.size() << endl;
      return 0;
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_recover) {
      // Warmup
      if (FLAGS_ycsb_warmup) {
         cout << "Warmup: Scanning..." << endl;
         {
            begin = chrono::high_resolution_clock::now();
            if (FLAGS_ycsb_worker_per_table) {
               for (u64 t_i = 0; t_i < FLAGS_worker_threads; ++t_i) {
                  crm.scheduleJobAsync(t_i, [&, t_i]() {
                     for (u64 i = 0; i < n; i++) {
                        YCSBPayload result;
                        tables[t_i].lookup1({static_cast<YCSBKey>(i)}, [&](const KVTable& record) { result = record.my_payload; });
                        DO_NOT_OPTIMIZE(result);
                     }
                  });
               }
            } else {
               utils::Parallelize::range(FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
                  crm.scheduleJobAsync(t_i, [&, begin, end]() {
                     for (u64 i = begin; i < end; i++) {
                        YCSBPayload result;
                        tables[t_i].lookup1({static_cast<YCSBKey>(i)}, [&](const KVTable& record) { result = record.my_payload; });
                        DO_NOT_OPTIMIZE(result);
                     }
                  });
               });
            }
            crm.joinAll();
            end = chrono::high_resolution_clock::now();
         }
         // -------------------------------------------------------------------------------------
         cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
         cout << calculateMTPS(begin, end, n) << " M tps" << endl;
         cout << "-------------------------------------------------------------------------------------" << endl;
      }
   } else {
      cout << "Inserting " << ycsb_tuple_count << " values" << endl;
      begin = chrono::high_resolution_clock::now();
      if (FLAGS_ycsb_worker_per_table) {
              for (u64 t_i = 0; t_i < FLAGS_worker_threads; ++t_i) {
				 crm.scheduleJobAsync(t_i, [&, t_i]() {
                    // cout << table_id << "Inserting ..." << endl;
					for (u64 i = 0; i < n; i++) {
					   YCSBPayload payload;
					   utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
					   YCSBKey key = i;
					   cr::Worker::my().startTX(tx_type, leanstore::TX_ISOLATION_LEVEL::READ_COMMITTED);
					   tables[t_i].insert({key}, {payload});
					   cr::Worker::my().commitTX();
					}
				 });
			  }
      } else {
			  utils::Parallelize::range(FLAGS_ycsb_insert_threads ? FLAGS_ycsb_insert_threads : FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
				 crm.scheduleJobAsync(t_i, [&, begin, end]() {
					for (u64 i = begin; i < end; i++) {
					   YCSBPayload payload;
					   utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
					   YCSBKey key = i;
					   cr::Worker::my().startTX(tx_type, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
					   tables[0].insert({key}, {payload});
					   cr::Worker::my().commitTX();
					}
				 });
			  });
      }
      crm.joinAll();
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      // -------------------------------------------------------------------------------------
      const u64 written_pages = db.getBufferManager().consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
      if (FLAGS_run_for_seconds == 0) {
         return 0;
      }
   }
   // -------------------------------------------------------------------------------------
   auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
   auto rjzipf = RejectionInversionZipfSampler(ycsb_tuple_count, FLAGS_zipf_factor);
   std::vector<u32> updatePattern;
   if (FLAGS_zipf_factor != 0) {
      updatePattern.resize(ycsb_tuple_count);
      for (uint64_t i = 0; i < updatePattern.size(); i++) {
         updatePattern[i] = i;
      }
      std::random_device rd;
      std::mt19937_64 g(rd());
      std::shuffle(updatePattern.begin(), updatePattern.end(), g);
   }
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   cout << "~Transactions" << endl;
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   const u32 exec_threads = FLAGS_ycsb_threads ? FLAGS_ycsb_threads : FLAGS_worker_threads;
   const float rate = FLAGS_tx_rate;
   std::atomic<u64> next_tx_start_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
   for (u64 t_i = 0; t_i < exec_threads - ((FLAGS_ycsb_sleepy_thread) ? 1 : 0); t_i++) {
      crm.scheduleJobAsync(t_i, [&, t_i]() {
         const u64 table_id = FLAGS_ycsb_worker_per_table ? t_i : 0;
         LeanStoreAdapter<KVTable> *table = &tables[table_id];
         running_threads_counter++;
         std::random_device rd;
         std::mt19937_64 gen(rd());
         std::exponential_distribution<> expDist(rate);
         auto this_expected_start_time = next_tx_start_time.load();
         while (keep_running) {
            utils::Timer timer(CRCounters::myCounters().cc_ms_oltp_tx);
            auto start = std::chrono::high_resolution_clock::now().time_since_epoch().count();
            jumpmuTry()
            {
               YCSBKey key;
               if (FLAGS_zipf_factor == 0) {
                  key = utils::RandomGenerator::getRandU64(0, ycsb_tuple_count);
               } else {
                  s64 r = rjzipf.sample(gen) - 1;
                  if (!(r >= 0 && r < updatePattern.size())) {
                     cerr << "Value " << r << "Out of Bounds [0," << updatePattern.size() << ")" << endl;
                     raise(SIGINT);
                  }
                  key = updatePattern.at(r);
               }
               assert(key < ycsb_tuple_count);
               YCSBPayload result;
               cr::Worker::my().startTX(tx_type, isolation_level);
               for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
                  if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                     table->lookup1({key}, [&](const KVTable&) {});  // result = record.my_payload;
                     // leanstore::storage::BMC::global_bf->evictLastPage();  // to ignore the replacement strategy effect on MVCC experiment
                  } else {
                     UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);
                     utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&result), sizeof(YCSBPayload));
                     // -------------------------------------------------------------------------------------
                     table->update1(
                         {key}, [&](KVTable& rec) { rec.my_payload = result; }, tabular_update_descriptor);
                     // leanstore::storage::BMC::global_bf->evictLastPage();  // to ignore the replacement strategy effect on MVCC experiment
                  }
               }
               cr::Worker::my().commitTX();
               WorkerCounters::myCounters().tx++;
            }
            jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
            auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
            COUNTERS_BLOCK(txHist)
            {
               auto elapsed = now - start;
               if (WorkerCounters::myCounters().txHistLock.try_lock()) {
                  WorkerCounters::myCounters().txHist.increaseSlot(elapsed / 1000);
                  WorkerCounters::myCounters().txHistLock.unlock();
               }
            }
            if (FLAGS_tx_rate > 0) {
               COUNTERS_BLOCK()
               {
                  auto elapsedIncWait = now - this_expected_start_time;
                  if (WorkerCounters::myCounters().txIncWaitHistLock.try_lock()) {
                     WorkerCounters::myCounters().txIncWaitHist.increaseSlot(elapsedIncWait / 1000);
                     WorkerCounters::myCounters().txIncWaitHistLock.unlock();
                  }
               }
               const auto d = expDist(gen);
               const auto dd = std::chrono::nanoseconds((int)(d * 1e9)).count();
               // reset if we're not able to keep up with the rate
               if (now > next_tx_start_time + 5 * 1e9) {
                  next_tx_start_time = now;
                  std::cout << "reset start time" << std::endl;
               }
               this_expected_start_time = next_tx_start_time.load();
               while (true) {
                  while (now < this_expected_start_time) {
                     std::this_thread::sleep_for(std::chrono::nanoseconds(this_expected_start_time - now - 500));
                     now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
                  }
                  // CAS or expected is new next_tx_start_time
                  if (next_tx_start_time.compare_exchange_strong(this_expected_start_time, this_expected_start_time + dd)) {
                     break;
                  }
               }
            }
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_ycsb_sleepy_thread) {
      const leanstore::TX_MODE tx_type = FLAGS_olap_mode ? leanstore::TX_MODE::OLAP : leanstore::TX_MODE::OLTP;
      crm.scheduleJobAsync(exec_threads - 1, [&]() {
         running_threads_counter++;
         while (keep_running) {
            jumpmuTry()
            {
               cr::Worker::my().startTX(tx_type, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
               sleep(FLAGS_ycsb_sleepy_thread);
               cr::Worker::my().commitTX();
            }
            jumpmuCatch() {}
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   {
      // Shutdown threads
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
      }
      crm.joinAll();
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   return 0;
}
