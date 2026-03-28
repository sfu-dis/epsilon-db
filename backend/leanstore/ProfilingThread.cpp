#include "LeanStore.hpp"

#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/profiling/tables/LatencyTable.hpp"
// -------------------------------------------------------------------------------------
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <sys/resource.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
using RowType = std::vector<variant<std::string, const char*, tabulate::Table>>;
// -------------------------------------------------------------------------------------
std::string to_hhmmss(uint64_t total_seconds)
{
   uint64_t hours = total_seconds / 3600;
   uint64_t minutes = (total_seconds % 3600) / 60;
   uint64_t seconds = total_seconds % 60;

   char buffer[32];
   std::snprintf(buffer, sizeof(buffer), "%02lu:%02lu:%02lu", hours, minutes, seconds);
   return std::string(buffer);
}
// -------------------------------------------------------------------------------------
void LeanStore::profilingThread()
{
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

         table.add_row({"t", "pre TX", "dur TX", "W MiB", "R MiB", "Discard MiB", "Dirty Read %", "WAL GiB/s", "WALmtx %", "gct_p1%", "gct_p2%",
                        "gct_w%", "failed trypop", "GC Fix Mib/s", "False dirty", "GC Clean", "GC hot", "GSPeak GiB"});
         table.add_row({
             to_hhmmss(seconds),
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
         if ((seconds % 30) == 0) {
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
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore
