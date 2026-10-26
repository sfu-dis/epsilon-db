#include "LeanStore.hpp"

#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
// -------------------------------------------------------------------------------------
#include <yaml-cpp/yaml.h>
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <sys/resource.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
using RowType = std::vector<variant<std::string, const char*, tabulate::Table>>;
// -------------------------------------------------------------------------------------
RowType load_header_from_yaml(const YAML::Node& cols)
{
   RowType header;

   header.reserve(cols.size() + 1);

   header.push_back("Time");

   for (auto it = cols.begin(); it != cols.end(); ++it) {
      const YAML::Node& cfg = it->second;

      if (!cfg["enabled"] || !cfg["enabled"].as<bool>())
         continue;

      header.push_back(cfg["field"].as<std::string>());
   }

   return header;
}
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
   std::vector<profiling::ProfilingTable*> tables = {&configs_table, &bm_table, &dt_table, &cr_table};
   if (FLAGS_cpu_counters) {
      tables.push_back(&cpu_table);
   }
   // std::vector<profiling::ProfilingTable*> tables = {&bm_table, &cr_table};
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
   YAML::Node config = YAML::LoadFile("display-stats.yaml");
   auto columns = config["columns"];
   RowType header = load_header_from_yaml(columns);
   RowType stats_row;
   stats_row.reserve(columns.size() + 1);
   while (profiling_thread_keep_running) {
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

         table.add_row(header);
         stats_row.push_back(to_hhmmss(seconds));
         for (auto it = columns.begin(); it != columns.end(); ++it) {
            const std::string key = it->first.as<std::string>();
            const YAML::Node& col = it->second;

            bool enabled = col["enabled"] ? col["enabled"].as<bool>() : true;
            if (!enabled) {
               continue;
            }

            std::string table = col["source"].as<std::string>();

            if (table == "cr") {
               stats_row.push_back(cr_table.get("0", key));
            } else if (table == "bm") {
               stats_row.push_back(bm_table.get("0", key));
            } else if (table == "dt") {
               stats_row.push_back(dt_table.get("0", key));
            } else {
               stats_row.push_back("N/A");  // No need to fail
            }
         }
         table.add_row(stats_row);

         // -------------------------------------------------------------------------------------
         table.format().width(8);
         table.column(0).format().width(10);
         table.column(1).format().width(10);
         table.column(2).format().width(10);
         // table.column(13).format().width(22);
         // -------------------------------------------------------------------------------------
         // Header cells wrap when a label is wider than its column, so locate the rows by their borders instead of fixed line numbers
         std::stringstream ss;
         table.print(ss);
         std::vector<std::string> lines;
         std::vector<u64> borders;
         for (std::string line; std::getline(ss, line);) {
            if (!line.empty() && line[0] == '+') {
               borders.push_back(lines.size());
            } else if (line.find_first_not_of("| ") == std::string::npos) {
               continue;  // padding line
            }
            lines.push_back(line);
         }
         if (borders.size() >= 3) {
            if ((seconds % 30) == 0) {
               cout << endl;
               for (u64 l_i = 0; l_i < borders[1]; l_i++) {
                  cout << lines[l_i] << endl;
               }
            }
            for (u64 l_i = borders[1] + 1; l_i < borders[2]; l_i++) {
               cout << lines[l_i] << endl;
            }
         }
         stats_row.clear();
      }
      // -------------------------------------------------------------------------------------
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      seconds += 1;
      std::locale::global(std::locale::classic());
   }
   profiling_threads_counter--;
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore
