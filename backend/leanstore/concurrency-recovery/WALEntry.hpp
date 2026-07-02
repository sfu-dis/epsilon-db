#pragma once
#include "Units.hpp"
#include "Exceptions.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct WALEntry {
   enum class TYPE : u8 { TX_START, TX_COMMIT, TX_ABORT, DT_SPECIFIC, PER_PAGE_DT_SPECIFIC, CARRIAGE_RETURN, SKIP };
   // -------------------------------------------------------------------------------------
   // If the type is SKIP then accesses the below fields below the type is undefined and
   // the next log record is located at the next 4KiB boundary.
   TYPE type;
   u16 size;
   u64 magic_debugging_number = 99;
   std::atomic<LID> lsn;
   LID prev_lsn;
   void computeCRC() { magic_debugging_number = utils::CRC(reinterpret_cast<u8*>(this) + sizeof(u64), size - sizeof(u64)); }
   void checkCRC() const
   {
      if (magic_debugging_number != utils::CRC(reinterpret_cast<const u8*>(this) + sizeof(u64), size - sizeof(u64))) {
         raise(SIGTRAP);
         ensure(false);
      }
   }
   void dump() const {
      std::cout << "WALEntry Dump:" << std::endl;
      std::cout << "  type = " << typeToString(type) << std::endl;
      if (type == TYPE::SKIP) return;
      std::cout << "  size = " << size << std::endl;
      std::cout << "  magic_debugging_number = " << magic_debugging_number << std::endl;
      std::cout << "  lsn = " << lsn.load() << std::endl;
      std::cout << "  prev lsn = " << prev_lsn << std::endl;
   }

private:
   static const char* typeToString(TYPE t) {
      switch (t) {
         case TYPE::TX_START:        return "TX_START";
         case TYPE::TX_COMMIT:       return "TX_COMMIT";
         case TYPE::TX_ABORT:        return "TX_ABORT";
         case TYPE::DT_SPECIFIC:     return "DT_SPECIFIC";
         case TYPE::PER_PAGE_DT_SPECIFIC:     return "PER_PAGE_DT_SPECIFIC";
         case TYPE::CARRIAGE_RETURN: return "CARRIAGE_RETURN";
         case TYPE::SKIP:            return "SKIP";
         default:                    return "UNKNOWN";
      }
   }

};
// -------------------------------------------------------------------------------------
struct WALMetaEntry : WALEntry {
};
// static_assert(sizeof(WALMetaEntry) == 32, "");
// -------------------------------------------------------------------------------------
struct WALDTEntry : WALEntry {
   LID gsn;
   DTID dt_id;
   PID pid;
   s64 ru_epoch; // TODO(mfd) : just for debugging, remove later
   u8 payload[];

   void dump()
   {
       std::cout << "WALDTEntry dump:\n";
       WALEntry::dump();
       std::cout << "  gsn       = " << gsn << "\n";
       std::cout << "  dt_id     = " << dt_id << "\n";
       std::cout << "  pid       = " << pid << "\n";
       std::cout << "  ru_epoch  = " << ru_epoch << "\n";
   }
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
