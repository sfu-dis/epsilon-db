#pragma once
#include "Units.hpp"
#include "Worker.hpp"
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
   enum class TYPE : u8 { TX_START, TX_COMMIT, TX_ABORT, DT_SPECIFIC, CARRIAGE_RETURN };
   // -------------------------------------------------------------------------------------
   u64 magic_debugging_number = 99;
   std::atomic<LID> lsn;
   u16 size;
   TYPE type;
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
      std::cout << "  magic_debugging_number = " << magic_debugging_number << std::endl;
      std::cout << "  lsn = " << lsn.load() << std::endl;
      std::cout << "  size = " << size << std::endl;
      std::cout << "  type = " << typeToString(type) << std::endl;
   }

private:
   static const char* typeToString(TYPE t) {
      switch (t) {
         case TYPE::TX_START:        return "TX_START";
         case TYPE::TX_COMMIT:       return "TX_COMMIT";
         case TYPE::TX_ABORT:        return "TX_ABORT";
         case TYPE::DT_SPECIFIC:     return "DT_SPECIFIC";
         case TYPE::CARRIAGE_RETURN: return "CARRIAGE_RETURN";
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
   u8 payload[];
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
