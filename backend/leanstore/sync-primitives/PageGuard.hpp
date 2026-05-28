#pragma once
#include "Exceptions.hpp"
#include "Latch.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/buffer-manager/Tracing.hpp"
#include "leanstore/storage/btree/core/BTreeGenericWALEntry.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Objects of this class must be thread local !
// OptimisticPageGuard can hold the mutex. There are 3 locations where it can release it:
// 1- Destructor if not moved
// 2- Assign operator
// 3- kill()
template <typename T>
class ExclusivePageGuard;
template <typename T>
class SharedPageGuard;
template <typename T>
class HybridPageGuard
{
  protected:
   void latchAccordingToFallbackMode(Guard& guard, const LATCH_FALLBACK_MODE if_contended)
   {
      if (if_contended == LATCH_FALLBACK_MODE::SPIN) {
         guard.toOptimisticSpin();
      } else if (if_contended == LATCH_FALLBACK_MODE::EXCLUSIVE) {
         guard.toOptimisticOrExclusive();
      } else if (if_contended == LATCH_FALLBACK_MODE::SHARED) {
         guard.toOptimisticOrShared();
      } else if (if_contended == LATCH_FALLBACK_MODE::JUMP) {
         guard.toOptimisticOrJump();
      } else {
         UNREACHABLE();
      }
   }

  public:
   BufferFrame* bf = nullptr;
   Guard guard;
   bool keep_alive = true;
   // -------------------------------------------------------------------------------------
   // Constructors
   HybridPageGuard() : bf(nullptr), guard(nullptr) { jumpmu_registerDestructor(); }  // use with caution
   HybridPageGuard(Guard&& guard, BufferFrame* bf) : bf(bf), guard(std::move(guard)) { jumpmu_registerDestructor(); }
   // -------------------------------------------------------------------------------------
   HybridPageGuard(HybridPageGuard& other) = delete;   // Copy constructor
   HybridPageGuard(HybridPageGuard&& other) = delete;  // Move constructor
   // -------------------------------------------------------------------------------------
   // I: Allocate a new page
   HybridPageGuard(DTID dt_id, bool keep_alive)
       : bf(&BMC::global_bf->allocatePage()), guard(bf->header.latch, GUARD_STATE::EXCLUSIVE), keep_alive(keep_alive)
   {
      assert(BMC::global_bf != nullptr);
      ensure(bf->header.discardable == false);
      // By default pin a newly allocated page until we write a WALPageInit log record.
      // This will give us more flexibility on when to increment the GSN.
      bf->header.keep_in_memory = true;
      bf->page.dt_id = dt_id;
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   // I: Root case
   HybridPageGuard(Swip<BufferFrame> sentinal_swip, const LATCH_FALLBACK_MODE if_contended = LATCH_FALLBACK_MODE::SPIN)
       : bf(&sentinal_swip.asBufferFrame()), guard(bf->header.latch)
   {
      latchAccordingToFallbackMode(guard, if_contended);
      syncGSN();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   // I: Lock coupling
   template <typename T2>
   HybridPageGuard(HybridPageGuard<T2>& p_guard, Swip<T>& swip, const LATCH_FALLBACK_MODE if_contended = LATCH_FALLBACK_MODE::SPIN)
       : bf(&BMC::global_bf->tryFastResolveSwip(p_guard.guard, swip.template cast<BufferFrame>())), guard(bf->header.latch)
   {
      latchAccordingToFallbackMode(guard, if_contended);
      syncGSN();
      jumpmu_registerDestructor();
      // -------------------------------------------------------------------------------------
      PARANOID_BLOCK()
      {
         [[maybe_unused]] DTID p_dt_id = p_guard.bf->page.dt_id, dt_id = bf->page.dt_id;
         [[maybe_unused]] PID pid = bf->header.pid;
         p_guard.recheck();
         recheck();
         if (p_dt_id != dt_id) {
            cout << "p_dt_id != dt_id" << endl;
            leanstore::storage::Tracing::printStatus(pid);
         }
      }
      // -------------------------------------------------------------------------------------
      p_guard.recheck();
   }
   // I: Downgrade exclusive
   HybridPageGuard(ExclusivePageGuard<T>&&) = delete;
   HybridPageGuard& operator=(ExclusivePageGuard<T>&&)
   {
      guard.unlock();
      return *this;
   }
   // I: Downgrade shared
   HybridPageGuard(SharedPageGuard<T>&&) = delete;
   HybridPageGuard& operator=(SharedPageGuard<T>&&)
   {
      guard.unlock();
      return *this;
   }
   // -------------------------------------------------------------------------------------
   // Assignment operator
   constexpr HybridPageGuard& operator=(HybridPageGuard& other) = delete;
   template <typename T2>
   constexpr HybridPageGuard& operator=(HybridPageGuard<T2>&& other)
   {
      bf = other.bf;
      guard = std::move(other.guard);
      keep_alive = other.keep_alive;
      return *this;
   }
   // -------------------------------------------------------------------------------------
   inline void markAsDirty()
   {
      bf->page.PLSN++;
      if (FLAGS_enable_discarding
         && bf->header.discardable.load(std::memory_order_acquire)
         && (bf->page.PLSN - bf->header.last_written_plsn) > FLAGS_max_log_records_to_discard) {
         bf->header.discardable.store(false, std::memory_order_release);
      }
   }
   inline void incrementGSN()
   {
      assert(bf != nullptr);
      ensure(bf->page.GSN <= cr::Worker::my().getCurrentGSN());
      markAsDirty();
      LID new_gsn = cr::Worker::my().getCurrentGSN() + 1;
      bf->page.GSN = new_gsn;
      bf->header.last_writer_worker_id = cr::Worker::my().worker_id;  // RFA
      cr::Worker::my().setCurrentGSN(new_gsn);
   }
   // WAL
   inline void syncGSN()
   {
      // TODO: don't sync on temporary table pages like HistoryTree
      if (FLAGS_wal) {
         if (FLAGS_wal_rfa) {
            if (bf->page.GSN > cr::Worker::my().per_worker_logging_info.rfa_gsn_flushed && bf->header.last_writer_worker_id != cr::Worker::my().worker_id) {  //
               cr::Worker::my().per_worker_logging_info.remote_flush_dependency = true;
            }
         }
         LID new_gsn = std::max<LID>(cr::Worker::my().getCurrentGSN(), bf->page.GSN);
         cr::Worker::my().setCurrentGSN(new_gsn);
      }
   }
   inline void unPin()
   {
      ensure(bf->header.keep_in_memory);
      bf->header.keep_in_memory = false;
   }
   template <typename WT>
   cr::Logging::WALEntryHandler<WT> reserveWALEntry(u64 extra_size, bool disable_discarding = true)
   {
      assert(FLAGS_wal);
      assert(guard.state == GUARD_STATE::EXCLUSIVE);
      if (disable_discarding && bf->isDiscardable()) {
         bf->markUnDiscardable();
      }
      if (!FLAGS_wal_tuple_rfa) {
         incrementGSN();
      }
      // -------------------------------------------------------------------------------------
      const auto pid = bf->header.pid;
      const auto dt_id = bf->page.dt_id;
      s64 failure_counter = 0;
   retry:
      s64 reclaiming_ru_epoch_snapshot = storage::BMC::global_bf->reclaiming_ru_epoch.load(std::memory_order_acquire);
      auto& logging = cr::LogManager::getLog(bf);
      logging.mutex.lock();
      if (logging.redirect_to_sink_log.load(std::memory_order_acquire)) {
         logging.mutex.unlock();
         goto retry;
      }
      if (reclaiming_ru_epoch_snapshot < bf->page.ru_epoch) {
         s64 reclaiming_ru_epoch_snapshot2 = storage::BMC::global_bf->reclaiming_ru_epoch.load(std::memory_order_acquire);
         if ((reclaiming_ru_epoch_snapshot2 != reclaiming_ru_epoch_snapshot)
           && (reclaiming_ru_epoch_snapshot2 >= bf->page.ru_epoch)) {
            logging.mutex.unlock();
            if ((++failure_counter % 4096) == 0) {
               printf("[WARN] I think I am stuck retrying in reserveWALEntry!!\n");
            }
            goto retry;
         }
      }
      bool first_entry_in_log = false;
      if (bf->header.logging != nullptr) {
         if (bf->header.logging != &logging) {
            // This only happens when the RU on which the page reside has started to be reclaimed.
            if (!logging.is_sink_log) {
               cerr << "Previous Log ID " << bf->header.logging->log_id << endl;
               cerr << "New Log ID " << logging.log_id << endl;
               bf->dump();
               raise(SIGTRAP);
            }
            first_entry_in_log = true;
            bf->header.flush_sink_log = true;
         }
      } else {
         first_entry_in_log = true;
      }
      if (logging.is_sink_log) {
         bf->markUnDiscardable();
      }
      bf->header.logging = &logging;
      logging.walEnsureEnoughSpace(sizeof(leanstore::cr::WALDTEntry) + sizeof(WT) + extra_size);
      ensure_equal(cr::Worker::my().getCurrentGSN(), bf->page.GSN);
      LID logGSN = std::max<LID>(bf->page.GSN, logging.getCurrentGSN() + 1);
      logging.setCurrentGSN(logGSN);
      cr::Worker::my().syncGSN(logGSN);
      bf->page.GSN = logGSN;
      auto handler = logging.reserveDTEntry<WT>(sizeof(WT) + extra_size, pid, logGSN, dt_id);
      logging.active_dt_entry->ru_epoch = bf->page.ru_epoch;
      // FIXME(mfd) : In case of abort the page last written lsn should be recovered to the previous one.
      logging.active_dt_entry->prev_lsn = first_entry_in_log ? INVALID_LSN : bf->page.last_written_lsn;
      logging.mutex.unlock();
      // -------------------------------------------------------------------------------------
      bf->header.absorbed_writes++;
      if (FLAGS_wal_pwrite) {
         bf->page.last_written_lsn = handler.lsn;
         bf->page.log_id = logging.log_id;
      } else {
         // Write a special marker for debugging if something went wrong
         // when recovering from the case where wal_pwrite was off.
         bf->page.last_written_lsn = cr::LogManager::NON_PERSISTED_LSN;
      }
      LID *pending_lsn = bf->header.pending_lsn;
      if (FLAGS_enable_discarding && bf->isDiscardable()) {
         ensure(!logging.is_sink_log);
         ensure(!first_entry_in_log || (bf->header.pending_lsn_count == 0));
         ensure_lt(bf->header.pending_lsn_count, FLAGS_max_log_records_to_discard);
         pending_lsn[bf->header.pending_lsn_count++] = handler.lsn;
         if (bf->header.pending_lsn_count != (bf->page.PLSN - bf->header.last_written_plsn)) {
            bf->dump();
         }
         ensure_equal(bf->header.pending_lsn_count, bf->page.PLSN - bf->header.last_written_plsn);
      }
      return handler;
   }
   // -------------------------------------------------------------------------------------
   inline bool hasFacedContention() { return guard.faced_contention; }
   inline void unlock() { guard.unlock(); }
   inline void recheck() { guard.recheck(); }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(bf->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(bf->page.dt); }
   inline Swip<T> swip() { return Swip<T>(bf); }
   inline T* operator->() { return reinterpret_cast<T*>(bf->page.dt); }
   // -------------------------------------------------------------------------------------
   // Use with caution!
   void toShared() { guard.toShared(); }
   void toExclusive() { guard.toExclusive(); }
   void tryToShared() { guard.tryToShared(); }        // Can jump
   void tryToExclusive() { guard.tryToExclusive(); }  // Can jump
   // -------------------------------------------------------------------------------------
   void reclaim()
   {
      BMC::global_bf->reclaimPage(*(bf));
      guard.state = GUARD_STATE::MOVED;
   }
   // -------------------------------------------------------------------------------------
   jumpmu_defineCustomDestructor(HybridPageGuard)
       // -------------------------------------------------------------------------------------
       ~HybridPageGuard()
   {
      if (guard.state == GUARD_STATE::EXCLUSIVE) {
         if (!keep_alive) {
            reclaim();
         }
      }
      guard.unlock();
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
template <typename T>
class ExclusivePageGuard
{
  private:
   HybridPageGuard<T>& ref_guard;

  public:
   // -------------------------------------------------------------------------------------
   // I: Upgrade
   ExclusivePageGuard(HybridPageGuard<T>&& o_guard) : ref_guard(o_guard) { ref_guard.guard.toExclusive(); }
   // -------------------------------------------------------------------------------------
   template <typename WT>
   cr::Logging::WALEntryHandler<WT> reserveWALEntry(u64 extra_size)
   {
      return ref_guard.template reserveWALEntry<WT>(extra_size);
   }
   // -------------------------------------------------------------------------------------
   template <typename... Args>
   void init(Args&&... args)
   {
      new (ref_guard.bf->page.dt) T(std::forward<Args>(args)...);
   }
   // -------------------------------------------------------------------------------------
   void keepAlive() { ref_guard.keep_alive = true; }
   void incrementGSN() { ref_guard.incrementGSN(); }
   void markAsDirty() { ref_guard.markAsDirty(); }
   void unPin() { ref_guard.unPin(); }
   // -------------------------------------------------------------------------------------
   ~ExclusivePageGuard()
   {
      if (!ref_guard.keep_alive && ref_guard.guard.state == GUARD_STATE::EXCLUSIVE) {
         ref_guard.reclaim();
      } else {
         ref_guard.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline Swip<T> swip() { return Swip<T>(ref_guard.bf); }
   inline T* operator->() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline BufferFrame* bf() { return ref_guard.bf; }
   inline void reclaim() { ref_guard.reclaim(); }
};
// -------------------------------------------------------------------------------------
template <typename T>
class SharedPageGuard
{
  public:
   HybridPageGuard<T>& ref_guard;
   // I: Upgrade
   SharedPageGuard(HybridPageGuard<T>&& h_guard) : ref_guard(h_guard) { ref_guard.toShared(); }
   // -------------------------------------------------------------------------------------
   ~SharedPageGuard() { ref_guard.unlock(); }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline Swip<T> swip() { return Swip<T>(ref_guard.bf); }
   inline T* operator->() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
