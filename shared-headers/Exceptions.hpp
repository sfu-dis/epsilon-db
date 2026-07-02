#pragma once
#include <signal.h>

#include <exception>
#include <string>

#include "Units.hpp"
#include "Backtrace.hpp"
// -------------------------------------------------------------------------------------
#define imply(lhs, rhs) (!(lhs) || (rhs))
//--------------------------------------------------------------------------------------
#define Generic_Exception(name)                                                                                        \
   struct name : public std::exception {                                                                               \
      const std::string msg;                                                                                           \
      explicit name() : msg(#name) { printf("Throwing exception: %s\n", #name); leanstore::print_backtrace(); }        \
      explicit name(const std::string& msg) : msg(msg) {                                                               \
          printf("Throwing exception: %s(%s)\n", #name, msg.c_str()); leanstore::print_backtrace(); }                  \
      ~name() = default;                                                                                               \
      virtual const char* what() const noexcept { return msg.c_str(); }                                                \
   };                                                                                                                  \
//--------------------------------------------------------------------------------------
namespace leanstore
{
namespace ex
{
Generic_Exception(GenericException);
Generic_Exception(EnsureFailed);
Generic_Exception(UnReachable);
Generic_Exception(TODO);
}  // namespace ex
}  // namespace leanstore
// -------------------------------------------------------------------------------------
#define UNREACHABLE() throw leanstore::ex::UnReachable(std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));
// -------------------------------------------------------------------------------------
#define always_check(e)                                                                                                          \
   (__builtin_expect(!(e), 0) ? throw leanstore::ex::EnsureFailed(std::string(__func__) + " in " + std::string(__FILE__) + "@" + \
                                                                  std::to_string(__LINE__) + " msg: " + std::string(#e))         \
                              : (void)0)

#ifdef PARANOID
#define paranoid(e) always_check(e);
#define PARANOID_BLOCK() if constexpr (true)
#else
#ifdef DEBUG
#define PARANOID_BLOCK() if constexpr (true)
#else
#define PARANOID_BLOCK() if constexpr (false)
#endif
#define paranoid(e) assert(e)
#endif

#ifdef DEBUG
#define ensure(e) assert(e);
#else
#define ensure(e) always_check(e)
#endif

#define ensure_equal(a, b) \
    do { \
        if ((a) != (b)) { \
            fprintf(stderr, "Equality check failed: %s != %s (values: %lld vs %lld) at %s:%d\n", \
                    #a, #b, (long long)(a), (long long)(b), __FILE__, __LINE__); \
            leanstore::print_backtrace(); \
            abort(); \
        } \
    } while (0)

#define ensure_equal_goto_fail(a, b) \
    do { \
        if ((a) != (b)) { \
            fprintf(stderr, "Equality check failed: %s != %s (values: %lld vs %lld) at %s:%d\n", \
                    #a, #b, (long long)(a), (long long)(b), __FILE__, __LINE__); \
            goto fail; \
        } \
    } while (0)

#define ensure_lt(a, b) \
    do { \
        if ((a) >= (b)) { \
            fprintf(stderr, "Less than check failed: %s >= %s (values: %lld vs %lld) at %s:%d\n", \
                    #a, #b, (long long)(a), (long long)(b), __FILE__, __LINE__); \
            leanstore::print_backtrace(); \
            abort(); \
        } \
    } while (0)

#define ensure_lte(a, b) \
    do { \
        if ((a) > (b)) { \
            fprintf(stderr, "Less than or equal check failed: %s > %s (values: %lld vs %lld) at %s:%d\n", \
                    #a, #b, (long long)(a), (long long)(b), __FILE__, __LINE__); \
            leanstore::print_backtrace(); \
            abort(); \
        } \
    } while (0)

#define WARN_IF_SUSPECT_CONDITION_STUCK(cond)                                       \
    do {                                                                            \
        enum { _warn_id = __COUNTER__ };                                            \
        static bool _warn_flag_##_warn_id = false;                                  \
        static unsigned long _warn_count_##_warn_id = 0;                            \
                                                                                    \
        if (cond) {                                                                 \
            if (_warn_flag_##_warn_id) {                                            \
                _warn_count_##_warn_id++;                                           \
                if ((_warn_count_##_warn_id % 4194304UL) == 0) {                    \
                    fprintf(stderr,                                                 \
                        "[WARN] condition %s on line %d in file %s repeated "       \
                        "consecutively %lu times\n",                                \
                        #cond, __LINE__, __FILE__, _warn_count_##_warn_id);         \
                }                                                                   \
            } else {                                                                \
                _warn_flag_##_warn_id = true;                                       \
                _warn_count_##_warn_id = 1;                                         \
            }                                                                       \
        } else {                                                                    \
            _warn_flag_##_warn_id = false;                                          \
           _warn_count_##_warn_id = 0;                                              \
        }                                                                           \
    } while (0)

// -------------------------------------------------------------------------------------
#define TODOException() throw leanstore::ex::TODO(std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));
#define SetupFailed(msg) throw leanstore::ex::GenericException(msg + std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));
#define NotImplementedWithDiscarding()                 \
   if (FLAGS_enable_discarding) {                      \
      SetupFailed("Not Implemented With Discarding");  \
   }
// -------------------------------------------------------------------------------------
#define explainIfNot(e) \
   if (!(e)) {          \
      raise(SIGTRAP);   \
   };
#define explainWhen(e) \
   if (e) {            \
      raise(SIGTRAP);  \
   };
// -------------------------------------------------------------------------------------
#ifdef MACRO_CHECK_DEBUG
#define DEBUG_BLOCK() if (true)
#define RELEASE_BLOCK() if (true)
#define BENCHMARK_BLOCK() if (true)
#else
#define DEBUG_BLOCK() if (false)
#ifdef MACRO_CHECK_RELEASE
#define RELEASE_BLOCK() if (true)
#define BENCHMARK_BLOCK() if (true)
#else
#define RELEASE_BLOCK() if (false)
#ifdef MACRO_CHECK_BENCHMARK
#define BENCHMARK_BLOCK() if (true)
#else
#define BENCHMARK_BLOCK() if (false)
#endif
#endif
#endif
// -------------------------------------------------------------------------------------
// HACKY, so I can incrementally migrate COUNTERS_BLOCK() with no argumnet
#ifdef MACRO_COUNTERS_ALL
#define COUNTER_ 1
#else
#define COUNTER_ 0
#endif

#include "counters_config.hpp"
#define COUNTERS_BLOCK(name) \
   if constexpr (COUNTER_ || COUNTER_##name)
// -------------------------------------------------------------------------------------
template <typename T>
inline void DO_NOT_OPTIMIZE(T const& value)
{
#if defined(__clang__)
   asm volatile("" : : "g"(value) : "memory");
#else
   asm volatile("" : : "i,r,m"(value) : "memory");
#endif
}
// -------------------------------------------------------------------------------------
#define posix_check(expr) \
   if (!(expr)) {         \
      perror(#expr);      \
      raise(SIGTRAP);      \
   }
