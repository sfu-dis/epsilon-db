#pragma once
#include <setjmp.h>
#include <signal.h>

#include <cassert>
#include <utility>

#define JUMPMU_STACK_SIZE 100
namespace jumpmu
{
extern __thread int checkpoint_counter;
extern __thread jmp_buf env[JUMPMU_STACK_SIZE];
extern __thread int val[JUMPMU_STACK_SIZE];
extern __thread int checkpoint_stacks_counter[JUMPMU_STACK_SIZE];
extern __thread void (*de_stack_arr[JUMPMU_STACK_SIZE])(void*);
extern __thread void* de_stack_obj[JUMPMU_STACK_SIZE];
extern __thread int de_stack_counter;
extern __thread bool in_jump;
void jump(int val = 1);
inline void clearLastDestructor()
{
   de_stack_obj[de_stack_counter - 1] = nullptr;
   de_stack_arr[de_stack_counter - 1] = nullptr;
   de_stack_counter--;
   assert(de_stack_counter >= 0);
}

}  // namespace jumpmu
   // -------------------------------------------------------------------------------------
   // clang-format off
#define jumpmu_registerDestructor()                       \
  assert(jumpmu::de_stack_counter < JUMPMU_STACK_SIZE);assert(jumpmu::checkpoint_counter < JUMPMU_STACK_SIZE);jumpmu::de_stack_arr[jumpmu::de_stack_counter] = &des;assert(jumpmu::de_stack_arr[jumpmu::de_stack_counter]!=nullptr);jumpmu::de_stack_obj[jumpmu::de_stack_counter] = this;jumpmu::de_stack_counter++;

#define jumpmu_defineCustomDestructor(NAME) static void des(void* t) { reinterpret_cast<NAME*>(t)->~NAME(); }

// without calling destructors
#define jumpmu_return           \
  jumpmu::checkpoint_counter--; return

#define jumpmu_break                            \
  jumpmu::checkpoint_counter--; break

#define jumpmu_continue                         \
  jumpmu::checkpoint_counter--; continue

// ATTENTION DO NOT DO ANYTHING BETWEEN setjmp and if !!
#define jumpmuTry()                                                                         \
  assert(jumpmu::de_stack_counter >= 0); jumpmu::checkpoint_stacks_counter[jumpmu::checkpoint_counter] = jumpmu::de_stack_counter; int _lval = setjmp(jumpmu::env[jumpmu::checkpoint_counter++]); if (_lval == 0) {

#define jumpmuCatch()           \
  jumpmu::checkpoint_counter--; } else

// ATTENTION DOES NOT SUPPORT MULTIPLE CATCHES, EITHER USE THIS OR jumpmuCatch()
#define jumpmuCatchExp(e)           \
   jumpmu::checkpoint_counter--; } else if (_lval == e) 

enum JumpMURetryCause {
   NONE = 0,
   UNKOWN = 1,
   GUARD_RECHECK = 2,
   IO_FRAME_PARENT_CHANGE = 3,
   IO_FRAME_READING = 4,
   IO_FRAME_TO_DELETE = 5,
   TRY_POP = 6,
   TO_OPTIMISTIC = 7,
   TO_SHARED = 8,
   TO_EXCLUSIVE = 9,
   TOTAL_CAUSES = 10
};

#ifdef DEBUG_LIVELOCK
#define LIVELOCK_DEBUG_INIT_BLOCK()                \
   volatile u64 _jump_cause[TOTAL_CAUSES];         \
   for (int i = 0; i < TOTAL_CAUSES; ++i) {        \
      _jump_cause[i] = 0;                          \
   }                                               \
   volatile u64 _failed_count = 0;                 \
   if constexpr (true)

#define LIVELOCK_DEBUG_BLOCK() if constexpr (true)

#else
#define LIVELOCK_DEBUG_INIT_BLOCK()                \
   if constexpr (false)

#define LIVELOCK_DEBUG_BLOCK() if constexpr (false)
#endif

#ifdef DEBUG_LIVELOCK
#define jumpmuCatchWarnOnLivelock()              \
 jumpmu::checkpoint_counter--; } else            \
 {                                               \
    _jump_cause[_lval] = _jump_cause[_lval] + 1; \
    _failed_count = _failed_count + 1;           \
    if ((_failed_count % (1048576*16)) == 0) {   \
       printf("[WARN] %s:%d : Suspect stuck for %lu!!!\n", __FILE__, __LINE__, _failed_count); \
       for (int i = 0; i < TOTAL_CAUSES; ++i) { \
	  cout << _jump_cause[i] << ",";        \
       }                                        \
       cout << endl;                            \
    }                                           \
 }
#else 
#define jumpmuCatchWarnOnLivelock() jumpmuCatch()
#endif

   // clang-format on
template <typename T>
class JMUW
{
  public:
   T obj;
   template <typename... Args>
   JMUW(Args&&... args) : obj(std::forward<Args>(args)...)
   {
      jumpmu_registerDestructor();
   }
   static void des(void* t) { reinterpret_cast<JMUW<T>*>(t)->~JMUW<T>(); }
   ~JMUW() { jumpmu::clearLastDestructor(); }
   T* operator->() { return reinterpret_cast<T*>(&obj); }
};
