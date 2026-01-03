#include "InstrumentedMutex.hpp"

namespace leanstore
{
// -------------------------------------------------------------------------------------
std::atomic<u64> instrumented_mutex::mutex_id{0};
std::unordered_map<std::string, u64> instrumented_mutex::name2id;
std::mutex instrumented_mutex::init_mutex;
// -------------------------------------------------------------------------------------
}  // namespace leanstore
