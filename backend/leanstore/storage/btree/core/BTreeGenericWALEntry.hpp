#pragma once
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
inline constexpr u8 WAL_BTREE_MAGIC = 0x55;
enum class WAL_LOG_TYPE : u8 {
   WALInsert = 1,
   WALUpdate = 2,
   WALRemove = 3,
   WALAfterBeforeImage = 4,
   WALAfterImage = 5,
   WALLogicalSplit = 10,
   WALInitPage = 11
};
struct WALEntry {
   WAL_LOG_TYPE type;
   u8 magic_debugging_number = WAL_BTREE_MAGIC;
};
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
