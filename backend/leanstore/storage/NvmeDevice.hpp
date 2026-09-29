#pragma once
// -------------------------------------------------------------------------------------
#ifdef WITH_FDP
#include <fdp.h>
#endif
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
inline int nvme_open(const char* path, int flags, mode_t mode = 0644)
{
   if (FLAGS_enable_fdp) {
#ifdef WITH_FDP
      return fdp_open(path, flags);
#else
      UNREACHABLE();
#endif
   }
   return ::open(path, flags, mode);
}
// -------------------------------------------------------------------------------------
#if 0
inline ssize_t nvme_pwrite(int fd, void* buf, size_t n, off_t off,
                             [[maybe_unused]] uint16_t plid) {
#ifdef WITH_FDP
    return fdp_pwrite(fd, buf, n, off, plid);
#else
    return ::pwrite(fd, buf, n, off);
#endif
}
#endif
// -------------------------------------------------------------------------------------
inline void nvme_io_uring_prep_write(struct io_uring_sqe* sqe, int fd, const void* buf, unsigned count, u64 offset, uint16_t plid = 0)
{
   if (FLAGS_enable_fdp) {
#ifdef WITH_FDP
      fdp_io_uring_prep_write(sqe, fd, buf, count, offset, plid);
#else
      UNREACHABLE();
#endif
   } else {
      io_uring_prep_write(sqe, fd, buf, count, offset);
   }
}
// -------------------------------------------------------------------------------------
inline void nvme_close(int fd)
{
   if (FLAGS_enable_fdp) {
#ifdef WITH_FDP
      fdp_close(fd);
#else
      UNREACHABLE();
#endif
   } else {
      ::close(fd);
   }
}
// -------------------------------------------------------------------------------------
// Number of RUHs available to write to on an FDP device opened with nvme_open(),
// -1 with errno set on failure.
inline int nvme_get_nruh([[maybe_unused]] int fd)
{
#ifdef WITH_FDP
   return fdp_get_nruh(fd);
#else
   UNREACHABLE();
#endif
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore
// -------------------------------------------------------------------------------------
