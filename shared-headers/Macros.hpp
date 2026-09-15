#pragma once

namespace leanstore
{

#define DO_NOT_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  cname& operator=(const cname &) = delete; /* NOLINT */

#define DO_NOT_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  cname& operator=(cname &&) = delete; /* NOLINT */

#define DO_NOT_COPY_AND_MOVE(cname) \
  DO_NOT_COPY(cname);               \
  DO_NOT_MOVE(cname);

} // namespace leanstore
