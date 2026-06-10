#pragma once

#include <cstdio>
#include <ctime>

namespace leanstore
{
namespace utils
{

struct CrashLogger {
   explicit CrashLogger(const std::string& pathname)
   {
      FILE* f = fopen(pathname.c_str(), "a");
      if (!f)
         return;

      time_t t = time(nullptr);
      char ts[32];
      strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&t));
      fprintf(f, "\n=== CRASH %s ===\n", ts);
      fflush(f);

      // Redirect stdout and stderr into the same file.
      int fd = fileno(f);
      dup2(fd, 1);
      dup2(fd, 2);
   }

   ~CrashLogger()
   {
      fflush(stdout);
      fflush(stderr);
   }
};

}  // namespace utils
}  // namespace leanstore
