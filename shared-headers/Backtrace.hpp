#pragma once
#include <execinfo.h>
#include <unistd.h>
#include <sstream>
#include <vector>
#include <cstdio>
#include <cxxabi.h>
#include <iostream>
#include <memory>

namespace leanstore
{

inline std::string demangle(const char* name)
{
   int status = 0;
   std::unique_ptr<char, void (*)(void*)> res{abi::__cxa_demangle(name, nullptr, nullptr, &status), std::free};
   return (status == 0) ? res.get() : name;
}

inline void print_backtrace()
{
   constexpr int MAX_FRAMES = 64;
   void* frames[MAX_FRAMES];

   int count = backtrace(frames, MAX_FRAMES);
   char** symbols = backtrace_symbols(frames, count);

   std::cout << "Backtrace:\n";
   for (int i = 0; i < count; ++i) {
      std::string s = symbols[i];

      auto begin = s.find('(');
      auto end = s.find('+', begin);

      if (begin != std::string::npos && end != std::string::npos) {
         std::string mangled = s.substr(begin + 1, end - begin - 1);
         std::string pretty = demangle(mangled.c_str());
         s.replace(begin + 1, end - begin - 1, pretty);
      }

      std::cout << "  #" << i << " " << s << "\n";
   }

   free(symbols);
}

} // namespace leanstore
