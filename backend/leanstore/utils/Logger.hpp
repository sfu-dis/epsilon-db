/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   This software may be modified and distributed under the terms         *
 *   of the MIT license.  See the LICENSE file for details.                *
 *                                                                         *
 ***************************************************************************/
/***************************************************************************
 * Adopted from H-Store for epsilonDB                                      *
 ***************************************************************************/
#pragma once

#include <sys/syscall.h>
#include <unistd.h>
#include <cstdarg>
#include <ctime>
#include <fstream>
#include <stdexcept>
#include <string>
// -------------------------------------------------------------------------------------
using cstr = const char*;
// -------------------------------------------------------------------------------------
static constexpr auto PastLastSlash(cstr a, cstr b) -> cstr
{
   return *a == '\0' ? b : *a == '/' ? PastLastSlash(a + 1, a + 1) : PastLastSlash(a + 1, b);
}

static constexpr auto PastLastSlash(cstr a) -> cstr
{
   return PastLastSlash(a, a);
}
#define __SHORT_FILE__                              \
   ({                                               \
      constexpr cstr sf__{PastLastSlash(__FILE__)}; \
      sf__;                                         \
   })
// -------------------------------------------------------------------------------------
static constexpr int LOG_LEVEL_OFF = 1000;
static constexpr int LOG_LEVEL_ERROR = 500;
static constexpr int LOG_LEVEL_WARN = 400;
static constexpr int LOG_LEVEL_INFO = 300;
static constexpr int LOG_LEVEL_DEBUG = 200;
static constexpr int LOG_LEVEL_TRACE = 100;
static constexpr int LOG_LEVEL_ALL = 0;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
#define LOG_LOG_TIME_FORMAT "%H:%M:%S"

#ifndef LOG_LEVEL
#ifndef NDEBUG
static constexpr int LOG_LEVEL = LOG_LEVEL_DEBUG;
#else
static constexpr int LOG_LEVEL = LOG_LEVEL_INFO;
#endif
#endif

#if !defined(__FUNCTION__) && !defined(__GNUC__)
#define __FUNCTION__ ""
#endif

class Logger
{
  public:
   explicit Logger(const std::string& filename);
   ~Logger();

   Logger(const Logger&) = delete;
   Logger& operator=(const Logger&) = delete;

   void log(const char* file, int line, const char* func, int level, const char* fmt, ...);

  private:
   std::ofstream file_;
   void OutputLogHeader(const char* file, int line, const char* func, int level);
};

// LOG_ERROR
#ifdef LOG_ERROR_ENABLED
#undef LOG_ERROR_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ERROR_ENABLED
#define LOG_ERROR(logger, ...) (logger)->log(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_ERROR, __VA_ARGS__)
#else
#define LOG_ERROR(logger, ...) ((void)0)
#endif

// LOG_WARN
#ifdef LOG_WARN_ENABLED
#undef LOG_WARN_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_WARN_ENABLED
#define LOG_WARN(logger, ...) (logger)->log(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_WARN, __VA_ARGS__)
#else
#define LOG_WARN(logger, ...) ((void)0)
#endif

// LOG_INFO
#ifdef LOG_INFO_ENABLED
#undef LOG_INFO_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_INFO_ENABLED
#define LOG_INFO(logger, ...) (logger)->log(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_INFO, __VA_ARGS__)
#else
#define LOG_INFO(logger, ...) ((void)0)
#endif

// LOG_DEBUG
#ifdef LOG_DEBUG_ENABLED
#undef LOG_DEBUG_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_DEBUG_ENABLED
#define LOG_DEBUG(logger, ...) (logger)->log(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_DEBUG, __VA_ARGS__)
#else
#define LOG_DEBUG(logger, ...) ((void)0)
#endif

// LOG_TRACE
#ifdef LOG_TRACE_ENABLED
#undef LOG_TRACE_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_TRACE
#define LOG_TRACE_ENABLED
#define LOG_TRACE(logger, ...) (logger)->log(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_TRACE, __VA_ARGS__)
#else
#define LOG_TRACE(logger, ...) ((void)0)
#endif

}  // namespace utils
}  // namespace leanstore
