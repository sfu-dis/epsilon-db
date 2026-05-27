#include "Logger.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
Logger::Logger(const std::string& filename)
{
   file_.open(filename, std::ios::trunc);
   if (!file_.is_open()) {
      throw std::runtime_error("Logger: failed to open file: " + filename);
   }
}
// -------------------------------------------------------------------------------------
Logger::~Logger()
{
   if (file_.is_open()) {
      file_.close();
   }
}
// -------------------------------------------------------------------------------------
void Logger::log(const char* file, int line, const char* func, int level, const char* fmt, ...)
{
   OutputLogHeader(file, line, func, level);

   char buf[1024];
   va_list args;
   va_start(args, fmt);
   ::vsnprintf(buf, sizeof(buf), fmt, args);
   va_end(args);

   file_ << buf << "\n";
   file_.flush();
}
// -------------------------------------------------------------------------------------
void Logger::OutputLogHeader(const char* file, int line, const char* func, int level)
{
   time_t t = ::time(nullptr);
   tm* curTime = ::localtime(&t);  // NOLINT
   char time_str[32];
   ::strftime(time_str, sizeof(time_str), LOG_LOG_TIME_FORMAT, curTime);

   const char* type;
   switch (level) {
      case LOG_LEVEL_ERROR:
         type = "ERROR";
         break;
      case LOG_LEVEL_WARN:
         type = "WARN ";
         break;
      case LOG_LEVEL_INFO:
         type = "INFO ";
         break;
      case LOG_LEVEL_DEBUG:
         type = "DEBUG";
         break;
      case LOG_LEVEL_TRACE:
         type = "TRACE";
         break;
      default:
         type = "UNKNOWN";
   }

   file_ << time_str << " (" << ::syscall(SYS_gettid) << ")" << " [" << file << ":" << line << ":" << func << "] " << type << " - ";
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
