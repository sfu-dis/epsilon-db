// get_waf.cpp - read NVMe write counters exactly, with no text parsing.
//
// Issues the Get Log Page admin command straight to the drive and reads the
// counters out of the returned binary log page at their spec-defined byte
// offsets. Nothing here depends on how nvme-cli formats its output.
//
//   sudo ./get_waf -d /dev/nvme3n1
//   Host bytes  : 39292248064000
//   Media bytes : 49840493363200
//   WAF         : 1.268456
//
// Counters are cumulative over the life of the drive. For a WAF measurement,
// run this before and after the workload and subtract.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <errno.h>
#include <fcntl.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <unistd.h>

using u128 = __uint128_t;

// ---------------------------------------------------------------- log fields

// A counter, located by log page id, byte offset inside the page, and width.
struct Field {
   uint8_t lid;    // log page identifier
   uint16_t off;   // byte offset within the page
   uint8_t width;  // width in bytes, 1..16
};

// NVMe Base Spec, SMART / Health Information log page 02h:
//   bytes 32..47  Data Units Read
//   bytes 48..63  Data Units Written
// The unit is one thousand 512-byte units, i.e. 512000 bytes.
static constexpr Field F_HOST_UNITS{0x02, 48, 16};
static constexpr uint64_t HOST_UNIT_BYTES = 512000;

// OCP Datacenter NVMe SSD Spec, SMART / Health Extended log page C0h:
//   bytes 0..15   Physical Media Units Written, in bytes
static constexpr Field F_MEDIA_BYTES{0xC0, 0, 16};

static constexpr size_t LOG_LEN = 512;

// ---------------------------------------------------------------- helpers

// Little-endian integer out of a byte buffer. Exact for any width up to 16.
static u128 read_le(const uint8_t* p, unsigned width)
{
   u128 v = 0;
   for (unsigned i = width; i-- > 0;)
      v = (v << 8) | p[i];
   return v;
}

// printf has no conversion for 128-bit integers, so build the digits here.
static std::string to_dec(u128 v)
{
   char buf[41];
   int i = 40;
   buf[i] = '\0';
   if (v == 0)
      buf[--i] = '0';
   while (v) {
      buf[--i] = char('0' + unsigned(v % 10));
      v /= 10;
   }
   return std::string(buf + i);
}

static void die(const char* what)
{
   fprintf(stderr, "get_waf: %s: %s\n", what, strerror(errno));
   exit(1);
}

// ---------------------------------------------------------------- nvme

// Get Log Page (opcode 02h), whole page from offset 0.
static void get_log(int fd, uint8_t lid, uint8_t uuid_index, uint8_t* buf, size_t len)
{
   const uint32_t numd = uint32_t(len / 4) - 1;  // zero-based dword count

   nvme_passthru_cmd cmd{};
   cmd.opcode = 0x02;
   cmd.nsid = 0xFFFFFFFF;  // whole controller
   cmd.addr = uint64_t(uintptr_t(buf));
   cmd.data_len = uint32_t(len);
   cmd.cdw10 = uint32_t(lid) |           // LID    bits 7:0
               ((numd & 0xFFFF) << 16);  // NUMDL  bits 31:16
   cmd.cdw11 = (numd >> 16) & 0xFFFF;    // NUMDU  bits 15:0
   cmd.cdw12 = 0;                        // log page offset, low
   cmd.cdw13 = 0;                        // log page offset, high
   cmd.cdw14 = uuid_index & 0x7F;        // UUID index for vendor pages
   cmd.timeout_ms = 15000;

   int rc = ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);
   if (rc < 0)
      die("ioctl NVME_IOCTL_ADMIN_CMD");
   if (rc > 0) {
      fprintf(stderr, "get_waf: get log page %02Xh failed, NVMe status 0x%x\n", lid, rc);
      exit(1);
   }
}

static u128 read_field(int fd, const Field& f, uint8_t uuid_index)
{
   static_assert(sizeof(u128) == 16, "need a 128-bit integer type");
   if (f.width < 1 || f.width > 16 || size_t(f.off) + f.width > LOG_LEN) {
      fprintf(stderr, "get_waf: field %02Xh:%u:%u is outside the log page\n", f.lid, f.off, f.width);
      exit(1);
   }
   alignas(4096) static uint8_t buf[LOG_LEN];
   memset(buf, 0, sizeof buf);
   get_log(fd, f.lid, uuid_index, buf, LOG_LEN);
   return read_le(buf + f.off, f.width);
}

// Print a page as aligned 64-bit and 128-bit values, to locate a field.
static void dump(int fd, uint8_t lid, uint8_t uuid_index)
{
   alignas(4096) static uint8_t buf[LOG_LEN];
   memset(buf, 0, sizeof buf);
   get_log(fd, lid, uuid_index, buf, LOG_LEN);

   printf("log page %02Xh, uuid index %u\n", lid, uuid_index);
   printf("%-6s %-24s %s\n", "offset", "u64 at offset", "u128 at offset");
   for (size_t o = 0; o < LOG_LEN; o += 8) {
      std::string a = to_dec(read_le(buf + o, 8));
      std::string b = (o + 16 <= LOG_LEN) ? to_dec(read_le(buf + o, 16)) : "-";
      printf("%-6zu %-24s %s\n", o, a.c_str(), b.c_str());
   }
}

// ---------------------------------------------------------------- main

static void usage(void)
{
   fprintf(stderr,
           "usage: get_waf -d DEVICE [options]\n"
           "  -d DEV   nvme character or block device, e.g. /dev/nvme3n1\n"
           "  -u IDX   UUID index for vendor log pages (default 0)\n"
           "  -D LID   dump log page LID as numbers and exit\n"
           "  -H       print in human readable format\n");
   exit(1);
}

int main(int argc, char** argv)
{
   const char* dev = nullptr;
   uint8_t uuid_index = 0;
   int dump_lid = -1;
   bool human = false;

   int c;
   while ((c = getopt(argc, argv, "d:u:D:H")) != -1) {
      char* end = nullptr;
      switch (c) {
         case 'd':
            dev = optarg;
            break;
         case 'u':
            uuid_index = uint8_t(strtoul(optarg, &end, 0));
            break;
         case 'D':
            dump_lid = int(strtoul(optarg, &end, 0));
            break;
         case 'H':
            human = true;
            break;
         default:
            usage();
      }
      if (end && *end) {  // reject "12abc", accept "0xC0"
         fprintf(stderr, "get_waf: -%c takes a number\n", c);
         exit(1);
      }
   }
   if (!dev)
      usage();

   int fd = open(dev, O_RDONLY);
   if (fd < 0)
      die(dev);

   if (dump_lid >= 0) {
      dump(fd, uint8_t(dump_lid), uuid_index);
      close(fd);
      return 0;
   }

   const u128 host = read_field(fd, F_HOST_UNITS, uuid_index) * HOST_UNIT_BYTES;
   const u128 media = read_field(fd, F_MEDIA_BYTES, uuid_index);
   close(fd);

   if (human) {
      printf("Host bytes  : ");
   }
   printf("%s\n", to_dec(host).c_str());
   if (human) {
      printf("Media bytes : ");
   }
   printf("%s\n", to_dec(media).c_str());
   if (host == 0) {
      printf("WAF         : n/a (host counter is zero)\n");
      return 1;
   } else {
      if (human) {
         printf("WAF         : ");
      }
      printf("%.6Lf\n", (long double)media / (long double)host);
   }
   return 0;
}
