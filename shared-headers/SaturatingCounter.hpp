// Minimal, header-only saturating integer counter.
#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>

template <class T>
class saturating_counter
{
   static_assert(std::is_integral_v<T> && !std::is_same_v<std::remove_cv_t<T>, bool>, "saturating_counter<T> requires a non-bool integral type");

   using UT = std::make_unsigned_t<T>;  // modular domain for internal math

  public:
   using value_type = T;

   static constexpr T min_value = std::numeric_limits<T>::min();
   static constexpr T max_value = std::numeric_limits<T>::max();

   constexpr saturating_counter() noexcept = default;

   template <class U, std::enable_if_t<std::is_integral_v<U>, int> = 0>
   constexpr saturating_counter(U x) noexcept : v_(add_sat(T{0}, x))
   {
   }

   template <class U, std::enable_if_t<std::is_integral_v<U>, int> = 0>
   constexpr saturating_counter& operator=(U x) noexcept
   {
      v_ = add_sat(T{0}, x);
      return *this;
   }

   // Reads like a plain T.
   constexpr operator T() const noexcept { return v_; }
   constexpr T value() const noexcept { return v_; }

   constexpr bool at_max() const noexcept { return v_ == max_value; }
   constexpr bool at_min() const noexcept { return v_ == min_value; }

   constexpr saturating_counter& operator++() noexcept
   {
      if (v_ != max_value)
         v_ = static_cast<T>(v_ + 1);
      return *this;
   }
   constexpr saturating_counter& operator--() noexcept
   {
      if (v_ != min_value)
         v_ = static_cast<T>(v_ - 1);
      return *this;
   }
   constexpr saturating_counter operator++(int) noexcept
   {
      saturating_counter old = *this;
      ++*this;
      return old;
   }
   constexpr saturating_counter operator--(int) noexcept
   {
      saturating_counter old = *this;
      --*this;
      return old;
   }

   template <class U, std::enable_if_t<std::is_integral_v<U>, int> = 0>
   constexpr saturating_counter& operator+=(U d) noexcept
   {
      v_ = add_sat(v_, d);
      return *this;
   }
   template <class U, std::enable_if_t<std::is_integral_v<U>, int> = 0>
   constexpr saturating_counter& operator-=(U d) noexcept
   {
      v_ = sub_sat(v_, d);
      return *this;
   }

   template <class U>
   constexpr saturating_counter& operator+=(const saturating_counter<U>& o) noexcept
   {
      return *this += o.value();
   }

   template <class U>
   constexpr saturating_counter& operator-=(const saturating_counter<U>& o) noexcept
   {
      return *this -= o.value();
   }

  private:
   // ---- saturating primitives -------------------------------------------
   // add_mag / sub_mag take an *unsigned magnitude*, so the delta can never
   // overflow while being computed, and the range check is exact.

   template <class M>
   static constexpr T add_mag(T a, M m) noexcept
   {
      using W = std::common_type_t<UT, M>;  // widest unsigned of the two
      const W room = static_cast<W>(static_cast<UT>(static_cast<UT>(max_value) - static_cast<UT>(a)));
      if (static_cast<W>(m) >= room)
         return max_value;
      return static_cast<T>(static_cast<UT>(static_cast<UT>(a) + static_cast<UT>(m)));
   }

   template <class M>
   static constexpr T sub_mag(T a, M m) noexcept
   {
      using W = std::common_type_t<UT, M>;
      const W room = static_cast<W>(static_cast<UT>(static_cast<UT>(a) - static_cast<UT>(min_value)));
      if (static_cast<W>(m) >= room)
         return min_value;
      return static_cast<T>(static_cast<UT>(static_cast<UT>(a) - static_cast<UT>(m)));
   }

   template <class U>
   static constexpr U magnitude_of_negative(U d) noexcept
   {
      using UD = std::make_unsigned_t<U>;
      return static_cast<U>(static_cast<UD>(0) - static_cast<UD>(d));  // |INT_MIN| safe
   }

   template <class U>
   static constexpr T add_sat(T a, U d) noexcept
   {
      if constexpr (std::is_signed_v<U>) {
         if (d < U{0})
            return sub_mag(a, static_cast<std::make_unsigned_t<U>>(magnitude_of_negative(d)));
      }
      return add_mag(a, static_cast<std::make_unsigned_t<U>>(d));
   }

   template <class U>
   static constexpr T sub_sat(T a, U d) noexcept
   {
      if constexpr (std::is_signed_v<U>) {
         if (d < U{0})
            return add_mag(a, static_cast<std::make_unsigned_t<U>>(magnitude_of_negative(d)));
      }
      return sub_mag(a, static_cast<std::make_unsigned_t<U>>(d));
   }

   T v_{};
};

using saturating_u8 = saturating_counter<std::uint8_t>;
using saturating_u16 = saturating_counter<std::uint16_t>;
using saturating_u32 = saturating_counter<std::uint32_t>;
using saturating_u64 = saturating_counter<std::uint64_t>;
