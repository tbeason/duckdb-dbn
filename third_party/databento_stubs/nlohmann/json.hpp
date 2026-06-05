// Phase 1 stub: databento-cpp's exceptions.hpp declares JsonResponseError
// static methods that take `nlohmann::json` and `nlohmann::detail::parse_error`
// by reference. We never link the bodies of those methods (they live in
// databento-cpp's exceptions.cpp, which we don't compile), so the compiler
// only needs the type names to be visible.
#pragma once

namespace nlohmann {
class json {};
namespace detail {
class parse_error {};
} // namespace detail
} // namespace nlohmann
