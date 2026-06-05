// Phase 1 stub: databento-cpp's exceptions.hpp pulls in <httplib.h> for the
// HTTP client error types. Our header-only consumption never instantiates
// those exception classes, so we only need the symbol `httplib::Error` to
// parse. This file is on the include path ahead of any system httplib.
#pragma once

namespace httplib {
enum class Error {
	Unknown = -1,
};
} // namespace httplib
