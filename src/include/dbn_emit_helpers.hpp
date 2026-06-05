#pragma once

#include <cstring>

#include "duckdb.hpp"

namespace duckdb_dbn {

// Centralizes the StringVector::AddString boilerplate used by every per-schema
// scan callback. Inline so each translation unit gets its own copy.

// Emit a single-character VARCHAR cell (used for Action/Side enum-as-char and
// for TriState / plain-char fields like auction_type, significant_imbalance).
inline duckdb::string_t EmitChar1(duckdb::Vector &vec, char c) {
	return duckdb::StringVector::AddString(vec, &c, 1);
}

// Emit a NUL-terminated cstring from a fixed-width char buffer in the record.
// `max_n` bounds the strnlen so we never run off the end if the source is
// malformed and missing its terminator.
inline duckdb::string_t EmitCstr(duckdb::Vector &vec, const char *p, std::size_t max_n) {
	const auto n = ::strnlen(p, max_n);
	return duckdb::StringVector::AddString(vec, p, n);
}

} // namespace duckdb_dbn
