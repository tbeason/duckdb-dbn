#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>

#include "databento/enums.hpp"
#include "databento/record.hpp"

namespace duckdb_dbn {

// Subset of the DBN metadata block we surface. The full struct in databento-cpp
// (dbn.hpp's Metadata) pulls in date/date.h which forces a heavy transitive
// dependency we want to avoid; this is the lightweight, stdlib-only form.
struct DbnMetadata {
	std::uint8_t version = 0;
	std::string dataset; // up to 16 chars, NUL-trimmed
	// nullopt when the file's schema field is kNullSchema (mixed-schema files)
	std::optional<databento::Schema> schema;
	std::int64_t start_ns = 0;
	std::int64_t end_ns = 0;
	std::uint64_t limit = 0;
	// nullopt when stype_in is the file's kNullSType sentinel
	std::optional<databento::SType> stype_in;
	databento::SType stype_out = databento::SType::InstrumentId;
	bool ts_out = false;
	std::size_t symbol_cstr_len = 0;
};

class DbnFileReader {
public:
	// Worst-case record body in Phase 2/3: v3 InstrumentDefMsg (520 bytes).
	// + 8 bytes for an optional ts_out trailer that some streams append per record.
	static constexpr std::size_t kMaxRecordLen = 528;

	explicit DbnFileReader(const std::string &path);

	const DbnMetadata &GetMetadata() const {
		return metadata_;
	}
	std::uint8_t Version() const {
		return metadata_.version;
	}
	bool HasTsOut() const {
		return metadata_.ts_out;
	}

	bool NextRecordRaw(std::byte *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
	                   std::uint64_t *ts_out_out);

	template <typename T>
	bool NextAs(T &out, databento::RType expected_rtype) {
		static_assert(sizeof(T) <= kMaxRecordLen, "record type larger than reader buffer");
		alignas(8) std::byte buf[kMaxRecordLen];
		databento::RecordHeader hdr {};
		std::size_t rec_len = 0;
		while (NextRecordRaw(buf, &hdr, &rec_len, nullptr)) {
			if (hdr.rtype != expected_rtype) {
				continue;
			}
			if (rec_len != sizeof(T)) {
				throw std::runtime_error("dbn: record size mismatch (expected " + std::to_string(sizeof(T)) +
				                         " bytes, got " + std::to_string(rec_len) +
				                         "); the source file is likely a DBN v1/v2 layout for a record whose "
				                         "C++ struct is the v3 layout (e.g. InstrumentDefMsg, StatMsg). "
				                         "Version-aware decoding is a future-phase item.");
			}
			std::memcpy(&out, buf, sizeof(T));
			return true;
		}
		return false;
	}

private:
	std::ifstream file_;
	DbnMetadata metadata_;
};

} // namespace duckdb_dbn
