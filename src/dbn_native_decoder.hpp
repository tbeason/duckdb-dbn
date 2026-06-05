#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>

#include "databento/record.hpp"

namespace duckdb_dbn {

// Minimal Phase-2 DBN file reader. Reuses databento-cpp's wire-compatible
// record struct definitions (TradeMsg, MboMsg, Mbp1Msg, ...) so anyone porting
// between this extension and databento-cpp sees identical byte layouts.
//
// Scope:
//   - Uncompressed .dbn only (throws on Zstd magic). Phase 3 adds .dbn.zst.
//   - Reads the ts_out flag from the metadata header so files with ts_out=true
//     stay aligned record-by-record; the rest of the metadata block is skipped.
//   - Templated NextAs<T> dispatches per record type, filtered by rtype.
//   - Variable-length records (e.g. InstrumentDefMsg across DBN v1/v2/v3) are
//     out of scope: NextAs<T> throws on size mismatch with a clear error.
class DbnFileReader {
public:
	// Worst-case record body in Phase 2/3: v3 InstrumentDefMsg (520 bytes).
	// + 8 bytes for an optional ts_out trailer that some streams append per record.
	static constexpr std::size_t kMaxRecordLen = 528;

	explicit DbnFileReader(const std::string &path);

	std::uint8_t Version() const {
		return version_;
	}
	bool HasTsOut() const {
		return ts_out_;
	}

	// Read one record. Returns false at clean EOF; throws on truncation or
	// oversize records. On success `*hdr` holds the parsed header, `buf` holds
	// the full record bytes (including header), and `*record_len_bytes` is the
	// header.length * 4 byte count. If the stream has ts_out=true, the trailing
	// 8 bytes are consumed and (if `ts_out_out` is non-null) stored.
	bool NextRecordRaw(std::byte *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
	                   std::uint64_t *ts_out_out);

	// Typed convenience used by the per-schema scan callbacks. Loops calling
	// NextRecordRaw, skipping records whose rtype != expected_rtype, until one
	// matches or EOF. On a match, sizeof(T) must equal record_len_bytes
	// (otherwise we're looking at a v1/v2 variant of a record whose layout
	// differs from the cpp struct — throws with a version note).
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
	std::uint8_t version_ = 0;
	bool ts_out_ = false;
};

} // namespace duckdb_dbn
