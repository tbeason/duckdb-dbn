#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "databento/enums.hpp"
#include "databento/record.hpp"

namespace duckdb_dbn {

// Subset of the DBN metadata block we surface. The full struct in databento-cpp
// (dbn.hpp's Metadata) pulls in date/date.h which forces a heavy transitive
// dependency we want to avoid; this is the lightweight, stdlib-only form.
struct DbnMetadata {
	std::uint8_t version = 0;
	std::string dataset; // up to 16 chars, NUL-trimmed
	std::optional<databento::Schema> schema;
	std::int64_t start_ns = 0;
	std::int64_t end_ns = 0;
	std::uint64_t limit = 0;
	std::optional<databento::SType> stype_in;
	databento::SType stype_out = databento::SType::InstrumentId;
	bool ts_out = false;
	std::size_t symbol_cstr_len = 0;
};

// Abstract byte-source the decoder reads from. Two implementations live in
// the .cpp: a plain file reader and a streaming Zstd decompressor that uses
// DuckDB's bundled `duckdb_zstd::` namespace.
class IDbnInput {
public:
	virtual ~IDbnInput() = default;
	// Read up to `n` bytes into `buf`. Returns the number of bytes actually
	// read; a value < n indicates EOF.
	virtual std::size_t Read(char *buf, std::size_t n) = 0;
};

class DbnFileReader {
public:
	static constexpr std::size_t kMaxRecordLen = 528;

	explicit DbnFileReader(const std::string &path);
	// Multi-file constructor — used for glob inputs. The reader iterates the
	// files in order, transparently advancing across file boundaries inside
	// NextRecordRaw. All files must share schema, version, and ts_out — any
	// mismatch from the first file's metadata throws on open.
	explicit DbnFileReader(std::vector<std::string> paths);

	const DbnMetadata &GetMetadata() const {
		return first_metadata_;
	}
	std::uint8_t Version() const {
		return first_metadata_.version;
	}
	bool HasTsOut() const {
		return first_metadata_.ts_out;
	}

	bool NextRecordRaw(std::byte *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
	                   std::uint64_t *ts_out_out);

	// Symbol resolution for live captures. When enabled, the reader watches the
	// SymbolMappingMsg records it would otherwise skip (they're a different
	// rtype than any market-data reader's target) and maintains a live
	// instrument_id → output-symbol map. Because mappings always precede the
	// records they describe in the stream, CurrentSymbol() is correct as of the
	// most recently returned record — i.e. stream order *is* the validity
	// window. No second pass over the file, and record order is untouched.
	void EnableSymbolTracking() {
		track_symbols_ = true;
	}
	// Returns the active output symbol for `instrument_id` as of the records
	// consumed so far, or nullptr if no mapping has been seen yet. The pointed-to
	// string is stable for the reader's lifetime (backed by symbol_pool_).
	const std::string *CurrentSymbol(std::uint32_t instrument_id) const {
		auto it = live_symbols_.find(instrument_id);
		return it == live_symbols_.end() ? nullptr : it->second;
	}

	template <typename T>
	bool NextAs(T &out, databento::RType expected_rtype) {
		static_assert(sizeof(T) <= kMaxRecordLen, "record type larger than reader buffer");
		alignas(8) std::byte buf[kMaxRecordLen];
		databento::RecordHeader hdr {};
		std::size_t rec_len = 0;
		while (NextRecordRaw(buf, &hdr, &rec_len, nullptr)) {
			if (track_symbols_ && hdr.rtype == databento::RType::SymbolMapping) {
				ObserveSymbolMapping(hdr, buf, rec_len);
			}
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
	void OpenAndParseFile(const std::string &path);
	bool AdvanceToNextFile();
	// Decode a SymbolMappingMsg (version-aware) and update live_symbols_ with
	// its instrument_id → stype_out_symbol binding. No-op on malformed length.
	void ObserveSymbolMapping(const databento::RecordHeader &hdr, const std::byte *buf, std::size_t len);

	std::unique_ptr<IDbnInput> input_;
	DbnMetadata metadata_;       // current file's metadata
	DbnMetadata first_metadata_; // first file's metadata (for GetMetadata)
	std::vector<std::string> remaining_paths_;
	std::string current_path_;
	bool first_opened_ = false;

	// Symbol tracking (off unless EnableSymbolTracking() called). symbol_pool_
	// owns the strings with stable addresses (std::deque never relocates
	// existing elements on push_back), so the pointers handed out by
	// CurrentSymbol() — and snapshotted per-row by the scans — stay valid even
	// as later mappings arrive. A remap of an id appends a *new* pool entry and
	// repoints the map, leaving earlier snapshots untouched.
	bool track_symbols_ = false;
	std::deque<std::string> symbol_pool_;
	std::unordered_map<std::uint32_t, const std::string *> live_symbols_;
};

} // namespace duckdb_dbn
