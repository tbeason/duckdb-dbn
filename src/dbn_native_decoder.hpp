#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "databento/enums.hpp"
#include "databento/record.hpp"

namespace duckdb_dbn {

// Minimal stand-in for std::optional (the extension compiles as C++14) covering
// exactly the operations the readers use on the two nullable metadata fields.
template <typename T>
class Optional {
public:
	Optional() : has_(false), value_() {
	}
	Optional &operator=(T v) {
		has_ = true;
		value_ = v;
		return *this;
	}
	bool has_value() const {
		return has_;
	}
	const T &operator*() const {
		return value_;
	}
	bool operator==(const Optional &other) const {
		return has_ == other.has_ && (!has_ || value_ == other.value_);
	}
	bool operator!=(const Optional &other) const {
		return !(*this == other);
	}

private:
	bool has_;
	T value_;
};

// Subset of the DBN metadata block we surface. The full struct in databento-cpp
// (dbn.hpp's Metadata) pulls in date/date.h which forces a heavy transitive
// dependency we want to avoid; this is the lightweight, stdlib-only form.
struct DbnMetadata {
	std::uint8_t version = 0;
	std::string dataset; // up to 16 chars, NUL-trimmed
	Optional<databento::Schema> schema;
	std::int64_t start_ns = 0;
	std::int64_t end_ns = 0;
	std::uint64_t limit = 0;
	Optional<databento::SType> stype_in;
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

	bool NextRecordRaw(std::uint8_t *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
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
		databento::RecordHeader hdr {};
		std::size_t rec_len = 0;
		// Zero-copy: NextRecordView hands back a pointer into the reader's block
		// buffer (refilled in large chunks), so the hot path makes neither a
		// per-record input read nor a body copy — just the single memcpy into the
		// caller's typed struct below.
		const std::uint8_t *p;
		while ((p = NextRecordView(&hdr, &rec_len)) != nullptr) {
			if (track_symbols_ && hdr.rtype == databento::RType::SymbolMapping) {
				ObserveSymbolMapping(hdr, p, rec_len);
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
			std::memcpy(&out, p, sizeof(T));
			return true;
		}
		return false;
	}

private:
	void OpenAndParseFile(const std::string &path);
	bool AdvanceToNextFile();
	// Decode a SymbolMappingMsg (version-aware) and update live_symbols_ with
	// its instrument_id → stype_out_symbol binding. No-op on malformed length.
	void ObserveSymbolMapping(const databento::RecordHeader &hdr, const std::uint8_t *buf, std::size_t len);

	// Block-buffered record reader. NextRecordView returns a pointer to the next
	// record's contiguous bytes inside read_buf_ (valid until the next call),
	// having consumed record + any ts_out trailer. EnsureBytes guarantees `n`
	// contiguous bytes from buf_pos_, compacting and refilling from input_ in
	// large reads — so per-record input reads collapse to ~one per block.
	const std::uint8_t *NextRecordView(databento::RecordHeader *hdr, std::size_t *record_len_bytes);
	bool EnsureBytes(std::size_t n);

	static constexpr std::size_t kReadBufSize = 1u << 20; // 1 MiB block buffer

	std::unique_ptr<IDbnInput> input_;
	DbnMetadata metadata_;       // current file's metadata
	DbnMetadata first_metadata_; // first file's metadata (for GetMetadata)
	std::vector<std::string> remaining_paths_;
	std::string current_path_;
	bool first_opened_ = false;

	// Block buffer for NextRecordView. Records are served from [buf_pos_, buf_len_).
	std::vector<std::uint8_t> read_buf_;
	std::size_t buf_pos_ = 0;
	std::size_t buf_len_ = 0;

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
