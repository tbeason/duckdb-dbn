#include "dbn_native_decoder.hpp"

#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <vector>

#include "databento/v1.hpp"
#include "databento/v2.hpp"

#define ZSTD_STATIC_LINKING_ONLY 1
#include <zstd.h>

namespace duckdb_dbn {

namespace {

constexpr std::size_t kMagicSize = 4;
constexpr std::size_t kMetadataPreludeSize = 8;
constexpr std::uint32_t kZstdMagicNumber = 0xFD2FB528;
constexpr char kDbnPrefix[] = "DBN";
constexpr std::size_t kFixedMetadataLen = 100;
constexpr std::size_t kDatasetCstrLen = 16;
constexpr std::size_t kRecordHeaderLengthMultiplier = 4;
constexpr std::size_t kSymbolCstrLenV1 = 22;
constexpr std::uint16_t kNullSchema = std::numeric_limits<std::uint16_t>::max();
constexpr std::uint8_t kNullSType = std::numeric_limits<std::uint8_t>::max();

constexpr std::size_t kOffSchema = 16;
constexpr std::size_t kOffStart = 18;
constexpr std::size_t kOffEnd = 26;
constexpr std::size_t kOffLimit = 34;
constexpr std::size_t kOffStypeInV2 = 42;
constexpr std::size_t kOffStypeInV1 = 50;

template <typename T>
T ReadLE(const std::uint8_t *p) {
	T v {};
	std::memcpy(&v, p, sizeof(T));
	return v;
}

class FileInput : public IDbnInput {
public:
	explicit FileInput(const std::string &path) : f_(path, std::ios::binary) {
		if (!f_) {
			throw std::runtime_error("dbn: failed to open file: " + path);
		}
	}
	std::size_t Read(char *buf, std::size_t n) override {
		f_.read(buf, static_cast<std::streamsize>(n));
		return static_cast<std::size_t>(f_.gcount());
	}

private:
	std::ifstream f_;
};

class ZstdInput : public IDbnInput {
public:
	explicit ZstdInput(const std::string &path) : f_(path, std::ios::binary) {
		if (!f_) {
			throw std::runtime_error("dbn: failed to open file: " + path);
		}
		dstream_ = duckdb_zstd::ZSTD_createDStream();
		if (!dstream_) {
			throw std::runtime_error("dbn: ZSTD_createDStream returned null");
		}
		const auto init_ret = duckdb_zstd::ZSTD_initDStream(dstream_);
		if (duckdb_zstd::ZSTD_isError(init_ret)) {
			throw std::runtime_error(std::string("dbn: ZSTD_initDStream: ") +
			                         duckdb_zstd::ZSTD_getErrorName(init_ret));
		}
		in_buf_.resize(duckdb_zstd::ZSTD_DStreamInSize());
		zin_.src = in_buf_.data();
		zin_.size = 0;
		zin_.pos = 0;
	}
	~ZstdInput() override {
		if (dstream_) {
			duckdb_zstd::ZSTD_freeDStream(dstream_);
		}
	}

	std::size_t Read(char *buf, std::size_t n) override {
		duckdb_zstd::ZSTD_outBuffer zout {};
		zout.dst = buf;
		zout.size = n;
		zout.pos = 0;

		while (zout.pos < n) {
			if (zin_.pos == zin_.size) {
				f_.read(in_buf_.data(), static_cast<std::streamsize>(in_buf_.size()));
				const auto got = static_cast<std::size_t>(f_.gcount());
				if (got == 0) {
					return zout.pos;
				}
				zin_.size = got;
				zin_.pos = 0;
			}
			const auto ret = duckdb_zstd::ZSTD_decompressStream(dstream_, &zout, &zin_);
			if (duckdb_zstd::ZSTD_isError(ret)) {
				throw std::runtime_error(std::string("dbn: zstd decompression error: ") +
				                         duckdb_zstd::ZSTD_getErrorName(ret));
			}
		}
		return zout.pos;
	}

private:
	std::ifstream f_;
	duckdb_zstd::ZSTD_DStream *dstream_ = nullptr;
	std::vector<char> in_buf_;
	duckdb_zstd::ZSTD_inBuffer zin_ {};
};

std::unique_ptr<IDbnInput> OpenInput(const std::string &path) {
	std::ifstream peek(path, std::ios::binary);
	if (!peek) {
		throw std::runtime_error("dbn: failed to open file: " + path);
	}
	std::array<std::uint8_t, kMagicSize> magic {};
	peek.read(reinterpret_cast<char *>(magic.data()), magic.size());
	if (peek.gcount() != static_cast<std::streamsize>(magic.size())) {
		throw std::runtime_error("dbn: file too short to contain magic bytes");
	}
	peek.close();

	const std::uint32_t magic_word = ReadLE<std::uint32_t>(magic.data());
	if (magic_word == kZstdMagicNumber) {
		return std::make_unique<ZstdInput>(path);
	}
	if (std::memcmp(magic.data(), kDbnPrefix, 3) == 0) {
		return std::make_unique<FileInput>(path);
	}
	throw std::runtime_error("dbn: file is neither a DBN nor a Zstd-compressed DBN: " + path);
}

void ReadExact(IDbnInput &input, void *buf, std::size_t n, const char *what) {
	const auto got = input.Read(reinterpret_cast<char *>(buf), n);
	if (got != n) {
		throw std::runtime_error(std::string("dbn: short read while reading ") + what + " (expected " +
		                         std::to_string(n) + " bytes, got " + std::to_string(got) + ")");
	}
}

} // namespace

DbnFileReader::DbnFileReader(const std::string &path) : DbnFileReader(std::vector<std::string> {path}) {
}

DbnFileReader::DbnFileReader(std::vector<std::string> paths) {
	if (paths.empty()) {
		throw std::runtime_error("dbn: no files to read");
	}
	remaining_paths_ = std::move(paths);
	AdvanceToNextFile();
}

void DbnFileReader::OpenAndParseFile(const std::string &path) {
	input_ = OpenInput(path);
	current_path_ = path;

	std::array<std::uint8_t, kMetadataPreludeSize> prelude {};
	ReadExact(*input_, prelude.data(), prelude.size(), "metadata prelude");

	if (std::memcmp(prelude.data(), kDbnPrefix, 3) != 0) {
		throw std::runtime_error("dbn: missing DBN prefix in file: " + path);
	}
	DbnMetadata md {};
	md.version = prelude[3];

	const std::uint32_t frame_size = ReadLE<std::uint32_t>(prelude.data() + kMagicSize);
	if (frame_size < kFixedMetadataLen) {
		throw std::runtime_error("dbn: metadata frame shorter than fixed-metadata length in: " + path);
	}

	std::vector<std::uint8_t> meta_buf(frame_size);
	ReadExact(*input_, meta_buf.data(), frame_size, "metadata frame");

	const auto *ds_ptr = reinterpret_cast<const char *>(meta_buf.data());
	const auto ds_len = ::strnlen(ds_ptr, kDatasetCstrLen);
	md.dataset.assign(ds_ptr, ds_len);

	const auto raw_schema = ReadLE<std::uint16_t>(meta_buf.data() + kOffSchema);
	if (raw_schema != kNullSchema) {
		md.schema = static_cast<databento::Schema>(raw_schema);
	}

	md.start_ns = static_cast<std::int64_t>(ReadLE<std::uint64_t>(meta_buf.data() + kOffStart));
	md.end_ns = static_cast<std::int64_t>(ReadLE<std::uint64_t>(meta_buf.data() + kOffEnd));
	md.limit = ReadLE<std::uint64_t>(meta_buf.data() + kOffLimit);

	const std::size_t stype_in_off = (md.version == 1) ? kOffStypeInV1 : kOffStypeInV2;
	const std::uint8_t raw_stype_in = meta_buf[stype_in_off];
	if (raw_stype_in != kNullSType) {
		md.stype_in = static_cast<databento::SType>(raw_stype_in);
	}
	md.stype_out = static_cast<databento::SType>(meta_buf[stype_in_off + 1]);
	md.ts_out = (meta_buf[stype_in_off + 2] != 0);

	if (md.version > 1) {
		md.symbol_cstr_len = ReadLE<std::uint16_t>(meta_buf.data() + stype_in_off + 3);
	} else {
		md.symbol_cstr_len = kSymbolCstrLenV1;
	}

	if (first_opened_) {
		if (md.schema != first_metadata_.schema || md.version != first_metadata_.version ||
		    md.ts_out != first_metadata_.ts_out) {
			throw std::runtime_error("dbn: multi-file scan requires consistent schema, version, and "
			                         "ts_out across files; mismatched file: " +
			                         path);
		}
	} else {
		first_metadata_ = md;
		first_opened_ = true;
	}
	metadata_ = md;
}

bool DbnFileReader::AdvanceToNextFile() {
	if (remaining_paths_.empty()) {
		input_.reset();
		return false;
	}
	auto path = std::move(remaining_paths_.front());
	remaining_paths_.erase(remaining_paths_.begin());
	OpenAndParseFile(path);
	return true;
}

bool DbnFileReader::EnsureBytes(std::size_t n) {
	std::size_t remaining = buf_len_ - buf_pos_;
	if (remaining >= n) {
		return true;
	}
	// Compact the unconsumed tail to the front so the read below is contiguous.
	if (buf_pos_ != 0) {
		if (remaining != 0) {
			std::memmove(read_buf_.data(), read_buf_.data() + buf_pos_, remaining);
		}
		buf_pos_ = 0;
		buf_len_ = remaining;
	}
	// Size the buffer to the block size (or larger if a single record exceeds
	// it — can't happen for valid DBN, but keeps EnsureBytes self-contained).
	const std::size_t want = (n > kReadBufSize) ? n : kReadBufSize;
	if (read_buf_.size() < want) {
		read_buf_.resize(want);
	}
	// One large read per refill instead of one per record. For ZstdInput this
	// drives ZSTD_decompressStream with a ~1 MiB output buffer rather than 16 B.
	// input_ is null once the last file is exhausted (AdvanceToNextFile resets
	// it) — guard so a post-EOF call returns "no bytes" instead of dereferencing.
	while (input_ && buf_len_ < n) {
		const std::size_t space = read_buf_.size() - buf_len_;
		const std::size_t got = input_->Read(reinterpret_cast<char *>(read_buf_.data()) + buf_len_, space);
		if (got == 0) {
			break; // EOF on the current input
		}
		buf_len_ += got;
	}
	return (buf_len_ - buf_pos_) >= n;
}

const std::byte *DbnFileReader::NextRecordView(databento::RecordHeader *hdr, std::size_t *record_len_bytes) {
	for (;;) {
		if (!EnsureBytes(sizeof(databento::RecordHeader))) {
			// A clean file boundary leaves zero unconsumed bytes; anything else is
			// a truncated trailing record.
			if (buf_len_ - buf_pos_ != 0) {
				throw std::runtime_error("dbn: truncated record header in file: " + current_path_);
			}
			if (!AdvanceToNextFile()) {
				return nullptr;
			}
			continue;
		}
		std::memcpy(hdr, read_buf_.data() + buf_pos_, sizeof(databento::RecordHeader));

		const std::size_t record_bytes = static_cast<std::size_t>(hdr->length) * kRecordHeaderLengthMultiplier;
		if (record_bytes < sizeof(databento::RecordHeader)) {
			throw std::runtime_error("dbn: record length shorter than header in: " + current_path_);
		}
		if (record_bytes > kMaxRecordLen) {
			throw std::runtime_error("dbn: record exceeds reader buffer (" + std::to_string(record_bytes) +
			                         " bytes) in: " + current_path_);
		}

		// Pull the whole record plus any ts_out trailer into the buffer so the
		// returned view is contiguous and the trailer is consumed atomically.
		const std::size_t total = record_bytes + (metadata_.ts_out ? sizeof(std::uint64_t) : 0);
		if (!EnsureBytes(total)) {
			throw std::runtime_error("dbn: short read while reading record body in: " + current_path_);
		}
		// Re-fetch the pointer: EnsureBytes(total) may have compacted/reallocated.
		const std::byte *p = read_buf_.data() + buf_pos_;
		buf_pos_ += total;
		if (record_len_bytes) {
			*record_len_bytes = record_bytes;
		}
		return p;
	}
}

bool DbnFileReader::NextRecordRaw(std::byte *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
                                  std::uint64_t *ts_out_out) {
	std::size_t rec_len = 0;
	const std::byte *p = NextRecordView(hdr, &rec_len);
	if (!p) {
		return false;
	}
	std::memcpy(buf, p, rec_len);
	if (record_len_bytes) {
		*record_len_bytes = rec_len;
	}
	// The trailer sits at p[rec_len..); NextRecordView already consumed it, but
	// the bytes remain valid in the buffer until the next view call.
	if (ts_out_out && metadata_.ts_out) {
		std::memcpy(ts_out_out, p + rec_len, sizeof(std::uint64_t));
	}
	return true;
}

void DbnFileReader::ObserveSymbolMapping(const databento::RecordHeader &hdr, const std::byte *buf,
                                         std::size_t len) {
	// Pull stype_out_symbol — the output-symbology symbol bound to this
	// instrument_id (the human-readable symbol, e.g. the OSI option symbol on
	// OPRA). v2/v3 share the canonical 176-byte SymbolMappingMsg; v1 is the
	// 80-byte legacy layout. memcpy into a local to dodge alignment/aliasing.
	const char *sym = nullptr;
	std::size_t cap = 0;
	databento::SymbolMappingMsg v2 {};
	databento::v1::SymbolMappingMsg v1 {};
	if (first_metadata_.version >= 2 && len == sizeof(v2)) {
		std::memcpy(&v2, buf, sizeof(v2));
		sym = v2.stype_out_symbol.data();
		cap = v2.stype_out_symbol.size();
	} else if (len == sizeof(v1)) {
		std::memcpy(&v1, buf, sizeof(v1));
		sym = v1.stype_out_symbol.data();
		cap = v1.stype_out_symbol.size();
	} else {
		return; // unrecognized length — leave the map untouched
	}

	std::size_t n = 0;
	while (n < cap && sym[n] != '\0') {
		++n;
	}

	// Skip the update if the binding is unchanged — keeps symbol_pool_ from
	// growing on the (common) repeated-snapshot case.
	auto it = live_symbols_.find(hdr.instrument_id);
	if (it != live_symbols_.end() && it->second->size() == n &&
	    std::memcmp(it->second->data(), sym, n) == 0) {
		return;
	}
	symbol_pool_.emplace_back(sym, n);
	live_symbols_[hdr.instrument_id] = &symbol_pool_.back();
}

} // namespace duckdb_dbn
