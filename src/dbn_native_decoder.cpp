#include "dbn_native_decoder.hpp"

#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <vector>

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

bool DbnFileReader::NextRecordRaw(std::byte *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
                                  std::uint64_t *ts_out_out) {
	while (input_) {
		const auto got = input_->Read(reinterpret_cast<char *>(buf), sizeof(databento::RecordHeader));
		if (got == 0) {
			if (!AdvanceToNextFile()) {
				return false;
			}
			continue;
		}
		if (got != sizeof(databento::RecordHeader)) {
			throw std::runtime_error("dbn: truncated record header in file: " + current_path_);
		}

		std::memcpy(hdr, buf, sizeof(databento::RecordHeader));

		const std::size_t record_bytes = static_cast<std::size_t>(hdr->length) * kRecordHeaderLengthMultiplier;
		if (record_bytes < sizeof(databento::RecordHeader)) {
			throw std::runtime_error("dbn: record length shorter than header in: " + current_path_);
		}
		if (record_bytes > kMaxRecordLen) {
			throw std::runtime_error("dbn: record exceeds reader buffer (" + std::to_string(record_bytes) +
			                         " bytes) in: " + current_path_);
		}

		const std::size_t remaining = record_bytes - sizeof(databento::RecordHeader);
		if (remaining > 0) {
			ReadExact(*input_, reinterpret_cast<char *>(buf) + sizeof(databento::RecordHeader), remaining,
			          "record body");
		}

		if (record_len_bytes) {
			*record_len_bytes = record_bytes;
		}

		if (metadata_.ts_out) {
			std::uint64_t trailer = 0;
			ReadExact(*input_, &trailer, sizeof(trailer), "ts_out trailer");
			if (ts_out_out) {
				*ts_out_out = trailer;
			}
		}

		return true;
	}
	return false;
}

} // namespace duckdb_dbn
