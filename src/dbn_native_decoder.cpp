#include "dbn_native_decoder.hpp"

#include <array>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <vector>

namespace duckdb_dbn {

namespace {

// Mirrors databento-cpp/src/dbn_constants.hpp.
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

// Fixed metadata layout offsets (from start of metadata block, not file).
//   0..15  dataset cstr
//   16..17 schema u16
//   18..25 start  u64
//   26..33 end    u64
//   34..41 limit  u64
//   v1 ONLY: 42..49 deprecated record_count u64
//   then: stype_in u8, stype_out u8, ts_out u8 (in that order)
//   v2+: symbol_cstr_len u16
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

} // namespace

DbnFileReader::DbnFileReader(const std::string &path) : file_(path, std::ios::binary) {
	if (!file_) {
		throw std::runtime_error("dbn: failed to open file: " + path);
	}

	std::array<std::uint8_t, kMetadataPreludeSize> prelude {};
	file_.read(reinterpret_cast<char *>(prelude.data()), prelude.size());
	if (file_.gcount() != static_cast<std::streamsize>(prelude.size())) {
		throw std::runtime_error("dbn: file too short to contain metadata prelude");
	}

	std::uint32_t magic_word = ReadLE<std::uint32_t>(prelude.data());
	if (magic_word == kZstdMagicNumber) {
		throw std::runtime_error("dbn: Zstd-compressed .dbn.zst is not yet supported (Phase 3-B)");
	}

	if (std::memcmp(prelude.data(), kDbnPrefix, 3) != 0) {
		throw std::runtime_error("dbn: missing DBN prefix in file header");
	}
	metadata_.version = prelude[3];

	const std::uint32_t frame_size = ReadLE<std::uint32_t>(prelude.data() + kMagicSize);
	if (frame_size < kFixedMetadataLen) {
		throw std::runtime_error("dbn: metadata frame shorter than fixed-metadata length");
	}

	std::vector<std::uint8_t> meta_buf(frame_size);
	file_.read(reinterpret_cast<char *>(meta_buf.data()), static_cast<std::streamsize>(frame_size));
	if (file_.gcount() != static_cast<std::streamsize>(frame_size)) {
		throw std::runtime_error("dbn: file too short to contain advertised metadata frame");
	}

	const auto *ds_ptr = reinterpret_cast<const char *>(meta_buf.data());
	const auto ds_len = ::strnlen(ds_ptr, kDatasetCstrLen);
	metadata_.dataset.assign(ds_ptr, ds_len);

	const auto raw_schema = ReadLE<std::uint16_t>(meta_buf.data() + kOffSchema);
	if (raw_schema != kNullSchema) {
		metadata_.schema = static_cast<databento::Schema>(raw_schema);
	}

	metadata_.start_ns = static_cast<std::int64_t>(ReadLE<std::uint64_t>(meta_buf.data() + kOffStart));
	metadata_.end_ns = static_cast<std::int64_t>(ReadLE<std::uint64_t>(meta_buf.data() + kOffEnd));
	metadata_.limit = ReadLE<std::uint64_t>(meta_buf.data() + kOffLimit);

	const std::size_t stype_in_off = (metadata_.version == 1) ? kOffStypeInV1 : kOffStypeInV2;
	const std::uint8_t raw_stype_in = meta_buf[stype_in_off];
	if (raw_stype_in != kNullSType) {
		metadata_.stype_in = static_cast<databento::SType>(raw_stype_in);
	}
	metadata_.stype_out = static_cast<databento::SType>(meta_buf[stype_in_off + 1]);
	metadata_.ts_out = (meta_buf[stype_in_off + 2] != 0);

	if (metadata_.version > 1) {
		metadata_.symbol_cstr_len = ReadLE<std::uint16_t>(meta_buf.data() + stype_in_off + 3);
	} else {
		metadata_.symbol_cstr_len = kSymbolCstrLenV1;
	}
}

bool DbnFileReader::NextRecordRaw(std::byte *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
                                  std::uint64_t *ts_out_out) {
	file_.read(reinterpret_cast<char *>(buf), sizeof(databento::RecordHeader));
	const auto got = file_.gcount();
	if (got == 0) {
		return false;
	}
	if (got != static_cast<std::streamsize>(sizeof(databento::RecordHeader))) {
		throw std::runtime_error("dbn: truncated record header at end of file");
	}

	std::memcpy(hdr, buf, sizeof(databento::RecordHeader));

	const std::size_t record_bytes = static_cast<std::size_t>(hdr->length) * kRecordHeaderLengthMultiplier;
	if (record_bytes < sizeof(databento::RecordHeader)) {
		throw std::runtime_error("dbn: record length shorter than header");
	}
	if (record_bytes > kMaxRecordLen) {
		throw std::runtime_error("dbn: record exceeds reader buffer (" + std::to_string(record_bytes) + " bytes)");
	}

	const std::size_t remaining = record_bytes - sizeof(databento::RecordHeader);
	if (remaining > 0) {
		file_.read(reinterpret_cast<char *>(buf) + sizeof(databento::RecordHeader),
		           static_cast<std::streamsize>(remaining));
		if (file_.gcount() != static_cast<std::streamsize>(remaining)) {
			throw std::runtime_error("dbn: truncated record body");
		}
	}

	if (record_len_bytes) {
		*record_len_bytes = record_bytes;
	}

	if (metadata_.ts_out) {
		std::uint64_t trailer = 0;
		file_.read(reinterpret_cast<char *>(&trailer), sizeof(trailer));
		if (file_.gcount() != static_cast<std::streamsize>(sizeof(trailer))) {
			throw std::runtime_error("dbn: truncated ts_out trailer");
		}
		if (ts_out_out) {
			*ts_out_out = trailer;
		}
	}

	return true;
}

} // namespace duckdb_dbn
