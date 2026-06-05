#include "dbn_native_decoder.hpp"

#include <array>
#include <cstring>
#include <stdexcept>

namespace duckdb_dbn {

namespace {

// Mirrors databento-cpp/src/dbn_constants.hpp. Duplicated here so we don't
// have to pull in their internal src/ tree on the include path.
constexpr std::size_t kMagicSize = 4;
constexpr std::size_t kMetadataPreludeSize = 8;
constexpr std::uint32_t kZstdMagicNumber = 0xFD2FB528;
constexpr char kDbnPrefix[] = "DBN";
constexpr std::size_t kFixedMetadataLen = 100;
constexpr std::size_t kRecordHeaderLengthMultiplier = 4;

// Within the variable-length metadata block (after the 8-byte prelude), the
// fixed prefix layout per databento-cpp/src/dbn_decoder.cpp DecodeMetadataFields:
//   offset 0..15  dataset cstr (kDatasetCstrLen = 16)
//   offset 16..17 schema u16
//   offset 18..25 start  u64
//   offset 26..33 end    u64
//   offset 34..41 limit  u64
//   v1 ONLY: 42..49 deprecated record_count u64
//   stype_in u8
//   stype_out u8
//   ts_out u8  <-- the only field we read in Phase 2
//
// So ts_out lives at offset 44 in v2/v3 metadata blocks, offset 52 in v1.
constexpr std::size_t kTsOutOffsetV2 = 44;
constexpr std::size_t kTsOutOffsetV1 = 52;

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

	std::uint32_t magic_word = 0;
	std::memcpy(&magic_word, prelude.data(), sizeof(magic_word));
	if (magic_word == kZstdMagicNumber) {
		throw std::runtime_error("dbn: Zstd-compressed .dbn.zst is not yet supported (Phase 3)");
	}

	if (std::memcmp(prelude.data(), kDbnPrefix, 3) != 0) {
		throw std::runtime_error("dbn: missing DBN prefix in file header");
	}
	version_ = prelude[3];

	std::uint32_t frame_size = 0;
	std::memcpy(&frame_size, prelude.data() + kMagicSize, sizeof(frame_size));
	if (frame_size < kFixedMetadataLen) {
		throw std::runtime_error("dbn: metadata frame shorter than fixed-metadata length");
	}

	// Peek at the ts_out byte. Without this, ts_out=true streams misalign the
	// record loop because each record is followed by 8 trailer bytes.
	const std::size_t ts_out_offset = (version_ == 1) ? kTsOutOffsetV1 : kTsOutOffsetV2;
	if (ts_out_offset + 1 > frame_size) {
		throw std::runtime_error("dbn: metadata frame too small to contain ts_out flag");
	}
	const auto metadata_start = file_.tellg();
	file_.seekg(metadata_start + static_cast<std::streamoff>(ts_out_offset));
	std::uint8_t ts_out_byte = 0;
	file_.read(reinterpret_cast<char *>(&ts_out_byte), 1);
	if (file_.gcount() != 1) {
		throw std::runtime_error("dbn: failed to read ts_out flag from metadata");
	}
	ts_out_ = (ts_out_byte != 0);

	file_.seekg(metadata_start + static_cast<std::streamoff>(frame_size));
	if (!file_) {
		throw std::runtime_error("dbn: failed to seek past metadata block");
	}
}

bool DbnFileReader::NextRecordRaw(std::byte *buf, databento::RecordHeader *hdr, std::size_t *record_len_bytes,
                                  std::uint64_t *ts_out_out) {
	file_.read(reinterpret_cast<char *>(buf), sizeof(databento::RecordHeader));
	const auto got = file_.gcount();
	if (got == 0) {
		return false; // clean EOF
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

	if (ts_out_) {
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
