#include "dbn_native_decoder.hpp"

#include <array>
#include <cstring>
#include <stdexcept>

#include "databento/record.hpp"

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

} // namespace

DbnFileReader::DbnFileReader(const std::string &path)
    : file_(path, std::ios::binary) {
	if (!file_) {
		throw std::runtime_error("dbn: failed to open file: " + path);
	}

	// Read the 8-byte prelude: "DBN" + version byte + frame_size (u32 LE).
	std::array<std::uint8_t, kMetadataPreludeSize> prelude{};
	file_.read(reinterpret_cast<char *>(prelude.data()), prelude.size());
	if (file_.gcount() != static_cast<std::streamsize>(prelude.size())) {
		throw std::runtime_error("dbn: file too short to contain metadata prelude");
	}

	// Detect Zstd-framed input (we don't decompress in Phase 1).
	std::uint32_t magic_word = 0;
	std::memcpy(&magic_word, prelude.data(), sizeof(magic_word));
	if (magic_word == kZstdMagicNumber) {
		throw std::runtime_error(
		    "dbn: Zstd-compressed .dbn.zst is not yet supported (Phase 3)");
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

	// Skip the entire metadata block. Phase 1 doesn't surface metadata fields.
	file_.seekg(static_cast<std::streamoff>(frame_size), std::ios::cur);
	if (!file_) {
		throw std::runtime_error("dbn: failed to seek past metadata block");
	}
}

bool DbnFileReader::NextTrade(databento::TradeMsg &out) {
	for (;;) {
		// Peek at the RecordHeader to learn the record's size and rtype.
		databento::RecordHeader header{};
		file_.read(reinterpret_cast<char *>(&header), sizeof(header));
		const auto got = file_.gcount();
		if (got == 0) {
			return false; // clean EOF
		}
		if (got != static_cast<std::streamsize>(sizeof(header))) {
			throw std::runtime_error("dbn: truncated record header at end of file");
		}

		const std::size_t record_bytes =
		    static_cast<std::size_t>(header.length) * kRecordHeaderLengthMultiplier;
		if (record_bytes < sizeof(databento::RecordHeader)) {
			throw std::runtime_error("dbn: record length shorter than header");
		}
		const std::size_t remaining = record_bytes - sizeof(databento::RecordHeader);

		if (header.rtype == databento::RType::Mbp0) {
			if (record_bytes != sizeof(databento::TradeMsg)) {
				throw std::runtime_error(
				    "dbn: trades record size mismatch (expected " +
				    std::to_string(sizeof(databento::TradeMsg)) + ", got " +
				    std::to_string(record_bytes) + ")");
			}
			out.hd = header;
			char *dst = reinterpret_cast<char *>(&out) + sizeof(databento::RecordHeader);
			file_.read(dst, static_cast<std::streamsize>(remaining));
			if (file_.gcount() != static_cast<std::streamsize>(remaining)) {
				throw std::runtime_error("dbn: truncated trade record body");
			}
			return true;
		}

		// Skip the body of any non-trade record and try again.
		if (remaining > 0) {
			file_.seekg(static_cast<std::streamoff>(remaining), std::ios::cur);
			if (!file_) {
				throw std::runtime_error("dbn: failed to skip non-trade record body");
			}
		}
	}
}

} // namespace duckdb_dbn
