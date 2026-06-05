#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "databento/record.hpp"

namespace duckdb_dbn {

// Minimal Phase-1 DBN file reader. Reuses databento-cpp's wire-compatible
// record struct definitions (TradeMsg, RecordHeader, ...) so anyone porting
// between this extension and databento-cpp sees identical byte layouts.
//
// Scope (Phase 1):
//   - Uncompressed .dbn files only. Throws on Zstd magic in the prelude.
//   - Trades schema (RType::Mbp0). NextTrade() skips any other rtype.
//   - Metadata block is validated for "DBN" prefix and frame size, then
//     skipped — we don't surface metadata fields to SQL yet.
class DbnFileReader {
public:
	explicit DbnFileReader(const std::string &path);

	// Reads the next TradeMsg record into `out`. Returns true on success,
	// false at EOF. Non-trade records are silently skipped.
	bool NextTrade(databento::TradeMsg &out);

	std::uint8_t Version() const {
		return version_;
	}

private:
	std::ifstream file_;
	std::uint8_t version_ = 0;
};

} // namespace duckdb_dbn
