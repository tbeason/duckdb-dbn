// Vendored from databento-cpp v0.59.0 (https://github.com/databento/databento-cpp),
// Apache License 2.0 — see LICENSE in this directory. Trimmed to the DBN v2
// record-struct layouts whose shape differs from the current (v3) structs in
// record.hpp. Member functions and upgrade/conversion helpers are removed;
// std::byte reserved fields are std::uint8_t. The field order, types, and the
// static_asserts on size/alignment are the wire format and must not be changed.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "databento/enums.hpp"
#include "databento/record.hpp"
#include "databento/v1.hpp"

namespace databento {
namespace v2 {

static constexpr std::uint8_t kDbnVersion = 2;
static constexpr std::size_t kSymbolCstrLen = databento::kSymbolCstrLen;
static constexpr std::size_t kAssetCstrLen = 7;
static constexpr std::int32_t kUndefStatQuantity = v1::kUndefStatQuantity;

// A statistics message in DBN version 2 shares the v1 layout.
using StatMsg = databento::v1::StatMsg;

// A definition of an instrument in DBN version 2. The record of the definition
// schema.
struct InstrumentDefMsg {
	const char *Currency() const {
		return currency.data();
	}
	const char *SettlCurrency() const {
		return settl_currency.data();
	}
	const char *SecSubType() const {
		return secsubtype.data();
	}
	const char *RawSymbol() const {
		return raw_symbol.data();
	}
	const char *Group() const {
		return group.data();
	}
	const char *Exchange() const {
		return exchange.data();
	}
	const char *Asset() const {
		return asset.data();
	}
	const char *Cfi() const {
		return cfi.data();
	}
	const char *SecurityType() const {
		return security_type.data();
	}
	const char *UnitOfMeasure() const {
		return unit_of_measure.data();
	}
	const char *Underlying() const {
		return underlying.data();
	}
	const char *StrikePriceCurrency() const {
		return strike_price_currency.data();
	}

	RecordHeader hd;
	UnixNanos ts_recv;
	std::int64_t min_price_increment;
	std::int64_t display_factor;
	UnixNanos expiration;
	UnixNanos activation;
	std::int64_t high_limit_price;
	std::int64_t low_limit_price;
	std::int64_t max_price_variation;
	std::int64_t trading_reference_price;
	std::int64_t unit_of_measure_qty;
	std::int64_t min_price_increment_amount;
	std::int64_t price_ratio;
	std::int64_t strike_price;
	std::int32_t inst_attrib_value;
	std::uint32_t underlying_id;
	std::uint32_t raw_instrument_id;
	std::int32_t market_depth_implied;
	std::int32_t market_depth;
	std::uint32_t market_segment_id;
	std::uint32_t max_trade_vol;
	std::int32_t min_lot_size;
	std::int32_t min_lot_size_block;
	std::int32_t min_lot_size_round_lot;
	std::uint32_t min_trade_vol;
	std::int32_t contract_multiplier;
	std::int32_t decay_quantity;
	std::int32_t original_contract_size;
	std::uint16_t trading_reference_date;
	std::int16_t appl_id;
	std::uint16_t maturity_year;
	std::uint16_t decay_start_date;
	std::uint16_t channel_id;
	std::array<char, 4> currency;
	std::array<char, 4> settl_currency;
	std::array<char, 6> secsubtype;
	std::array<char, kSymbolCstrLen> raw_symbol;
	std::array<char, 21> group;
	std::array<char, 5> exchange;
	std::array<char, kAssetCstrLen> asset;
	std::array<char, 7> cfi;
	std::array<char, 7> security_type;
	std::array<char, 31> unit_of_measure;
	std::array<char, 21> underlying;
	std::array<char, 4> strike_price_currency;
	InstrumentClass instrument_class;
	MatchAlgorithm match_algorithm;
	std::uint8_t md_security_trading_status;
	std::uint8_t main_fraction;
	std::uint8_t price_display_format;
	std::uint8_t settl_price_type;
	std::uint8_t sub_fraction;
	std::uint8_t underlying_product;
	SecurityUpdateAction security_update_action;
	std::uint8_t maturity_month;
	std::uint8_t maturity_day;
	std::uint8_t maturity_week;
	UserDefinedInstrument user_defined_instrument;
	std::int8_t contract_multiplier_unit;
	std::int8_t flow_schedule_type;
	std::uint8_t tick_rule;
	std::array<std::uint8_t, 10> _reserved {};
};
static_assert(sizeof(InstrumentDefMsg) == 400, "InstrumentDefMsg size must match Rust");
static_assert(alignof(InstrumentDefMsg) == 8, "InstrumentDefMsg must have 8-byte alignment");

} // namespace v2
} // namespace databento
