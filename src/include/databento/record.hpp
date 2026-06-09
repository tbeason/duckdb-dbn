// Vendored from databento-cpp v0.59.0 (https://github.com/databento/databento-cpp),
// Apache License 2.0 — see LICENSE in this directory. Trimmed to the current
// (DBN v3) record-struct layouts the dbn extension memcpys raw file bytes into,
// with datetime.hpp, flag_set.hpp, and constants.hpp folded in. Member functions,
// ToString/iostream support, and comparison operators are removed; std::byte
// reserved fields are std::uint8_t so the header compiles as C++14. The field
// order, types, and the static_asserts on size/alignment are the wire format
// and must not be changed.
#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ratio>

#include "databento/enums.hpp"

namespace databento {

// Nanoseconds since the UNIX epoch.
using UnixNanos = std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<uint64_t, std::nano>>;
// A representation of the difference between two timestamps.
using TimeDeltaNanos = std::chrono::duration<int32_t, std::nano>;

// The decimal scaler of fixed prices.
static constexpr std::int64_t kFixedPriceScale = 1000000000;
// The sentinel value for an unset or null price.
static constexpr std::int64_t kUndefPrice = std::numeric_limits<std::int64_t>::max();
// The sentinel value for an unset or null order size.
static constexpr std::uint32_t kUndefOrderSize = std::numeric_limits<std::uint32_t>::max();
// The sentinel value for an unset statistic quantity.
static constexpr std::int64_t kUndefStatQuantity = std::numeric_limits<std::int64_t>::max();
// The sentinel value for an unset or null timestamp.
static constexpr std::uint64_t kUndefTimestamp = std::numeric_limits<std::uint64_t>::max();
// The length of fixed-length symbol strings.
static constexpr std::size_t kSymbolCstrLen = 71;
// The length of fixed-length asset strings.
static constexpr std::size_t kAssetCstrLen = 11;
// The multiplier for converting the `length` field in `RecordHeader` to bytes.
static constexpr std::size_t kRecordHeaderLengthMultiplier = 4;

// Transparent wrapper around the bit flags used in several DBN record types.
class FlagSet {
public:
	using Repr = std::uint8_t;
	// Indicates it's the last message in the event from the venue for a given
	// `instrument_id`.
	static constexpr Repr kLast = 1 << 7;
	// Indicates a top-of-book message, not an individual order.
	static constexpr Repr kTob = 1 << 6;
	// Indicates the message was sourced from a replay, such as a snapshot server.
	static constexpr Repr kSnapshot = 1 << 5;
	// Indicates an aggregated price level message, not an individual order.
	static constexpr Repr kMbp = 1 << 4;
	// Indicates the `ts_recv` value is inaccurate due to clock issues or packet
	// reordering.
	static constexpr Repr kBadTsRecv = 1 << 3;
	// Indicates an unrecoverable gap was detected in the channel.
	static constexpr Repr kMaybeBadBook = 1 << 2;
	// Indicates a publisher-specific event.
	static constexpr Repr kPublisherSpecific = 1 << 1;

	constexpr FlagSet() : repr_ {0} {
	}
	explicit constexpr FlagSet(Repr repr) : repr_ {repr} {
	}
	constexpr Repr Raw() const {
		return repr_;
	}

private:
	Repr repr_;
};
static_assert(sizeof(FlagSet) == sizeof(std::uint8_t), "FlagSet must be a transparent wrapper around std::uint8_t");

// Common data for all Databento records.
struct RecordHeader {
	// The length of the message in 32-bit words.
	std::uint8_t length;
	// The record type.
	RType rtype;
	// The publisher ID assigned by Databento, which denotes the dataset and venue.
	std::uint16_t publisher_id;
	// The numeric ID assigned to the instrument.
	std::uint32_t instrument_id;
	// The exchange timestamp in UNIX epoch nanoseconds.
	UnixNanos ts_event;
};

// A market-by-order (MBO) tick message. The record of the MBO schema.
struct MboMsg {
	RecordHeader hd;
	std::uint64_t order_id;
	std::int64_t price;
	std::uint32_t size;
	FlagSet flags;
	std::uint8_t channel_id;
	Action action;
	Side side;
	UnixNanos ts_recv;
	TimeDeltaNanos ts_in_delta;
	std::uint32_t sequence;
};
static_assert(sizeof(MboMsg) == 56, "MboMsg size must match Rust");
static_assert(alignof(MboMsg) == 8, "MboMsg must have 8-byte alignment");

// A price level.
struct BidAskPair {
	std::int64_t bid_px;
	std::int64_t ask_px;
	std::uint32_t bid_sz;
	std::uint32_t ask_sz;
	std::uint32_t bid_ct;
	std::uint32_t ask_ct;
};
static_assert(sizeof(BidAskPair) == 32, "BidAskPair size must match Rust");
static_assert(alignof(BidAskPair) == 8, "BidAskPair must have 8-byte alignment");

// A price level consolidated from multiple venues.
struct ConsolidatedBidAskPair {
	std::int64_t bid_px;
	std::int64_t ask_px;
	std::uint32_t bid_sz;
	std::uint32_t ask_sz;
	std::uint16_t bid_pb;
	std::array<std::uint8_t, 2> _reserved1 {};
	std::uint16_t ask_pb;
	std::array<std::uint8_t, 2> _reserved2 {};
};
static_assert(sizeof(ConsolidatedBidAskPair) == 32, "ConsolidatedBidAskPair size must match Rust");
static_assert(alignof(ConsolidatedBidAskPair) == 8, "ConsolidatedBidAskPair must have 8-byte alignment");

// Market-by-price implementation with a book depth of 0. Equivalent to MBP-0.
// The record of the Trades schema.
struct TradeMsg {
	RecordHeader hd;
	std::int64_t price;
	std::uint32_t size;
	Action action;
	Side side;
	FlagSet flags;
	std::uint8_t depth;
	UnixNanos ts_recv;
	TimeDeltaNanos ts_in_delta;
	std::uint32_t sequence;
};
static_assert(sizeof(TradeMsg) == 48, "TradeMsg size must match Rust");
static_assert(alignof(TradeMsg) == 8, "TradeMsg must have 8-byte alignment");

// Market-by-price implementation with a known book depth of 1. The record of
// the MBP-1 schema.
struct Mbp1Msg {
	RecordHeader hd;
	std::int64_t price;
	std::uint32_t size;
	Action action;
	Side side;
	FlagSet flags;
	std::uint8_t depth;
	UnixNanos ts_recv;
	TimeDeltaNanos ts_in_delta;
	std::uint32_t sequence;
	std::array<BidAskPair, 1> levels;
};
static_assert(sizeof(Mbp1Msg) == 80, "Mbp1Msg size must match Rust");
static_assert(alignof(Mbp1Msg) == 8, "Mbp1Msg must have 8-byte alignment");

// Market-by-price implementation with a known book depth of 10. The record of
// the MBP-10 schema.
struct Mbp10Msg {
	RecordHeader hd;
	std::int64_t price;
	std::uint32_t size;
	Action action;
	Side side;
	FlagSet flags;
	std::uint8_t depth;
	UnixNanos ts_recv;
	TimeDeltaNanos ts_in_delta;
	std::uint32_t sequence;
	std::array<BidAskPair, 10> levels;
};
static_assert(sizeof(Mbp10Msg) == 368, "Mbp10Msg size must match Rust");
static_assert(alignof(Mbp10Msg) == 8, "Mbp10Msg must have 8-byte alignment");

// Subsampled market by price with a known book depth of 1. The record of the
// BBO-1s and BBO-1m schemas.
struct BboMsg {
	RecordHeader hd;
	std::int64_t price;
	std::uint32_t size;
	std::uint8_t _reserved1 {};
	Side side;
	FlagSet flags;
	std::uint8_t _reserved2 {};
	UnixNanos ts_recv;
	std::array<std::uint8_t, 4> _reserved3 {};
	std::uint32_t sequence;
	std::array<BidAskPair, 1> levels;
};
static_assert(sizeof(BboMsg) == 80, "BboMsg size must match Rust");
static_assert(alignof(BboMsg) == 8, "BboMsg must have 8-byte alignment");

// Consolidated market-by-price implementation with a known book depth of 1.
// The record of the CMBP-1 schema.
struct Cmbp1Msg {
	RecordHeader hd;
	std::int64_t price;
	std::uint32_t size;
	Action action;
	Side side;
	FlagSet flags;
	std::array<std::uint8_t, 1> _reserved1 {};
	UnixNanos ts_recv;
	TimeDeltaNanos ts_in_delta;
	std::array<std::uint8_t, 4> _reserved2 {};
	std::array<ConsolidatedBidAskPair, 1> levels;
};
static_assert(sizeof(Cmbp1Msg) == 80, "Cmbp1Msg size must match Rust");
static_assert(alignof(Cmbp1Msg) == 8, "Cmbp1Msg must have 8-byte alignment");

// Subsampled consolidated market by price with a known book depth of 1. The
// record of the CBBO-1s and CBBO-1m schemas.
struct CbboMsg {
	RecordHeader hd;
	std::int64_t price;
	std::uint32_t size;
	std::uint8_t _reserved1 {};
	Side side;
	FlagSet flags;
	std::uint8_t _reserved2 {};
	UnixNanos ts_recv;
	std::array<std::uint8_t, 8> _reserved3 {};
	std::array<ConsolidatedBidAskPair, 1> levels;
};
static_assert(sizeof(CbboMsg) == 80, "CbboMsg size must match Rust");
static_assert(alignof(CbboMsg) == 8, "CbboMsg must have 8-byte alignment");

// The record of the TBBO schema.
using TbboMsg = Mbp1Msg;
// The record of the TCBBO schema.
using TcbboMsg = Cmbp1Msg;

// Open, high, low, close, and volume. The record of the OHLCV-1s/1m/1h/1d/eod
// schemas.
struct OhlcvMsg {
	RecordHeader hd;
	std::int64_t open;
	std::int64_t high;
	std::int64_t low;
	std::int64_t close;
	std::uint64_t volume;
};
static_assert(sizeof(OhlcvMsg) == 56, "OhlcvMsg size must match Rust");
static_assert(alignof(OhlcvMsg) == 8, "OhlcvMsg must have 8-byte alignment");

// A trading status update message. The record of the status schema.
struct StatusMsg {
	RecordHeader hd;
	UnixNanos ts_recv;
	StatusAction action;
	StatusReason reason;
	TradingEvent trading_event;
	TriState is_trading;
	TriState is_quoting;
	TriState is_short_sell_restricted;
	std::array<std::uint8_t, 7> _reserved {};
};
static_assert(sizeof(StatusMsg) == 40, "StatusMsg size must match Rust");
static_assert(alignof(StatusMsg) == 8, "StatusMsg must have 8-byte alignment");

// A definition of an instrument. The record of the definition schema.
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
	const char *LegRawSymbol() const {
		return leg_raw_symbol.data();
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
	std::int64_t unit_of_measure_qty;
	std::int64_t min_price_increment_amount;
	std::int64_t price_ratio;
	std::int64_t strike_price;
	std::uint64_t raw_instrument_id;
	std::int64_t leg_price;
	std::int64_t leg_delta;
	std::int32_t inst_attrib_value;
	std::uint32_t underlying_id;
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
	std::uint32_t leg_instrument_id;
	std::int32_t leg_ratio_price_numerator;
	std::int32_t leg_ratio_price_denominator;
	std::int32_t leg_ratio_qty_numerator;
	std::int32_t leg_ratio_qty_denominator;
	std::uint32_t leg_underlying_id;
	std::int16_t appl_id;
	std::uint16_t maturity_year;
	std::uint16_t decay_start_date;
	std::uint16_t channel_id;
	std::uint16_t leg_count;
	std::uint16_t leg_index;
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
	std::array<char, kSymbolCstrLen> leg_raw_symbol;
	InstrumentClass instrument_class;
	MatchAlgorithm match_algorithm;
	std::uint8_t main_fraction;
	std::uint8_t price_display_format;
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
	InstrumentClass leg_instrument_class;
	Side leg_side;
	std::array<std::uint8_t, 17> _reserved {};
};
static_assert(sizeof(InstrumentDefMsg) == 520, "InstrumentDefMsg size must match Rust");
static_assert(alignof(InstrumentDefMsg) == 8, "InstrumentDefMsg must have 8-byte alignment");

// An auction imbalance message.
struct ImbalanceMsg {
	RecordHeader hd;
	UnixNanos ts_recv;
	std::int64_t ref_price;
	UnixNanos auction_time;
	std::int64_t cont_book_clr_price;
	std::int64_t auct_interest_clr_price;
	std::int64_t ssr_filling_price;
	std::int64_t ind_match_price;
	std::int64_t upper_collar;
	std::int64_t lower_collar;
	std::uint32_t paired_qty;
	std::uint32_t total_imbalance_qty;
	std::uint32_t market_imbalance_qty;
	std::uint32_t unpaired_qty;
	char auction_type;
	Side side;
	std::uint8_t auction_status;
	std::uint8_t freeze_status;
	std::uint8_t num_extensions;
	Side unpaired_side;
	char significant_imbalance;
	std::array<std::uint8_t, 1> _reserved {};
};
static_assert(sizeof(ImbalanceMsg) == 112, "ImbalanceMsg size must match Rust");
static_assert(alignof(ImbalanceMsg) == 8, "ImbalanceMsg must have 8-byte alignment");

// A statistics message. A catchall for various data disseminated by publishers.
// The `stat_type` indicates the statistic contained in the message.
struct StatMsg {
	RecordHeader hd;
	UnixNanos ts_recv;
	UnixNanos ts_ref;
	std::int64_t price;
	std::int64_t quantity;
	std::uint32_t sequence;
	TimeDeltaNanos ts_in_delta;
	StatType stat_type;
	std::uint16_t channel_id;
	StatUpdateAction update_action;
	std::uint8_t stat_flags;
	std::array<std::uint8_t, 18> _reserved {};
};
static_assert(sizeof(StatMsg) == 80, "StatMsg size must match Rust");
static_assert(alignof(StatMsg) == 8, "StatMsg must have 8-byte alignment");

// A symbol mapping message from the live API which maps a symbol from one
// `SType` to another.
struct SymbolMappingMsg {
	RecordHeader hd;
	SType stype_in;
	std::array<char, kSymbolCstrLen> stype_in_symbol;
	SType stype_out;
	std::array<char, kSymbolCstrLen> stype_out_symbol;
	UnixNanos start_ts;
	UnixNanos end_ts;
};
static_assert(sizeof(SymbolMappingMsg) == 176, "SymbolMappingMsg size must match Rust");
static_assert(alignof(SymbolMappingMsg) == 8, "SymbolMappingMsg must have 8-byte alignment");

// A non-error message from the Databento Live Subscription Gateway (LSG). Also
// used for heartbeating.
struct SystemMsg {
	RecordHeader hd;
	std::array<char, 303> msg;
	SystemCode code;
};
static_assert(sizeof(SystemMsg) == 320, "SystemMsg size must match Rust");
static_assert(alignof(SystemMsg) == 8, "SystemMsg must have 8-byte alignment");

} // namespace databento
