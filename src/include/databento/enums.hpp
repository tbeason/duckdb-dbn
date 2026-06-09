// Vendored from databento-cpp v0.59.0 (https://github.com/databento/databento-cpp),
// Apache License 2.0 — see LICENSE in this directory. Trimmed to the enums the
// dbn extension consumes: API-client enums, ToString/FromString declarations and
// iostream operators are removed. The enumerator values are the DBN wire format
// and must not be changed.
#pragma once

#include <cstdint>

namespace databento {

// A record type sentinel.
namespace r_type {
enum RType : std::uint8_t {
	Mbp0 = 0x00,
	Mbp1 = 0x01,
	// Denotes a market-by-price record with a book depth of 10.
	Mbp10 = 0x0A,
	// Denotes an open, high, low, close, and volume record at an unspecified cadence.
	OhlcvDeprecated = 0x11,
	// Denotes an open, high, low, close, and volume record at a 1-second cadence.
	Ohlcv1S = 0x20,
	// Denotes an open, high, low, close, and volume record at a 1-minute cadence.
	Ohlcv1M = 0x21,
	// Denotes an open, high, low, close, and volume record at an hourly cadence.
	Ohlcv1H = 0x22,
	// Denotes an open, high, low, close, and volume record at a daily cadence
	// based on the UTC date.
	Ohlcv1D = 0x23,
	// Denotes an open, high, low, close, and volume record at a daily cadence
	// based on the end of the trading session.
	OhlcvEod = 0x24,
	// Denotes an exchange status record.
	Status = 0x12,
	// Denotes an instrument definition record.
	InstrumentDef = 0x13,
	// Denotes an order imbalance record.
	Imbalance = 0x14,
	// Denotes an error from gateway.
	Error = 0x15,
	// Denotes a symbol mapping record.
	SymbolMapping = 0x16,
	// Denotes a non-error message from the gateway. Also used for heartbeats.
	System = 0x17,
	// Denotes a statistics record from the publisher (not calculated by Databento).
	Statistics = 0x18,
	// Denotes a market-by-order record.
	Mbo = 0xA0,
	// Denotes a consolidated best bid and offer record.
	Cmbp1 = 0xB1,
	// Denotes a consolidated best bid and offer record subsampled on a one-second
	// interval.
	Cbbo1S = 0xC0,
	// Denotes a consolidated best bid and offer record subsampled on a one-minute
	// interval.
	Cbbo1M = 0xC1,
	// Denotes a consolidated best bid and offer trade record containing the
	// consolidated BBO before the trade.
	Tcbbo = 0xC2,
	// Denotes a best bid and offer record subsampled on a one-second interval.
	Bbo1S = 0xC3,
	// Denotes a best bid and offer record subsampled on a one-minute interval.
	Bbo1M = 0xC4,
};
} // namespace r_type
using r_type::RType;

// The side of the market for resting orders, or the side of the aggressor for trades.
namespace side {
enum Side : char {
	// A sell order or sell aggressor in a trade.
	Ask = 'A',
	// A buy order or a buy aggressor in a trade.
	Bid = 'B',
	// No side specified by the original source.
	None = 'N',
};
} // namespace side
using side::Side;

// An order event or order book operation.
namespace action {
enum Action : char {
	// An existing order was modified: price and/or size.
	Modify = 'M',
	// An aggressing order traded. Does not affect the book.
	Trade = 'T',
	// An existing order was filled. Does not affect the book.
	Fill = 'F',
	// An order was fully or partially cancelled.
	Cancel = 'C',
	// A new order was added to the book.
	Add = 'A',
	// Reset the book; clear all orders for an instrument.
	Clear = 'R',
	// Has no effect on the book, but may carry `flags` or other information.
	None = 'N',
};
} // namespace action
using action::Action;

// The class of instrument.
namespace instrument_class {
enum InstrumentClass : char {
	Bond = 'B',
	Call = 'C',
	Future = 'F',
	Index = 'I',
	Stock = 'K',
	MixedSpread = 'M',
	Put = 'P',
	FutureSpread = 'S',
	OptionSpread = 'T',
	FxSpot = 'X',
	CommoditySpot = 'Y',
};
} // namespace instrument_class
using instrument_class::InstrumentClass;

// The type of matching algorithm used for the instrument at the exchange.
namespace match_algorithm {
enum MatchAlgorithm : char {
	Undefined = ' ',
	Fifo = 'F',
	Configurable = 'K',
	ProRata = 'C',
	FifoLmm = 'T',
	ThresholdProRata = 'O',
	FifoTopLmm = 'S',
	ThresholdProRataLmm = 'Q',
	EurodollarFutures = 'Y',
	TimeProRata = 'P',
	InstitutionalPrioritization = 'V',
};
} // namespace match_algorithm
using match_algorithm::MatchAlgorithm;

// Whether the instrument is user-defined.
namespace user_defined_instrument {
enum UserDefinedInstrument : char {
	No = 'N',
	Yes = 'Y',
};
} // namespace user_defined_instrument
using user_defined_instrument::UserDefinedInstrument;

// The type of `InstrumentDefMsg` update.
namespace security_update_action {
enum SecurityUpdateAction : char {
	Add = 'A',
	Modify = 'M',
	Delete = 'D',
};
} // namespace security_update_action
using security_update_action::SecurityUpdateAction;

// A symbology type.
namespace s_type {
enum SType : std::uint8_t {
	// Symbology using a unique numeric ID.
	InstrumentId = 0,
	// Symbology using the original symbols provided by the publisher.
	RawSymbol = 1,
	// A set of Databento-specific symbologies for referring to groups of symbols.
	Smart = 2,
	// A Databento-specific symbology where one symbol may point to different
	// instruments at different points of time, e.g. to always refer to the front
	// month future.
	Continuous = 3,
	// A Databento-specific symbology for referring to a group of symbols by one
	// "parent" symbol, e.g. ES.FUT to refer to all ES futures.
	Parent = 4,
	// Symbology for US equities using NASDAQ Integrated suffix conventions.
	NasdaqSymbol = 5,
	// Symbology for US equities using CMS suffix conventions.
	CmsSymbol = 6,
	// Symbology using International Security Identification Numbers (ISIN).
	Isin = 7,
	// Symbology using US domestic CUSIP codes.
	UsCode = 8,
	// Symbology using Bloomberg composite global IDs.
	BbgCompId = 9,
	// Symbology using Bloomberg composite tickers.
	BbgCompTicker = 10,
	// Symbology using Bloomberg FIGI exchange level IDs.
	Figi = 11,
	// Symbology using Bloomberg exchange level tickers.
	FigiTicker = 12,
};
} // namespace s_type
using s_type::SType;

// A data record schema.
enum class Schema : std::uint16_t {
	Mbo = 0,
	Mbp1 = 1,
	Mbp10 = 2,
	Tbbo = 3,
	Trades = 4,
	Ohlcv1S = 5,
	Ohlcv1M = 6,
	Ohlcv1H = 7,
	Ohlcv1D = 8,
	Definition = 9,
	Statistics = 10,
	Status = 11,
	Imbalance = 12,
	OhlcvEod = 13,
	Cmbp1 = 14,
	Cbbo1S = 15,
	Cbbo1M = 16,
	Tcbbo = 17,
	Bbo1S = 18,
	Bbo1M = 19,
};

// The type of statistic contained in a `StatMsg`.
namespace stat_type {
enum StatType : std::uint16_t {
	OpeningPrice = 1,
	IndicativeOpeningPrice = 2,
	SettlementPrice = 3,
	TradingSessionLowPrice = 4,
	TradingSessionHighPrice = 5,
	ClearedVolume = 6,
	LowestOffer = 7,
	HighestBid = 8,
	OpenInterest = 9,
	FixingPrice = 10,
	ClosePrice = 11,
	NetChange = 12,
	Vwap = 13,
	Volatility = 14,
	Delta = 15,
	UncrossingPrice = 16,
	UpperPriceLimit = 17,
	LowerPriceLimit = 18,
	BlockVolume = 19,
	IndicativeClosePrice = 20,
	VenueSpecificVolume1 = 10001,
	VenueSpecificPrice1 = 10002,
};
} // namespace stat_type
using stat_type::StatType;

// The type of `StatMsg` update.
namespace stat_update_action {
enum StatUpdateAction : std::uint8_t {
	New = 1,
	Delete = 2,
};
} // namespace stat_update_action
using stat_update_action::StatUpdateAction;

// The primary enum for the type of `StatusMsg` update.
namespace status_action {
enum StatusAction : std::uint16_t {
	None = 0,
	PreOpen = 1,
	PreCross = 2,
	Quoting = 3,
	Cross = 4,
	Rotation = 5,
	NewPriceIndication = 6,
	Trading = 7,
	Halt = 8,
	Pause = 9,
	Suspend = 10,
	PreClose = 11,
	Close = 12,
	PostClose = 13,
	SsrChange = 14,
	NotAvailableForTrading = 15,
};
} // namespace status_action
using status_action::StatusAction;

// The secondary enum for a `StatusMsg` update, explains the cause of a halt or
// other change in `action`.
namespace status_reason {
enum StatusReason : std::uint16_t {
	None = 0,
	Scheduled = 1,
	SurveillanceIntervention = 2,
	MarketEvent = 3,
	InstrumentActivation = 4,
	InstrumentExpiration = 5,
	RecoveryInProcess = 6,
	Regulatory = 10,
	Administrative = 11,
	NonCompliance = 12,
	FilingsNotCurrent = 13,
	SecTradingSuspension = 14,
	NewIssue = 15,
	IssueAvailable = 16,
	IssuesReviewed = 17,
	FilingReqsSatisfied = 18,
	NewsPending = 30,
	NewsReleased = 31,
	NewsAndResumptionTimes = 32,
	NewsNotForthcoming = 33,
	OrderImbalance = 40,
	LuldPause = 50,
	Operational = 60,
	AdditionalInformationRequested = 70,
	MergerEffective = 80,
	Etf = 90,
	CorporateAction = 100,
	NewSecurityOffering = 110,
	MarketWideHaltLevel1 = 120,
	MarketWideHaltLevel2 = 121,
	MarketWideHaltLevel3 = 122,
	MarketWideHaltCarryover = 123,
	MarketWideHaltResumption = 124,
	QuotationNotAvailable = 130,
};
} // namespace status_reason
using status_reason::StatusReason;

// Further information about a status update.
namespace trading_event {
enum TradingEvent : std::uint16_t {
	None = 0,
	NoCancel = 1,
	ChangeTradingSession = 2,
	ImpliedMatchingOn = 3,
	ImpliedMatchingOff = 4,
};
} // namespace trading_event
using trading_event::TradingEvent;

// An enum for representing unknown, true, or false values.
namespace tri_state {
enum TriState : char {
	// The value is not applicable or not known.
	NotAvailable = '~',
	// False.
	No = 'N',
	// True.
	Yes = 'Y',
};
} // namespace tri_state
using tri_state::TriState;

// A `SystemMsg` code indicating the type of message from the live subscription
// gateway.
namespace system_code {
enum SystemCode : std::uint8_t {
	// A message sent in the absence of other records to indicate the connection
	// remains open.
	Heartbeat = 0,
	// An acknowledgement of a subscription request.
	SubscriptionAck = 1,
	// The gateway has detected this session is falling behind real-time.
	SlowReaderWarning = 2,
	// Indicates a replay subscription has caught up with real-time data.
	ReplayCompleted = 3,
	// Signals that all records for interval-based schemas have been published for
	// the given timestamp.
	EndOfInterval = 4,
	// No system code was specified or this record was upgraded from a version 1
	// struct where the code field didn't exist.
	Unset = 255,
};
} // namespace system_code
using system_code::SystemCode;

} // namespace databento
