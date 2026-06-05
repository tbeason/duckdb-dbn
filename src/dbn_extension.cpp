#define DUCKDB_EXTENSION_MAIN

#include "dbn_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

#include <memory>
#include <utility>

#include "databento/enums.hpp"
#include "databento/record.hpp"
#include "dbn_emit_helpers.hpp"
#include "dbn_native_decoder.hpp"

namespace duckdb {

// ─────────────────────────────────────────────────────────────────────────────
// Shared bind data and global state. Every per-schema table function uses
// these — only the column list and scan loop differ per schema.
// ─────────────────────────────────────────────────────────────────────────────

struct ReadDbnBindData : public TableFunctionData {
	std::string file_path;
};

struct ReadDbnGlobalState : public GlobalTableFunctionState {
	explicit ReadDbnGlobalState(const std::string &path)
	    : reader(std::make_unique<duckdb_dbn::DbnFileReader>(path)) {}
	std::unique_ptr<duckdb_dbn::DbnFileReader> reader;
};

static unique_ptr<GlobalTableFunctionState> ReadDbnInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<ReadDbnBindData>();
	return make_uniq<ReadDbnGlobalState>(bd.file_path);
}

static std::string GetFilePathArg(TableFunctionBindInput &input) {
	return input.inputs[0].GetValue<string>();
}

// ─────────────────────────────────────────────────────────────────────────────
// Shorthand for the per-row int64 timestamp extraction.
// ─────────────────────────────────────────────────────────────────────────────

static inline int64_t TsToInt64(databento::UnixNanos t) {
	return static_cast<int64_t>(t.time_since_epoch().count());
}

// ═════════════════════════════════════════════════════════════════════════════
// trades — RType::Mbp0, TradeMsg (48 bytes)
// Mirrors Julia trades_to_dataframe ordering.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> TradesBind(ClientContext &, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event", "ts_recv", "instrument_id", "publisher_id", "price",       "size",
	         "action",   "side",    "flags",         "depth",        "ts_in_delta", "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::UTINYINT,
	                LogicalType::UTINYINT,     LogicalType::INTEGER,      LogicalType::UINTEGER};
	return std::move(bd);
}

static void TradesScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto action_v = FlatVector::GetData<string_t>(out.data[6]);
	auto side_v = FlatVector::GetData<string_t>(out.data[7]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[8]);
	auto depth = FlatVector::GetData<uint8_t>(out.data[9]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[10]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[11]);

	databento::TradeMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbp0)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		depth[n] = rec.depth;
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbo — RType::Mbo, MboMsg (56 bytes)
// Mirrors Julia mbo_to_dataframe ordering.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> MboBind(ClientContext &, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event", "ts_recv", "instrument_id", "publisher_id", "order_id",   "price",      "size",
	         "flags",    "channel_id", "action",     "side",         "ts_in_delta", "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::UBIGINT,      LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UTINYINT,     LogicalType::UTINYINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::INTEGER,
	                LogicalType::UINTEGER};
	return std::move(bd);
}

static void MboScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto order_id = FlatVector::GetData<uint64_t>(out.data[4]);
	auto price = FlatVector::GetData<double>(out.data[5]);
	auto size = FlatVector::GetData<uint32_t>(out.data[6]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[7]);
	auto chan = FlatVector::GetData<uint8_t>(out.data[8]);
	auto action_v = FlatVector::GetData<string_t>(out.data[9]);
	auto side_v = FlatVector::GetData<string_t>(out.data[10]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[11]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[12]);

	databento::MboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbo)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		order_id[n] = rec.order_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		flags[n] = rec.flags.Raw();
		chan[n] = rec.channel_id;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[9], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[10], static_cast<char>(rec.side));
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbp-1 — RType::Mbp1, Mbp1Msg (80 bytes)
// Flattens levels[0] into bid_price/ask_price/bid_size/ask_size/bid_ct/ask_ct.
// Mirrors Julia mbp1_to_dataframe (action/side last).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> Mbp1Bind(ClientContext &, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event",  "ts_recv",  "instrument_id", "publisher_id", "bid_price",
	         "ask_price", "bid_size", "ask_size",      "bid_ct",       "ask_ct",
	         "flags",     "ts_in_delta", "sequence",   "action",       "side"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::UINTEGER,     LogicalType::UTINYINT,     LogicalType::INTEGER,
	                LogicalType::UINTEGER,     LogicalType::VARCHAR,      LogicalType::VARCHAR};
	return std::move(bd);
}

static void Mbp1ScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto bid_px = FlatVector::GetData<double>(out.data[4]);
	auto ask_px = FlatVector::GetData<double>(out.data[5]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[6]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[7]);
	auto bid_ct = FlatVector::GetData<uint32_t>(out.data[8]);
	auto ask_ct = FlatVector::GetData<uint32_t>(out.data[9]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[10]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[11]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[12]);
	auto action_v = FlatVector::GetData<string_t>(out.data[13]);
	auto side_v = FlatVector::GetData<string_t>(out.data[14]);

	databento::Mbp1Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_ct[n] = L.bid_ct;
		ask_ct[n] = L.ask_ct;
		flags[n] = rec.flags.Raw();
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[13], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[14], static_cast<char>(rec.side));
		++n;
	}
	out.SetCardinality(n);
}

static void Mbp1Scan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Mbp1ScanImpl(c, input, out, databento::RType::Mbp1);
}

// tbbo shares Mbp1Msg layout + RType::Mbp1; the only on-wire difference is
// metadata.schema, which Phase 2 does not parse. read_dbn_tbbo and
// read_dbn_mbp1 therefore return identical data on either fixture; Phase 3
// will gate them via metadata.schema once we parse it.
static void TbboScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Mbp1ScanImpl(c, input, out, databento::RType::Mbp1);
}

static unique_ptr<FunctionData> TbboBind(ClientContext &c, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	return Mbp1Bind(c, input, return_types, names);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbp-10 — RType::Mbp10, Mbp10Msg (368 bytes)
// 12 base columns + 10 levels × 6 fields flattened.
// ═════════════════════════════════════════════════════════════════════════════

static std::string LevelCol(const char *prefix, int i) {
	char buf[16];
	std::snprintf(buf, sizeof(buf), "%s_%02d", prefix, i);
	return std::string(buf);
}

static unique_ptr<FunctionData> Mbp10Bind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event",    "ts_recv", "instrument_id", "publisher_id", "price", "size",
	         "action",      "side",    "flags",         "depth",        "ts_in_delta", "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::UTINYINT,
	                LogicalType::UTINYINT,     LogicalType::INTEGER,      LogicalType::UINTEGER};
	for (int i = 0; i < 10; ++i) {
		names.push_back(LevelCol("bid_px", i));
		return_types.push_back(LogicalType::DOUBLE);
		names.push_back(LevelCol("ask_px", i));
		return_types.push_back(LogicalType::DOUBLE);
		names.push_back(LevelCol("bid_sz", i));
		return_types.push_back(LogicalType::UINTEGER);
		names.push_back(LevelCol("ask_sz", i));
		return_types.push_back(LogicalType::UINTEGER);
		names.push_back(LevelCol("bid_ct", i));
		return_types.push_back(LogicalType::UINTEGER);
		names.push_back(LevelCol("ask_ct", i));
		return_types.push_back(LogicalType::UINTEGER);
	}
	return std::move(bd);
}

static void Mbp10Scan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto action_v = FlatVector::GetData<string_t>(out.data[6]);
	auto side_v = FlatVector::GetData<string_t>(out.data[7]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[8]);
	auto depth = FlatVector::GetData<uint8_t>(out.data[9]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[10]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[11]);

	double *bid_px[10], *ask_px[10];
	uint32_t *bid_sz[10], *ask_sz[10], *bid_ct[10], *ask_ct[10];
	for (int i = 0; i < 10; ++i) {
		const idx_t base = 12 + i * 6;
		bid_px[i] = FlatVector::GetData<double>(out.data[base + 0]);
		ask_px[i] = FlatVector::GetData<double>(out.data[base + 1]);
		bid_sz[i] = FlatVector::GetData<uint32_t>(out.data[base + 2]);
		ask_sz[i] = FlatVector::GetData<uint32_t>(out.data[base + 3]);
		bid_ct[i] = FlatVector::GetData<uint32_t>(out.data[base + 4]);
		ask_ct[i] = FlatVector::GetData<uint32_t>(out.data[base + 5]);
	}

	databento::Mbp10Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbp10)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		depth[n] = rec.depth;
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		seq[n] = rec.sequence;
		for (int i = 0; i < 10; ++i) {
			const auto &L = rec.levels[i];
			bid_px[i][n] = static_cast<double>(L.bid_px) * 1e-9;
			ask_px[i][n] = static_cast<double>(L.ask_px) * 1e-9;
			bid_sz[i][n] = L.bid_sz;
			ask_sz[i][n] = L.ask_sz;
			bid_ct[i][n] = L.bid_ct;
			ask_ct[i][n] = L.ask_ct;
		}
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// bbo-1s / bbo-1m — BboMsg (80 bytes), RType::Bbo1S / Bbo1M.
// No action/depth/ts_in_delta. levels[0] flattened.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> BboBind(ClientContext &, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event",  "ts_recv",  "instrument_id", "publisher_id", "price",
	         "size",      "side",     "flags",         "bid_price",    "ask_price",
	         "bid_size",  "ask_size", "bid_ct",        "ask_ct",       "sequence"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::UTINYINT,     LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::UINTEGER};
	return std::move(bd);
}

static void BboScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto side_v = FlatVector::GetData<string_t>(out.data[6]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[7]);
	auto bid_px = FlatVector::GetData<double>(out.data[8]);
	auto ask_px = FlatVector::GetData<double>(out.data[9]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[10]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[11]);
	auto bid_ct = FlatVector::GetData<uint32_t>(out.data[12]);
	auto ask_ct = FlatVector::GetData<uint32_t>(out.data[13]);
	auto seq = FlatVector::GetData<uint32_t>(out.data[14]);

	databento::BboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		side_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_ct[n] = L.bid_ct;
		ask_ct[n] = L.ask_ct;
		seq[n] = rec.sequence;
		++n;
	}
	out.SetCardinality(n);
}

static void Bbo1sScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	BboScanImpl(c, input, out, databento::RType::Bbo1S);
}
static void Bbo1mScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	BboScanImpl(c, input, out, databento::RType::Bbo1M);
}

// ═════════════════════════════════════════════════════════════════════════════
// cbbo-1s — CbboMsg (80 bytes), RType::Cbbo1S.
// Uses ConsolidatedBidAskPair: bid_pb/ask_pb (publisher bitmask) instead of bid_ct/ask_ct.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> CbboBind(ClientContext &, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event", "ts_recv",   "instrument_id", "publisher_id", "price",    "size",   "side",   "flags",
	         "bid_price","ask_price", "bid_size",      "ask_size",     "bid_pb",   "ask_pb"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::UTINYINT,     LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::USMALLINT};
	return std::move(bd);
}

static void Cbbo1sScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto side_v = FlatVector::GetData<string_t>(out.data[6]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[7]);
	auto bid_px = FlatVector::GetData<double>(out.data[8]);
	auto ask_px = FlatVector::GetData<double>(out.data[9]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[10]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[11]);
	auto bid_pb = FlatVector::GetData<uint16_t>(out.data[12]);
	auto ask_pb = FlatVector::GetData<uint16_t>(out.data[13]);

	databento::CbboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Cbbo1S)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		side_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_pb[n] = L.bid_pb;
		ask_pb[n] = L.ask_pb;
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// cmbp-1 — Cmbp1Msg (80 bytes), RType::Cmbp1.
// Like Mbp1 but with ConsolidatedBidAskPair (bid_pb/ask_pb).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> Cmbp1Bind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event",  "ts_recv",  "instrument_id", "publisher_id", "price",
	         "size",      "action",   "side",          "flags",        "ts_in_delta",
	         "bid_price", "ask_price","bid_size",      "ask_size",     "bid_pb",
	         "ask_pb"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::UTINYINT,
	                LogicalType::INTEGER,      LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::USMALLINT,
	                LogicalType::USMALLINT};
	return std::move(bd);
}

static void Cmbp1Scan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto price = FlatVector::GetData<double>(out.data[4]);
	auto size = FlatVector::GetData<uint32_t>(out.data[5]);
	auto action_v = FlatVector::GetData<string_t>(out.data[6]);
	auto side_v = FlatVector::GetData<string_t>(out.data[7]);
	auto flags = FlatVector::GetData<uint8_t>(out.data[8]);
	auto ts_in_d = FlatVector::GetData<int32_t>(out.data[9]);
	auto bid_px = FlatVector::GetData<double>(out.data[10]);
	auto ask_px = FlatVector::GetData<double>(out.data[11]);
	auto bid_sz = FlatVector::GetData<uint32_t>(out.data[12]);
	auto ask_sz = FlatVector::GetData<uint32_t>(out.data[13]);
	auto bid_pb = FlatVector::GetData<uint16_t>(out.data[14]);
	auto ask_pb = FlatVector::GetData<uint16_t>(out.data[15]);

	databento::Cmbp1Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Cmbp1)) {
		const auto &L = rec.levels[0];
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		price[n] = static_cast<double>(rec.price) * 1e-9;
		size[n] = rec.size;
		action_v[n] = duckdb_dbn::EmitChar1(out.data[6], static_cast<char>(rec.action));
		side_v[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.side));
		flags[n] = rec.flags.Raw();
		ts_in_d[n] = static_cast<int32_t>(rec.ts_in_delta.count());
		bid_px[n] = static_cast<double>(L.bid_px) * 1e-9;
		ask_px[n] = static_cast<double>(L.ask_px) * 1e-9;
		bid_sz[n] = L.bid_sz;
		ask_sz[n] = L.ask_sz;
		bid_pb[n] = L.bid_pb;
		ask_pb[n] = L.ask_pb;
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// ohlcv-{1s,1m,1h,1d} — OhlcvMsg (56 bytes), RType::Ohlcv1{S,M,H,D}.
// 8 columns: ts_event, instrument_id, publisher_id, open, high, low, close, volume.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> OhlcvBind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event", "instrument_id", "publisher_id", "open", "high", "low", "close", "volume"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER, LogicalType::USMALLINT, LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::DOUBLE,   LogicalType::DOUBLE,    LogicalType::UBIGINT};
	return std::move(bd);
}

static void OhlcvScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[1]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[2]);
	auto open_ = FlatVector::GetData<double>(out.data[3]);
	auto high = FlatVector::GetData<double>(out.data[4]);
	auto low = FlatVector::GetData<double>(out.data[5]);
	auto close = FlatVector::GetData<double>(out.data[6]);
	auto volume = FlatVector::GetData<uint64_t>(out.data[7]);

	databento::OhlcvMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		open_[n] = static_cast<double>(rec.open) * 1e-9;
		high[n] = static_cast<double>(rec.high) * 1e-9;
		low[n] = static_cast<double>(rec.low) * 1e-9;
		close[n] = static_cast<double>(rec.close) * 1e-9;
		volume[n] = rec.volume;
		++n;
	}
	out.SetCardinality(n);
}

static void Ohlcv1sScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1S);
}
static void Ohlcv1mScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1M);
}
static void Ohlcv1hScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1H);
}
static void Ohlcv1dScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::Ohlcv1D);
}

// ═════════════════════════════════════════════════════════════════════════════
// status — StatusMsg (40 bytes), RType::Status.
// action/reason/trading_event as USMALLINT raw enums (forward-compat).
// is_trading/is_quoting/is_short_sell_restricted as 1-char VARCHAR (TriState).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> StatusBind(ClientContext &, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event",       "ts_recv",        "instrument_id",            "publisher_id", "action",
	         "reason",         "trading_event",  "is_trading",               "is_quoting",   "is_short_sell_restricted"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::USMALLINT,    LogicalType::USMALLINT,
	                LogicalType::USMALLINT,    LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::VARCHAR};
	return std::move(bd);
}

static void StatusScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto action = FlatVector::GetData<uint16_t>(out.data[4]);
	auto reason = FlatVector::GetData<uint16_t>(out.data[5]);
	auto trading_event = FlatVector::GetData<uint16_t>(out.data[6]);
	auto is_trading = FlatVector::GetData<string_t>(out.data[7]);
	auto is_quoting = FlatVector::GetData<string_t>(out.data[8]);
	auto is_ssr = FlatVector::GetData<string_t>(out.data[9]);

	databento::StatusMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Status)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		action[n] = static_cast<uint16_t>(rec.action);
		reason[n] = static_cast<uint16_t>(rec.reason);
		trading_event[n] = static_cast<uint16_t>(rec.trading_event);
		is_trading[n] = duckdb_dbn::EmitChar1(out.data[7], static_cast<char>(rec.is_trading));
		is_quoting[n] = duckdb_dbn::EmitChar1(out.data[8], static_cast<char>(rec.is_quoting));
		is_ssr[n] = duckdb_dbn::EmitChar1(out.data[9], static_cast<char>(rec.is_short_sell_restricted));
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// imbalance — ImbalanceMsg (112 bytes), RType::Imbalance.
// 23 columns; mirrors the wire layout (Julia's mapper has known bugs).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> ImbalanceBind(ClientContext &, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event",
	         "ts_recv",
	         "instrument_id",
	         "publisher_id",
	         "ref_price",
	         "auction_time",
	         "cont_book_clr_price",
	         "auct_interest_clr_price",
	         "ssr_filling_price",
	         "ind_match_price",
	         "upper_collar",
	         "lower_collar",
	         "paired_qty",
	         "total_imbalance_qty",
	         "market_imbalance_qty",
	         "unpaired_qty",
	         "auction_type",
	         "side",
	         "auction_status",
	         "freeze_status",
	         "num_extensions",
	         "unpaired_side",
	         "significant_imbalance"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::TIMESTAMP_NS,
	                LogicalType::DOUBLE,       LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::DOUBLE,       LogicalType::DOUBLE,
	                LogicalType::UINTEGER,     LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::UINTEGER,     LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::UTINYINT,     LogicalType::UTINYINT,     LogicalType::UTINYINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR};
	return std::move(bd);
}

static void ImbalanceScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	auto ts_event = FlatVector::GetData<int64_t>(out.data[0]);
	auto ts_recv = FlatVector::GetData<int64_t>(out.data[1]);
	auto instr = FlatVector::GetData<uint32_t>(out.data[2]);
	auto pub = FlatVector::GetData<uint16_t>(out.data[3]);
	auto ref_px = FlatVector::GetData<double>(out.data[4]);
	auto auc_time = FlatVector::GetData<int64_t>(out.data[5]);
	auto cont_clr = FlatVector::GetData<double>(out.data[6]);
	auto auct_int_clr = FlatVector::GetData<double>(out.data[7]);
	auto ssr_fill = FlatVector::GetData<double>(out.data[8]);
	auto ind_match = FlatVector::GetData<double>(out.data[9]);
	auto upper_collar = FlatVector::GetData<double>(out.data[10]);
	auto lower_collar = FlatVector::GetData<double>(out.data[11]);
	auto paired_qty = FlatVector::GetData<uint32_t>(out.data[12]);
	auto total_imb = FlatVector::GetData<uint32_t>(out.data[13]);
	auto mkt_imb = FlatVector::GetData<uint32_t>(out.data[14]);
	auto unpaired_qty = FlatVector::GetData<uint32_t>(out.data[15]);
	auto auc_type = FlatVector::GetData<string_t>(out.data[16]);
	auto side_v = FlatVector::GetData<string_t>(out.data[17]);
	auto auc_status = FlatVector::GetData<uint8_t>(out.data[18]);
	auto freeze = FlatVector::GetData<uint8_t>(out.data[19]);
	auto num_ext = FlatVector::GetData<uint8_t>(out.data[20]);
	auto unp_side = FlatVector::GetData<string_t>(out.data[21]);
	auto sig_imb = FlatVector::GetData<string_t>(out.data[22]);

	databento::ImbalanceMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Imbalance)) {
		ts_event[n] = TsToInt64(rec.hd.ts_event);
		ts_recv[n] = TsToInt64(rec.ts_recv);
		instr[n] = rec.hd.instrument_id;
		pub[n] = rec.hd.publisher_id;
		ref_px[n] = static_cast<double>(rec.ref_price) * 1e-9;
		auc_time[n] = TsToInt64(rec.auction_time);
		cont_clr[n] = static_cast<double>(rec.cont_book_clr_price) * 1e-9;
		auct_int_clr[n] = static_cast<double>(rec.auct_interest_clr_price) * 1e-9;
		ssr_fill[n] = static_cast<double>(rec.ssr_filling_price) * 1e-9;
		ind_match[n] = static_cast<double>(rec.ind_match_price) * 1e-9;
		upper_collar[n] = static_cast<double>(rec.upper_collar) * 1e-9;
		lower_collar[n] = static_cast<double>(rec.lower_collar) * 1e-9;
		paired_qty[n] = rec.paired_qty;
		total_imb[n] = rec.total_imbalance_qty;
		mkt_imb[n] = rec.market_imbalance_qty;
		unpaired_qty[n] = rec.unpaired_qty;
		auc_type[n] = duckdb_dbn::EmitChar1(out.data[16], rec.auction_type);
		side_v[n] = duckdb_dbn::EmitChar1(out.data[17], static_cast<char>(rec.side));
		auc_status[n] = rec.auction_status;
		freeze[n] = rec.freeze_status;
		num_ext[n] = rec.num_extensions;
		unp_side[n] = duckdb_dbn::EmitChar1(out.data[21], static_cast<char>(rec.unpaired_side));
		sig_imb[n] = duckdb_dbn::EmitChar1(out.data[22], rec.significant_imbalance);
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// Registration
// ═════════════════════════════════════════════════════════════════════════════

static void Register(ExtensionLoader &loader, const char *name, table_function_bind_t bind, table_function_t scan) {
	TableFunction f(name, {LogicalType::VARCHAR}, scan, bind, ReadDbnInitGlobal);
	loader.RegisterFunction(f);
}

static void LoadInternal(ExtensionLoader &loader) {
	Register(loader, "read_dbn_trades", TradesBind, TradesScan);
	Register(loader, "read_dbn_mbo", MboBind, MboScan);
	Register(loader, "read_dbn_mbp1", Mbp1Bind, Mbp1Scan);
	Register(loader, "read_dbn_mbp10", Mbp10Bind, Mbp10Scan);
	Register(loader, "read_dbn_bbo_1s", BboBind, Bbo1sScan);
	Register(loader, "read_dbn_bbo_1m", BboBind, Bbo1mScan);
	Register(loader, "read_dbn_cbbo_1s", CbboBind, Cbbo1sScan);
	Register(loader, "read_dbn_cmbp1", Cmbp1Bind, Cmbp1Scan);
	Register(loader, "read_dbn_tbbo", TbboBind, TbboScan);
	Register(loader, "read_dbn_ohlcv_1s", OhlcvBind, Ohlcv1sScan);
	Register(loader, "read_dbn_ohlcv_1m", OhlcvBind, Ohlcv1mScan);
	Register(loader, "read_dbn_ohlcv_1h", OhlcvBind, Ohlcv1hScan);
	Register(loader, "read_dbn_ohlcv_1d", OhlcvBind, Ohlcv1dScan);
	Register(loader, "read_dbn_status", StatusBind, StatusScan);
	Register(loader, "read_dbn_imbalance", ImbalanceBind, ImbalanceScan);
}

void DbnExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DbnExtension::Name() {
	return "dbn";
}

std::string DbnExtension::Version() const {
#ifdef EXT_VERSION_DBN
	return EXT_VERSION_DBN;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(dbn, loader) {
	duckdb::LoadInternal(loader);
}
}
