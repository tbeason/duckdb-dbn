#define DUCKDB_EXTENSION_MAIN

#include "dbn_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <memory>
#include <array>
#include <limits>
#include <utility>

#include "databento/enums.hpp"
#include "databento/record.hpp"
#include "databento/v1.hpp"
#include "databento/v2.hpp"
#include "dbn_emit_helpers.hpp"
#include "dbn_native_decoder.hpp"

namespace duckdb {

// ─────────────────────────────────────────────────────────────────────────────
// Shared bind data and global state. Every per-schema table function uses
// these — only the column list and scan loop differ per schema.
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Projection + filter pushdown infrastructure
// ─────────────────────────────────────────────────────────────────────────────
//
// All per-schema scans share a common contract:
//   * The first four logical columns are header-derived:
//       0 = ts_event (TIMESTAMP_NS, mapped from RecordHeader::ts_event)
//       1 = ts_recv  (TIMESTAMP_NS) — body field, NOT header. NOT eligible for
//                                     pre-write filtering by DbnHeaderFilter.
//       2 = instrument_id (UINTEGER)
//       3 = publisher_id  (USMALLINT)
//   * ohlcv schemas omit ts_recv; their layout is
//       0 = ts_event, 1 = instrument_id, 2 = publisher_id.
//   * dbn_records exposes (ts_event, rtype, length, publisher_id,
//                          instrument_id, body); the column indexes of the
//                          three pushdown-eligible fields differ.
//
// Because the canonical schemas all bind ts_event/instrument_id/publisher_id
// at fixed positions 0/2/3, we parameterize DbnHeaderFilter by *logical column
// id*. Each scan tells the filter (at init-global time) which logical
// columns correspond to ts_event / instrument_id / publisher_id; the filter
// then walks the TableFilterSet looking only at those entries and stashes
// the predicates as plain integer ranges + IN-lists.
//
// We intentionally only honor predicates on the three header fields. Filters
// on body columns (price, side, action, etc.) are left in the TableFilterSet
// for DuckDB to evaluate above us — that's how all built-in extension scans
// behave when they pre-screen on a subset of pushdown columns.

struct DbnHeaderFilter {
	// ts_event: closed interval. Default = wide open.
	int64_t ts_event_min = std::numeric_limits<int64_t>::min();
	int64_t ts_event_max = std::numeric_limits<int64_t>::max();
	bool ts_event_active = false;

	// instrument_id: optional equality / IN-list / range.
	std::vector<uint32_t> instr_in;   // empty = no IN-filter
	uint32_t instr_min = 0;
	uint32_t instr_max = std::numeric_limits<uint32_t>::max();
	bool instr_range_active = false;

	// publisher_id: same shape.
	std::vector<uint16_t> pub_in;
	uint16_t pub_min = 0;
	uint16_t pub_max = std::numeric_limits<uint16_t>::max();
	bool pub_range_active = false;

	bool any_active = false;

	// Returns true iff every active predicate is satisfied.
	inline bool Matches(const databento::RecordHeader &hd) const {
		if (!any_active) {
			return true;
		}
		if (ts_event_active) {
			const auto t = static_cast<int64_t>(hd.ts_event.time_since_epoch().count());
			if (t < ts_event_min || t > ts_event_max) {
				return false;
			}
		}
		if (instr_range_active) {
			if (hd.instrument_id < instr_min || hd.instrument_id > instr_max) {
				return false;
			}
		}
		if (!instr_in.empty()) {
			bool ok = false;
			for (auto v : instr_in) {
				if (v == hd.instrument_id) { ok = true; break; }
			}
			if (!ok) return false;
		}
		if (pub_range_active) {
			if (hd.publisher_id < pub_min || hd.publisher_id > pub_max) {
				return false;
			}
		}
		if (!pub_in.empty()) {
			bool ok = false;
			for (auto v : pub_in) {
				if (v == hd.publisher_id) { ok = true; break; }
			}
			if (!ok) return false;
		}
		return true;
	}
};

// Tighten min / max for a column whose logical type is integral (signed or
// unsigned). All inputs come from a ConstantFilter, so values are guaranteed
// to fit the declared SQL type — the static_casts are safe.
template <typename T>
static inline void TightenIntRange(const ConstantFilter &cf, T &lo, T &hi, bool &active) {
	const auto v = cf.constant.GetValue<int64_t>();
	const auto tv = static_cast<T>(v);
	switch (cf.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (tv > lo) lo = tv;
		if (tv < hi) hi = tv;
		active = true;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		// hd > tv  ⇒  hd >= tv+1  (only safe when tv < numeric_limits<T>::max())
		if (tv < std::numeric_limits<T>::max() && static_cast<T>(tv + 1) > lo) {
			lo = static_cast<T>(tv + 1);
		}
		active = true;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		if (tv > lo) lo = tv;
		active = true;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		if (tv > std::numeric_limits<T>::min() && static_cast<T>(tv - 1) < hi) {
			hi = static_cast<T>(tv - 1);
		}
		active = true;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		if (tv < hi) hi = tv;
		active = true;
		break;
	default:
		// Unsupported comparison — silently let DuckDB re-evaluate above us.
		break;
	}
}

// Specialization for ts_event: timestamps come through as TIMESTAMP_NS, whose
// physical storage is int64. We pull the raw int64 out via Value::GetValue<int64_t>().
static inline void TightenTsEventRange(const ConstantFilter &cf, DbnHeaderFilter &hf) {
	int64_t tv;
	try {
		tv = cf.constant.GetValue<int64_t>();
	} catch (...) {
		return; // unsupported constant type — skip
	}
	switch (cf.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (tv > hf.ts_event_min) hf.ts_event_min = tv;
		if (tv < hf.ts_event_max) hf.ts_event_max = tv;
		hf.ts_event_active = true;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		if (tv < std::numeric_limits<int64_t>::max() && tv + 1 > hf.ts_event_min) {
			hf.ts_event_min = tv + 1;
		}
		hf.ts_event_active = true;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		if (tv > hf.ts_event_min) hf.ts_event_min = tv;
		hf.ts_event_active = true;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		if (tv > std::numeric_limits<int64_t>::min() && tv - 1 < hf.ts_event_max) {
			hf.ts_event_max = tv - 1;
		}
		hf.ts_event_active = true;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		if (tv < hf.ts_event_max) hf.ts_event_max = tv;
		hf.ts_event_active = true;
		break;
	default:
		break;
	}
}

// Walk one (column, filter) entry from the TableFilterSet. `which` says which
// header field the column belongs to. Unknown filter kinds (expression, bloom,
// dynamic, struct) are silently skipped — DuckDB will re-apply them above us.
enum class HeaderColumn { TsEvent, InstrumentId, PublisherId };

static void IngestFilter(HeaderColumn which, const TableFilter &flt, DbnHeaderFilter &hf) {
	switch (flt.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		const auto &cf = flt.Cast<ConstantFilter>();
		switch (which) {
		case HeaderColumn::TsEvent:
			TightenTsEventRange(cf, hf);
			break;
		case HeaderColumn::InstrumentId:
			TightenIntRange<uint32_t>(cf, hf.instr_min, hf.instr_max, hf.instr_range_active);
			break;
		case HeaderColumn::PublisherId:
			TightenIntRange<uint16_t>(cf, hf.pub_min, hf.pub_max, hf.pub_range_active);
			break;
		}
		break;
	}
	case TableFilterType::IN_FILTER: {
		const auto &inf = flt.Cast<InFilter>();
		switch (which) {
		case HeaderColumn::TsEvent: {
			// IN on ts_event is rare and would produce a disjoint set, which the
			// flat min/max can't capture. Skip — DuckDB re-applies it.
			break;
		}
		case HeaderColumn::InstrumentId: {
			hf.instr_in.reserve(hf.instr_in.size() + inf.values.size());
			for (auto &v : inf.values) {
				try {
					hf.instr_in.push_back(static_cast<uint32_t>(v.GetValue<int64_t>()));
				} catch (...) {}
			}
			break;
		}
		case HeaderColumn::PublisherId: {
			hf.pub_in.reserve(hf.pub_in.size() + inf.values.size());
			for (auto &v : inf.values) {
				try {
					hf.pub_in.push_back(static_cast<uint16_t>(v.GetValue<int64_t>()));
				} catch (...) {}
			}
			break;
		}
		}
		break;
	}
	case TableFilterType::CONJUNCTION_AND: {
		const auto &cj = flt.Cast<ConjunctionAndFilter>();
		for (auto &child : cj.child_filters) {
			IngestFilter(which, *child, hf);
		}
		break;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		// OptionalFilter wraps a real child filter; the inner one carries the
		// actual predicate. We unwrap and recurse.
		// (OptionalFilter is defined as { unique_ptr<TableFilter> child_filter; }
		// in duckdb/planner/filter/optional_filter.hpp.)
		struct OptionalFilterShape : public TableFilter {
			unique_ptr<TableFilter> child_filter;
		};
		const auto &of = reinterpret_cast<const OptionalFilterShape &>(flt);
		if (of.child_filter) {
			IngestFilter(which, *of.child_filter, hf);
		}
		break;
	}
	// CONJUNCTION_OR, IS_NULL, IS_NOT_NULL, STRUCT_EXTRACT, DYNAMIC_FILTER,
	// EXPRESSION_FILTER, BLOOM_FILTER: not pre-screened. DuckDB applies above.
	default:
		break;
	}
}

// Header-column-id mapping for each per-schema layout. Pass kNoCol when a
// header field is absent from a schema (ohlcv has no ts_recv, but we don't
// pre-filter ts_recv anyway, so this mostly matters for asserting positions).
static constexpr column_t kNoCol = static_cast<column_t>(-1);

struct DbnHeaderColumnLayout {
	column_t ts_event_col;     // logical column id of ts_event, or kNoCol
	column_t instrument_id_col;
	column_t publisher_id_col;
};

// Standard layout for almost every schema: ts_event=0, instrument_id=2,
// publisher_id=3. Ohlcv: ts_event=0, instrument_id=1, publisher_id=2.
// dbn_records: ts_event=0, publisher_id=3, instrument_id=4.
static constexpr DbnHeaderColumnLayout kHeaderLayoutStandard {0, 2, 3};
static constexpr DbnHeaderColumnLayout kHeaderLayoutOhlcv    {0, 1, 2};
static constexpr DbnHeaderColumnLayout kHeaderLayoutRecords  {0, 4, 3};

static DbnHeaderFilter BuildHeaderFilter(optional_ptr<TableFilterSet> filters,
                                         const std::vector<column_t> &column_ids,
                                         const DbnHeaderColumnLayout &layout) {
	DbnHeaderFilter hf;
	if (!filters) {
		return hf;
	}
	// When projection_pushdown=true, TableFilterSet keys are indices into
	// column_ids (projected-space), NOT raw logical column ids. We translate
	// through column_ids to recover the original column id before matching
	// against the layout. Without this, a filter on instrument_id (col 2)
	// shows up as filter on projection slot 0 (because column_ids=[2]) and
	// got silently routed to whichever HeaderColumn matched logical-id 0
	// — usually ts_event — producing nonsense ranges that rejected every row.
	for (auto &entry : filters->filters) {
		const auto pidx = entry.first;
		if (pidx >= column_ids.size()) {
			continue;
		}
		const auto col = column_ids[pidx];
		if (col == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		const auto &flt = *entry.second;
		if (col == layout.ts_event_col) {
			IngestFilter(HeaderColumn::TsEvent, flt, hf);
		} else if (col == layout.instrument_id_col) {
			IngestFilter(HeaderColumn::InstrumentId, flt, hf);
		} else if (col == layout.publisher_id_col) {
			IngestFilter(HeaderColumn::PublisherId, flt, hf);
		}
		// Other columns — body predicates — are handled separately by
		// ApplyBodyFilters after the scan emits a chunk (see Phase 5-E).
		// Skipping them here keeps the header pre-screen branch-light.
	}
	hf.any_active = hf.ts_event_active || hf.instr_range_active || !hf.instr_in.empty() ||
	                hf.pub_range_active || !hf.pub_in.empty();
	return hf;
}

// ─────────────────────────────────────────────────────────────────────────────
// Body-column filter evaluator
//
// DbnHeaderFilter pre-screens records by ts_event / instrument_id /
// publisher_id during the scan loop (cheap, per-record) — but DuckDB pushes
// ALL WHEREs to us when filter_pushdown=true, including ones on body
// columns (price, size, side, bid_price, bid_size, ...). After the scan
// populates the output chunk with records that survived the header
// pre-screen, ApplyBodyFilters evaluates every non-header TableFilter
// against the chunk via ExpressionExecutor and slices to keep only the
// rows that satisfy all remaining predicates.
//
// Without this, body-column WHEREs are silently dropped — the very bug
// that motivated the temporary filter_pushdown=false from commit 307ff92.
// ─────────────────────────────────────────────────────────────────────────────

// Build the AND'd body-filter Expression from the pushed-down TableFilterSet.
// Header-column filters (ts_event / instrument_id / publisher_id) are skipped —
// they're applied per-record inside the scan loop by DbnHeaderFilter::Matches.
// Column types are read from the supplied chunk's vectors (which match the
// projected schema). Returns nullptr if there are no body filters to apply.
static unique_ptr<Expression> BuildBodyFilterExpr(const TableFilterSet &filters,
                                                  const std::vector<column_t> &column_ids,
                                                  const DbnHeaderColumnLayout &layout,
                                                  const DataChunk &chunk) {
	vector<unique_ptr<Expression>> filter_exprs;
	for (auto &entry : filters.filters) {
		const auto pidx = entry.first;
		if (pidx >= column_ids.size()) {
			continue;
		}
		const auto col = column_ids[pidx];
		if (col == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		if (col == layout.ts_event_col || col == layout.instrument_id_col ||
		    col == layout.publisher_id_col) {
			continue;  // header pre-screen already handled this
		}
		auto col_ref = make_uniq<BoundReferenceExpression>(chunk.data[pidx].GetType(), pidx);
		filter_exprs.push_back(entry.second->ToExpression(*col_ref));
	}
	if (filter_exprs.empty()) {
		return nullptr;
	}
	if (filter_exprs.size() == 1) {
		return std::move(filter_exprs[0]);
	}
	auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &e : filter_exprs) {
		conj->children.push_back(std::move(e));
	}
	return std::move(conj);
}

// Evaluate the cached body-filter Expression against `chunk` and slice down
// to the rows that match. Cheap no-op if `expr` is null. ExpressionExecutor
// is reconstructed per call: its state is per-pipeline and constructing
// from a const Expression is inexpensive compared to evaluating against
// STANDARD_VECTOR_SIZE rows.
static void EvalBodyFilterExpr(ClientContext &context, const Expression &expr, DataChunk &chunk) {
	ExpressionExecutor executor(context, expr);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	const idx_t pass = executor.SelectExpression(chunk, sel);
	if (pass < chunk.size()) {
		chunk.Slice(sel, pass);
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared bind data and global state. Every per-schema table function uses
// these — only the column list and scan loop differ per schema.
// ─────────────────────────────────────────────────────────────────────────────

struct ReadDbnBindData : public TableFunctionData {
	// Resolved at bind time. Always at least one entry; multiple entries
	// when the user passed a glob pattern.
	std::vector<std::string> file_paths;
	DbnHeaderColumnLayout header_layout = kHeaderLayoutStandard;
	// Set by the RegisterWithSymbols bind wrapper when the user passed
	// symbols := true. The resolved-symbol column is appended last, so its
	// logical column id equals the base column count.
	bool with_symbols = false;
	idx_t symbol_col_id = 0;
};

struct ReadDbnGlobalState : public GlobalTableFunctionState {
	ReadDbnGlobalState(std::vector<std::string> paths,
	                   std::vector<column_t> column_ids_p,
	                   DbnHeaderFilter header_filter_p,
	                   unique_ptr<TableFilterSet> filters_p)
	    : reader(std::make_unique<duckdb_dbn::DbnFileReader>(std::move(paths))),
	      column_ids(std::move(column_ids_p)),
	      header_filter(std::move(header_filter_p)),
	      filters(std::move(filters_p)) {}
	std::unique_ptr<duckdb_dbn::DbnFileReader> reader;
	// Logical column ids the optimizer asked for, in projection order.
	// out.data[i] corresponds to column_ids[i]. May contain
	// COLUMN_IDENTIFIER_ROW_ID for row-id requests — scans must treat that
	// as "skip, no output to fill for this slot" since we never emit row ids.
	std::vector<column_t> column_ids;
	DbnHeaderFilter header_filter;
	// Deep copy of the pushed-down filter set, stashed at init so the scan
	// wrapper can build/cache the body-filter Expression. TableFunctionInput
	// (scan) doesn't expose filters — only TableFunctionInitInput does.
	unique_ptr<TableFilterSet> filters;
	// Cached body-filter Expression, built lazily on the first scan call
	// (need a DataChunk in hand for projected column types). nullptr both
	// before init and when there are no body filters to apply.
	// `body_filter_built` distinguishes "not yet built" from "built, no-op".
	unique_ptr<Expression> body_filter_expr;
	bool body_filter_built = false;

	// Symbol resolution (symbols := true). When set, each scan snapshots the
	// active output symbol for every emitted row into sym_scratch[row], and the
	// ScanWithBodyFilter wrapper materializes the `symbol` VARCHAR column from
	// it. nullptr entries → SQL NULL (no mapping seen for that instrument yet).
	// Resolution is single-threaded (these scans declare no parallel state), so
	// a plain per-global-state scratch buffer is safe.
	bool with_symbols = false;
	idx_t symbol_col_id = 0;
	std::array<const std::string *, STANDARD_VECTOR_SIZE> sym_scratch {};

	// Snapshot the current symbol for `instr` into row slot `n`. Called once per
	// emitted row by the per-schema scans (no-op unless symbols are tracked).
	inline void NoteSymbol(idx_t n, uint32_t instr) {
		if (with_symbols) {
			sym_scratch[n] = reader->CurrentSymbol(instr);
		}
	}
};

static unique_ptr<GlobalTableFunctionState>
ReadDbnInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<ReadDbnBindData>();
	std::vector<column_t> col_ids = input.column_ids;
	auto hf = BuildHeaderFilter(input.filters, col_ids, bd.header_layout);
	auto filters_copy = input.filters ? input.filters->Copy() : nullptr;
	auto gs = make_uniq<ReadDbnGlobalState>(bd.file_paths, std::move(col_ids),
	                                        std::move(hf), std::move(filters_copy));
	if (bd.with_symbols) {
		gs->with_symbols = true;
		gs->symbol_col_id = bd.symbol_col_id;
		gs->reader->EnableSymbolTracking();
	}
	return std::move(gs);
}


static std::string GetFilePathArg(TableFunctionBindInput &input) {
	return input.inputs[0].GetValue<string>();
}

// Expand a glob (or literal path) into a list of files. Throws if no file
// matches the pattern.
static std::vector<std::string> ExpandPaths(ClientContext &context, const std::string &pattern) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto opens = fs.Glob(pattern);
	std::vector<std::string> paths;
	paths.reserve(opens.size());
	for (const auto &of : opens) {
		paths.push_back(of.path);
	}
	if (paths.empty()) {
		throw IOException("dbn: no files match: " + pattern);
	}
	return paths;
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

static unique_ptr<FunctionData> TradesBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	// Safety: with projection_pushdown=true, out.data.size() == projected_count.
	// We assert in debug to catch any optimizer surprise; in release we trust it.
	D_ASSERT(out.data.size() == projected_count);

	// Pre-resolve typed pointers exactly once for each projected slot. We
	// pay the cost of an indirection per projected column (not per record)
	// and keep the inner loop branch-light. Slots for COLUMN_IDENTIFIER_ROW_ID
	// (we don't emit row ids) get a null pointer and are skipped in the loop.
	//
	// Per-column logical-id constants for trades:
	enum : column_t {
		COL_TS_EVENT = 0, COL_TS_RECV = 1, COL_INSTR = 2, COL_PUB = 3,
		COL_PRICE = 4,    COL_SIZE = 5,    COL_ACTION = 6, COL_SIDE = 7,
		COL_FLAGS = 8,    COL_DEPTH = 9,   COL_TS_IN_D = 10, COL_SEQ = 11,
	};

	// One pointer-array entry per *projection slot*, not per logical column.
	// Index of arrays below is the projection position, value is the typed
	// data pointer for that slot's underlying vector.
	std::array<int64_t *,   16> p_i64  {}; // ts_event, ts_recv
	std::array<uint32_t *,  16> p_u32  {}; // instrument_id, size, sequence
	std::array<uint16_t *,  16> p_u16  {}; // publisher_id
	std::array<double *,    16> p_dbl  {}; // price
	std::array<string_t *,  16> p_str  {}; // action, side
	std::array<uint8_t *,   16> p_u8   {}; // flags, depth
	std::array<int32_t *,   16> p_i32  {}; // ts_in_delta
	D_ASSERT(projected_count <= 16);       // Trades has 12 columns; safe.

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) {
			continue; // No row ids emitted; DuckDB will not actually read this slot.
		}
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:  p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_PRICE:    p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_SIZE:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ACTION:   p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_SIDE:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_FLAGS:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_DEPTH:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_TS_IN_D:  p_i32[i] = FlatVector::GetData<int32_t>(out.data[i]); break;
		case COL_SEQ:      p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		default:
			// Unknown column id from the optimizer (would be a DuckDB bug given
			// our bind declares exactly 12 columns). Leave the slot pointer
			// null; the loop's default branch ignores it.
			break;
		}
	}

	databento::TradeMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbp0)) {
		// Header pre-screen (early skip during decode). Body-field predicates
		// are evaluated by ApplyBodyFilters after the chunk is populated.
		if (!st.header_filter.Matches(rec.hd)) {
			continue;
		}
		st.NoteSymbol(n, rec.hd.instrument_id);
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT:
				p_i64[i][n] = TsToInt64(rec.hd.ts_event);
				break;
			case COL_TS_RECV:
				p_i64[i][n] = TsToInt64(rec.ts_recv);
				break;
			case COL_INSTR:
				p_u32[i][n] = rec.hd.instrument_id;
				break;
			case COL_PUB:
				p_u16[i][n] = rec.hd.publisher_id;
				break;
			case COL_PRICE:
				p_dbl[i][n] = static_cast<double>(rec.price) / 1e9;
				break;
			case COL_SIZE:
				p_u32[i][n] = rec.size;
				break;
			case COL_ACTION:
				// StringVector::AddString is only invoked when the column is
				// actually projected (we never reach this case otherwise).
				p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.action));
				break;
			case COL_SIDE:
				p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side));
				break;
			case COL_FLAGS:
				p_u8[i][n] = rec.flags.Raw();
				break;
			case COL_DEPTH:
				p_u8[i][n] = rec.depth;
				break;
			case COL_TS_IN_D:
				p_i32[i][n] = static_cast<int32_t>(rec.ts_in_delta.count());
				break;
			case COL_SEQ:
				p_u32[i][n] = rec.sequence;
				break;
			default:
				// COLUMN_IDENTIFIER_ROW_ID and unknown ids: skip.
				break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbo — RType::Mbo, MboMsg (56 bytes)
// Mirrors Julia mbo_to_dataframe ordering.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> MboBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0, COL_TS_RECV = 1, COL_INSTR = 2,    COL_PUB = 3,
		COL_ORDER_ID = 4, COL_PRICE = 5,   COL_SIZE = 6,     COL_FLAGS = 7,
		COL_CHANNEL  = 8, COL_ACTION = 9,  COL_SIDE = 10,    COL_TS_IN_D = 11,
		COL_SEQ      = 12,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<uint64_t *, 16> p_u64 {};
	std::array<double *,   16> p_dbl {};
	std::array<uint8_t *,  16> p_u8  {};
	std::array<string_t *, 16> p_str {};
	std::array<int32_t *,  16> p_i32 {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:  p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_ORDER_ID: p_u64[i] = FlatVector::GetData<uint64_t>(out.data[i]); break;
		case COL_PRICE:    p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_SIZE:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_FLAGS:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_CHANNEL:  p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_ACTION:   p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_SIDE:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_TS_IN_D:  p_i32[i] = FlatVector::GetData<int32_t>(out.data[i]); break;
		case COL_SEQ:      p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::MboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbo)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT: p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:  p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:    p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:      p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_ORDER_ID: p_u64[i][n] = rec.order_id; break;
			case COL_PRICE:    p_dbl[i][n] = static_cast<double>(rec.price) / 1e9; break;
			case COL_SIZE:     p_u32[i][n] = rec.size; break;
			case COL_FLAGS:    p_u8[i][n]  = rec.flags.Raw(); break;
			case COL_CHANNEL:  p_u8[i][n]  = rec.channel_id; break;
			case COL_ACTION:   p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.action)); break;
			case COL_SIDE:     p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side)); break;
			case COL_TS_IN_D:  p_i32[i][n] = static_cast<int32_t>(rec.ts_in_delta.count()); break;
			case COL_SEQ:      p_u32[i][n] = rec.sequence; break;
			default: break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// mbp-1 — RType::Mbp1, Mbp1Msg (80 bytes)
// Flattens levels[0] into bid_price/ask_price/bid_size/ask_size/bid_ct/ask_ct.
// Mirrors Julia mbp1_to_dataframe (action/side last).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> Mbp1Bind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0,  COL_TS_RECV = 1,  COL_INSTR = 2,    COL_PUB = 3,
		COL_BID_PRICE = 4, COL_ASK_PRICE = 5, COL_BID_SIZE = 6, COL_ASK_SIZE = 7,
		COL_BID_CT = 8,    COL_ASK_CT = 9,   COL_FLAGS = 10,   COL_TS_IN_D = 11,
		COL_SEQ = 12,      COL_ACTION = 13,  COL_SIDE = 14,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<double *,   16> p_dbl {};
	std::array<uint8_t *,  16> p_u8  {};
	std::array<int32_t *,  16> p_i32 {};
	std::array<string_t *, 16> p_str {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT:  p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:   p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:       p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_BID_PRICE: p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_ASK_PRICE: p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_BID_SIZE:  p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ASK_SIZE:  p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_BID_CT:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ASK_CT:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_FLAGS:     p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_TS_IN_D:   p_i32[i] = FlatVector::GetData<int32_t>(out.data[i]); break;
		case COL_SEQ:       p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ACTION:    p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_SIDE:      p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::Mbp1Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		const auto &L = rec.levels[0];
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT:  p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:   p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:     p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:       p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_BID_PRICE: p_dbl[i][n] = static_cast<double>(L.bid_px) / 1e9; break;
			case COL_ASK_PRICE: p_dbl[i][n] = static_cast<double>(L.ask_px) / 1e9; break;
			case COL_BID_SIZE:  p_u32[i][n] = L.bid_sz; break;
			case COL_ASK_SIZE:  p_u32[i][n] = L.ask_sz; break;
			case COL_BID_CT:    p_u32[i][n] = L.bid_ct; break;
			case COL_ASK_CT:    p_u32[i][n] = L.ask_ct; break;
			case COL_FLAGS:     p_u8[i][n]  = rec.flags.Raw(); break;
			case COL_TS_IN_D:   p_i32[i][n] = static_cast<int32_t>(rec.ts_in_delta.count()); break;
			case COL_SEQ:       p_u32[i][n] = rec.sequence; break;
			case COL_ACTION:    p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.action)); break;
			case COL_SIDE:      p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side)); break;
			default: break;
			}
		}
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

static unique_ptr<FunctionData> Mbp10Bind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0, COL_TS_RECV = 1, COL_INSTR = 2, COL_PUB = 3,
		COL_PRICE = 4,    COL_SIZE = 5,    COL_ACTION = 6, COL_SIDE = 7,
		COL_FLAGS = 8,    COL_DEPTH = 9,   COL_TS_IN_D = 10, COL_SEQ = 11,
	};
	static constexpr column_t COL_LEVELS_BASE = 12;
	static constexpr column_t COL_LEVELS_END  = COL_LEVELS_BASE + 60;

	// 72 base columns (12 header/trade + 60 across 10 book levels); +1 headroom
	// for the optional appended `symbol` column when symbols := true.
	std::array<int64_t *,   73> p_i64  {};
	std::array<uint32_t *,  73> p_u32  {};
	std::array<uint16_t *,  73> p_u16  {};
	std::array<double *,    73> p_dbl  {};
	std::array<string_t *,  73> p_str  {};
	std::array<uint8_t *,   73> p_u8   {};
	std::array<int32_t *,   73> p_i32  {};
	D_ASSERT(projected_count <= 73);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:  p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_PRICE:    p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_SIZE:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ACTION:   p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_SIDE:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_FLAGS:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_DEPTH:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_TS_IN_D:  p_i32[i] = FlatVector::GetData<int32_t>(out.data[i]); break;
		case COL_SEQ:      p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		default:
			if (cid >= COL_LEVELS_BASE && cid < COL_LEVELS_END) {
				const column_t offset = cid - COL_LEVELS_BASE;
				const column_t sub    = offset % 6;
				switch (sub) {
				case 0:
				case 1:
					p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
				case 2:
				case 3:
				case 4:
				case 5:
					p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
				default: break;
				}
			}
			break;
		}
	}

	databento::Mbp10Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Mbp10)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT: p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:  p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:    p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:      p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_PRICE:    p_dbl[i][n] = static_cast<double>(rec.price) / 1e9; break;
			case COL_SIZE:     p_u32[i][n] = rec.size; break;
			case COL_ACTION:   p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.action)); break;
			case COL_SIDE:     p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side)); break;
			case COL_FLAGS:    p_u8[i][n]  = rec.flags.Raw(); break;
			case COL_DEPTH:    p_u8[i][n]  = rec.depth; break;
			case COL_TS_IN_D:  p_i32[i][n] = static_cast<int32_t>(rec.ts_in_delta.count()); break;
			case COL_SEQ:      p_u32[i][n] = rec.sequence; break;
			default:
				if (cid >= COL_LEVELS_BASE && cid < COL_LEVELS_END) {
					const column_t offset = cid - COL_LEVELS_BASE;
					const column_t lvl    = offset / 6;
					const column_t sub    = offset % 6;
					const auto &L = rec.levels[lvl];
					switch (sub) {
					case 0: p_dbl[i][n] = static_cast<double>(L.bid_px) / 1e9; break;
					case 1: p_dbl[i][n] = static_cast<double>(L.ask_px) / 1e9; break;
					case 2: p_u32[i][n] = L.bid_sz; break;
					case 3: p_u32[i][n] = L.ask_sz; break;
					case 4: p_u32[i][n] = L.bid_ct; break;
					case 5: p_u32[i][n] = L.ask_ct; break;
					default: break;
					}
				}
				break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// bbo-1s / bbo-1m — BboMsg (80 bytes), RType::Bbo1S / Bbo1M.
// No action/depth/ts_in_delta. levels[0] flattened.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> BboBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0,  COL_TS_RECV = 1,   COL_INSTR = 2,    COL_PUB = 3,
		COL_PRICE = 4,     COL_SIZE = 5,      COL_SIDE = 6,     COL_FLAGS = 7,
		COL_BID_PRICE = 8, COL_ASK_PRICE = 9, COL_BID_SIZE = 10, COL_ASK_SIZE = 11,
		COL_BID_CT = 12,   COL_ASK_CT = 13,   COL_SEQ = 14,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<double *,   16> p_dbl {};
	std::array<string_t *, 16> p_str {};
	std::array<uint8_t *,  16> p_u8  {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT:   p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:    p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:      p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:        p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_PRICE:      p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_SIZE:       p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_SIDE:       p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_FLAGS:      p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_BID_PRICE:  p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_ASK_PRICE:  p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_BID_SIZE:   p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ASK_SIZE:   p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_BID_CT:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ASK_CT:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_SEQ:        p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::BboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		const auto &L = rec.levels[0];
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT:   p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:    p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:      p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:        p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_PRICE:      p_dbl[i][n] = static_cast<double>(rec.price) / 1e9; break;
			case COL_SIZE:       p_u32[i][n] = rec.size; break;
			case COL_SIDE:       p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side)); break;
			case COL_FLAGS:      p_u8[i][n]  = rec.flags.Raw(); break;
			case COL_BID_PRICE:  p_dbl[i][n] = static_cast<double>(L.bid_px) / 1e9; break;
			case COL_ASK_PRICE:  p_dbl[i][n] = static_cast<double>(L.ask_px) / 1e9; break;
			case COL_BID_SIZE:   p_u32[i][n] = L.bid_sz; break;
			case COL_ASK_SIZE:   p_u32[i][n] = L.ask_sz; break;
			case COL_BID_CT:     p_u32[i][n] = L.bid_ct; break;
			case COL_ASK_CT:     p_u32[i][n] = L.ask_ct; break;
			case COL_SEQ:        p_u32[i][n] = rec.sequence; break;
			default: break;
			}
		}
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

static unique_ptr<FunctionData> CbboBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event", "ts_recv",   "instrument_id", "publisher_id", "price",    "size",   "side",   "flags",
	         "bid_price","ask_price", "bid_size",      "ask_size",     "bid_pb",   "ask_pb"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::DOUBLE,       LogicalType::UINTEGER,
	                LogicalType::VARCHAR,      LogicalType::UTINYINT,     LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::UINTEGER,     LogicalType::UINTEGER,
	                LogicalType::USMALLINT,    LogicalType::USMALLINT};
	return std::move(bd);
}

static void CbboScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0, COL_TS_RECV = 1, COL_INSTR = 2,    COL_PUB = 3,
		COL_PRICE = 4,    COL_SIZE = 5,    COL_SIDE = 6,     COL_FLAGS = 7,
		COL_BID_PX = 8,   COL_ASK_PX = 9,  COL_BID_SZ = 10,  COL_ASK_SZ = 11,
		COL_BID_PB = 12,  COL_ASK_PB = 13,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<double *,   16> p_dbl {};
	std::array<string_t *, 16> p_str {};
	std::array<uint8_t *,  16> p_u8  {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:  p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_PRICE:    p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_SIZE:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_SIDE:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_FLAGS:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_BID_PX:   p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_ASK_PX:   p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_BID_SZ:   p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ASK_SZ:   p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_BID_PB:   p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_ASK_PB:   p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::CbboMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		const auto &L = rec.levels[0];
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT: p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:  p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:    p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:      p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_PRICE:    p_dbl[i][n] = static_cast<double>(rec.price) / 1e9; break;
			case COL_SIZE:     p_u32[i][n] = rec.size; break;
			case COL_SIDE:     p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side)); break;
			case COL_FLAGS:    p_u8[i][n]  = rec.flags.Raw(); break;
			case COL_BID_PX:   p_dbl[i][n] = static_cast<double>(L.bid_px) / 1e9; break;
			case COL_ASK_PX:   p_dbl[i][n] = static_cast<double>(L.ask_px) / 1e9; break;
			case COL_BID_SZ:   p_u32[i][n] = L.bid_sz; break;
			case COL_ASK_SZ:   p_u32[i][n] = L.ask_sz; break;
			case COL_BID_PB:   p_u16[i][n] = L.bid_pb; break;
			case COL_ASK_PB:   p_u16[i][n] = L.ask_pb; break;
			default: break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

static void Cbbo1sScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	CbboScanImpl(c, input, out, databento::RType::Cbbo1S);
}
static void Cbbo1mScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	CbboScanImpl(c, input, out, databento::RType::Cbbo1M);
}

// ═════════════════════════════════════════════════════════════════════════════
// cmbp-1 — Cmbp1Msg (80 bytes), RType::Cmbp1.
// Like Mbp1 but with ConsolidatedBidAskPair (bid_pb/ask_pb).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> Cmbp1Bind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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

static void Cmbp1ScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0,  COL_TS_RECV = 1,  COL_INSTR = 2,    COL_PUB = 3,
		COL_PRICE = 4,     COL_SIZE = 5,     COL_ACTION = 6,   COL_SIDE = 7,
		COL_FLAGS = 8,     COL_TS_IN_D = 9,  COL_BID_PX = 10,  COL_ASK_PX = 11,
		COL_BID_SZ = 12,   COL_ASK_SZ = 13,  COL_BID_PB = 14,  COL_ASK_PB = 15,
	};

	// 16 base columns; +1 headroom for the optional appended `symbol` column
	// when symbols := true (tcbbo/cmbp1 both bind exactly 16 base columns).
	std::array<int64_t *,   17> p_i64  {};
	std::array<uint32_t *,  17> p_u32  {};
	std::array<uint16_t *,  17> p_u16  {};
	std::array<double *,    17> p_dbl  {};
	std::array<string_t *,  17> p_str  {};
	std::array<uint8_t *,   17> p_u8   {};
	std::array<int32_t *,   17> p_i32  {};
	D_ASSERT(projected_count <= 17);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:  p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_PRICE:    p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_SIZE:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ACTION:   p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_SIDE:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_FLAGS:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_TS_IN_D:  p_i32[i] = FlatVector::GetData<int32_t>(out.data[i]); break;
		case COL_BID_PX:   p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_ASK_PX:   p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_BID_SZ:   p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_ASK_SZ:   p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_BID_PB:   p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_ASK_PB:   p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::Cmbp1Msg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		const auto &L = rec.levels[0];
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT: p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:  p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:    p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:      p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_PRICE:    p_dbl[i][n] = static_cast<double>(rec.price) / 1e9; break;
			case COL_SIZE:     p_u32[i][n] = rec.size; break;
			case COL_ACTION:   p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.action)); break;
			case COL_SIDE:     p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side)); break;
			case COL_FLAGS:    p_u8[i][n]  = rec.flags.Raw(); break;
			case COL_TS_IN_D:  p_i32[i][n] = static_cast<int32_t>(rec.ts_in_delta.count()); break;
			case COL_BID_PX:   p_dbl[i][n] = static_cast<double>(L.bid_px) / 1e9; break;
			case COL_ASK_PX:   p_dbl[i][n] = static_cast<double>(L.ask_px) / 1e9; break;
			case COL_BID_SZ:   p_u32[i][n] = L.bid_sz; break;
			case COL_ASK_SZ:   p_u32[i][n] = L.ask_sz; break;
			case COL_BID_PB:   p_u16[i][n] = L.bid_pb; break;
			case COL_ASK_PB:   p_u16[i][n] = L.ask_pb; break;
			default: break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

static void Cmbp1Scan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Cmbp1ScanImpl(c, input, out, databento::RType::Cmbp1);
}
static void TcbboScan(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	Cmbp1ScanImpl(c, input, out, databento::RType::Tcbbo);
}

// ═════════════════════════════════════════════════════════════════════════════
// ohlcv-{1s,1m,1h,1d} — OhlcvMsg (56 bytes), RType::Ohlcv1{S,M,H,D}.
// 8 columns: ts_event, instrument_id, publisher_id, open, high, low, close, volume.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> OhlcvBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	bd->header_layout = kHeaderLayoutOhlcv;
	names = {"ts_event", "instrument_id", "publisher_id", "open", "high", "low", "close", "volume"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER, LogicalType::USMALLINT, LogicalType::DOUBLE,
	                LogicalType::DOUBLE,       LogicalType::DOUBLE,   LogicalType::DOUBLE,    LogicalType::UBIGINT};
	return std::move(bd);
}

static void OhlcvScanImpl(ClientContext &, TableFunctionInput &input, DataChunk &out, databento::RType rtype) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0, COL_INSTR = 1, COL_PUB = 2,
		COL_OPEN = 3,     COL_HIGH = 4,  COL_LOW = 5,
		COL_CLOSE = 6,    COL_VOLUME = 7,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<double *,   16> p_dbl {};
	std::array<uint64_t *, 16> p_u64 {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_OPEN:     p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_HIGH:     p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_LOW:      p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_CLOSE:    p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_VOLUME:   p_u64[i] = FlatVector::GetData<uint64_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::OhlcvMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, rtype)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT: p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_INSTR:    p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:      p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_OPEN:     p_dbl[i][n] = static_cast<double>(rec.open) / 1e9; break;
			case COL_HIGH:     p_dbl[i][n] = static_cast<double>(rec.high) / 1e9; break;
			case COL_LOW:      p_dbl[i][n] = static_cast<double>(rec.low) / 1e9; break;
			case COL_CLOSE:    p_dbl[i][n] = static_cast<double>(rec.close) / 1e9; break;
			case COL_VOLUME:   p_u64[i][n] = rec.volume; break;
			default: break;
			}
		}
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
static void OhlcvEodScan(ClientContext &c, TableFunctionInput &i, DataChunk &o) {
	OhlcvScanImpl(c, i, o, databento::RType::OhlcvEod);
}

// ═════════════════════════════════════════════════════════════════════════════
// status — StatusMsg (40 bytes), RType::Status.
// action/reason/trading_event as USMALLINT raw enums (forward-compat).
// is_trading/is_quoting/is_short_sell_restricted as 1-char VARCHAR (TriState).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> StatusBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0, COL_TS_RECV = 1, COL_INSTR = 2, COL_PUB = 3,
		COL_ACTION = 4,   COL_REASON = 5,  COL_TRADING_EVENT = 6,
		COL_IS_TRADING = 7, COL_IS_QUOTING = 8, COL_IS_SSR = 9,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<string_t *, 16> p_str {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT:       p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:        p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:          p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:            p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_ACTION:         p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_REASON:         p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_TRADING_EVENT:  p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_IS_TRADING:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_IS_QUOTING:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_IS_SSR:         p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::StatusMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Status)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT:      p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:       p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:         p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:           p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_ACTION:        p_u16[i][n] = static_cast<uint16_t>(rec.action); break;
			case COL_REASON:        p_u16[i][n] = static_cast<uint16_t>(rec.reason); break;
			case COL_TRADING_EVENT: p_u16[i][n] = static_cast<uint16_t>(rec.trading_event); break;
			case COL_IS_TRADING:    p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.is_trading)); break;
			case COL_IS_QUOTING:    p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.is_quoting)); break;
			case COL_IS_SSR:        p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.is_short_sell_restricted)); break;
			default: break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// imbalance — ImbalanceMsg (112 bytes), RType::Imbalance.
// 23 columns; mirrors the wire layout (Julia's mapper has known bugs).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> ImbalanceBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
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
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0,    COL_TS_RECV = 1,            COL_INSTR = 2,           COL_PUB = 3,
		COL_REF_PRICE = 4,   COL_AUCTION_TIME = 5,       COL_CONT_CLR = 6,        COL_AUCT_INT_CLR = 7,
		COL_SSR_FILL = 8,    COL_IND_MATCH = 9,          COL_UPPER_COLLAR = 10,   COL_LOWER_COLLAR = 11,
		COL_PAIRED_QTY = 12, COL_TOTAL_IMB = 13,         COL_MKT_IMB = 14,        COL_UNPAIRED_QTY = 15,
		COL_AUC_TYPE = 16,   COL_SIDE = 17,              COL_AUC_STATUS = 18,     COL_FREEZE = 19,
		COL_NUM_EXT = 20,    COL_UNP_SIDE = 21,          COL_SIG_IMB = 22,
	};

	std::array<int64_t *,  32> p_i64 {};
	std::array<uint32_t *, 32> p_u32 {};
	std::array<uint16_t *, 32> p_u16 {};
	std::array<double *,   32> p_dbl {};
	std::array<string_t *, 32> p_str {};
	std::array<uint8_t *,  32> p_u8  {};
	D_ASSERT(projected_count <= 32);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT:       p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:        p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:          p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:            p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_REF_PRICE:      p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_AUCTION_TIME:   p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_CONT_CLR:       p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_AUCT_INT_CLR:   p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_SSR_FILL:       p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_IND_MATCH:      p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_UPPER_COLLAR:   p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_LOWER_COLLAR:   p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_PAIRED_QTY:     p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_TOTAL_IMB:      p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_MKT_IMB:        p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_UNPAIRED_QTY:   p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_AUC_TYPE:       p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_SIDE:           p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_AUC_STATUS:     p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_FREEZE:         p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_NUM_EXT:        p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_UNP_SIDE:       p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_SIG_IMB:        p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		default: break;
		}
	}

	databento::ImbalanceMsg rec {};
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Imbalance)) {
		if (!st.header_filter.Matches(rec.hd)) { continue; }
		st.NoteSymbol(n, rec.hd.instrument_id);
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT:      p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
			case COL_TS_RECV:       p_i64[i][n] = TsToInt64(rec.ts_recv); break;
			case COL_INSTR:         p_u32[i][n] = rec.hd.instrument_id; break;
			case COL_PUB:           p_u16[i][n] = rec.hd.publisher_id; break;
			case COL_REF_PRICE:     p_dbl[i][n] = static_cast<double>(rec.ref_price) / 1e9; break;
			case COL_AUCTION_TIME:  p_i64[i][n] = TsToInt64(rec.auction_time); break;
			case COL_CONT_CLR:      p_dbl[i][n] = static_cast<double>(rec.cont_book_clr_price) / 1e9; break;
			case COL_AUCT_INT_CLR:  p_dbl[i][n] = static_cast<double>(rec.auct_interest_clr_price) / 1e9; break;
			case COL_SSR_FILL:      p_dbl[i][n] = static_cast<double>(rec.ssr_filling_price) / 1e9; break;
			case COL_IND_MATCH:     p_dbl[i][n] = static_cast<double>(rec.ind_match_price) / 1e9; break;
			case COL_UPPER_COLLAR:  p_dbl[i][n] = static_cast<double>(rec.upper_collar) / 1e9; break;
			case COL_LOWER_COLLAR:  p_dbl[i][n] = static_cast<double>(rec.lower_collar) / 1e9; break;
			case COL_PAIRED_QTY:    p_u32[i][n] = rec.paired_qty; break;
			case COL_TOTAL_IMB:     p_u32[i][n] = rec.total_imbalance_qty; break;
			case COL_MKT_IMB:       p_u32[i][n] = rec.market_imbalance_qty; break;
			case COL_UNPAIRED_QTY:  p_u32[i][n] = rec.unpaired_qty; break;
			case COL_AUC_TYPE:      p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], rec.auction_type); break;
			case COL_SIDE:          p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.side)); break;
			case COL_AUC_STATUS:    p_u8[i][n]  = rec.auction_status; break;
			case COL_FREEZE:        p_u8[i][n]  = rec.freeze_status; break;
			case COL_NUM_EXT:       p_u8[i][n]  = rec.num_extensions; break;
			case COL_UNP_SIDE:      p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.unpaired_side)); break;
			case COL_SIG_IMB:       p_str[i][n] = duckdb_dbn::EmitChar1(out.data[i], rec.significant_imbalance); break;
			default: break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// statistics — RType::Statistics, StatMsg.
// Version-aware: v1/v2 StatMsg is 64 bytes with int32 quantity; v3 is 80 bytes
// with int64 quantity. Canonical exposed type is BIGINT — v1/v2 quantity is
// sign-extended, with the kUndefStatQuantityV1 sentinel (INT32_MAX) mapped
// to the v3 sentinel value (INT64_MAX).
// ════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> StatisticsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	bd->header_layout = DbnHeaderColumnLayout{0, 3, 4};
	names = {"ts_event",   "ts_recv",       "ts_ref",      "instrument_id", "publisher_id",
	         "price",      "quantity",      "sequence",    "ts_in_delta",   "stat_type",
	         "channel_id", "update_action", "stat_flags"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS,
	                LogicalType::UINTEGER,     LogicalType::USMALLINT,    LogicalType::DOUBLE,
	                LogicalType::BIGINT,       LogicalType::UINTEGER,     LogicalType::INTEGER,
	                LogicalType::USMALLINT,    LogicalType::USMALLINT,    LogicalType::UTINYINT,
	                LogicalType::UTINYINT};
	return std::move(bd);
}

static void StatisticsScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT      = 0,
		COL_TS_RECV       = 1,
		COL_TS_REF        = 2,
		COL_INSTR         = 3,
		COL_PUB           = 4,
		COL_PRICE         = 5,
		COL_QUANTITY      = 6,
		COL_SEQUENCE      = 7,
		COL_TS_IN_DELTA   = 8,
		COL_STAT_TYPE     = 9,
		COL_CHANNEL_ID    = 10,
		COL_UPDATE_ACTION = 11,
		COL_STAT_FLAGS    = 12,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<double *,   16> p_dbl {};
	std::array<int32_t *,  16> p_i32 {};
	std::array<uint8_t *,  16> p_u8  {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT:      p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_RECV:       p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_TS_REF:        p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:         p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:           p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_PRICE:         p_dbl[i] = FlatVector::GetData<double>(out.data[i]); break;
		case COL_QUANTITY:      p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_SEQUENCE:      p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_TS_IN_DELTA:   p_i32[i] = FlatVector::GetData<int32_t>(out.data[i]); break;
		case COL_STAT_TYPE:     p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_CHANNEL_ID:    p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_UPDATE_ACTION: p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_STAT_FLAGS:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		default: break;
		}
	}

	const auto version = st.reader->Version();
	const bool use_v1 = (version == 1 || version == 2);

	idx_t n = 0;
	if (use_v1) {
		databento::v1::StatMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Statistics)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			st.NoteSymbol(n, rec.hd.instrument_id);
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT:      p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
				case COL_TS_RECV:       p_i64[i][n] = TsToInt64(rec.ts_recv); break;
				case COL_TS_REF:        p_i64[i][n] = TsToInt64(rec.ts_ref); break;
				case COL_INSTR:         p_u32[i][n] = rec.hd.instrument_id; break;
				case COL_PUB:           p_u16[i][n] = rec.hd.publisher_id; break;
				case COL_PRICE:         p_dbl[i][n] = static_cast<double>(rec.price) / 1e9; break;
				case COL_QUANTITY:
					if (rec.quantity == std::numeric_limits<std::int32_t>::max()) {
						p_i64[i][n] = std::numeric_limits<std::int64_t>::max();
					} else {
						p_i64[i][n] = static_cast<int64_t>(rec.quantity);
					}
					break;
				case COL_SEQUENCE:      p_u32[i][n] = rec.sequence; break;
				case COL_TS_IN_DELTA:   p_i32[i][n] = static_cast<int32_t>(rec.ts_in_delta.count()); break;
				case COL_STAT_TYPE:     p_u16[i][n] = static_cast<uint16_t>(rec.stat_type); break;
				case COL_CHANNEL_ID:    p_u16[i][n] = rec.channel_id; break;
				case COL_UPDATE_ACTION: p_u8[i][n]  = static_cast<uint8_t>(rec.update_action); break;
				case COL_STAT_FLAGS:    p_u8[i][n]  = rec.stat_flags; break;
				default: break;
				}
			}
			++n;
		}
	} else {
		databento::StatMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::Statistics)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			st.NoteSymbol(n, rec.hd.instrument_id);
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT:      p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
				case COL_TS_RECV:       p_i64[i][n] = TsToInt64(rec.ts_recv); break;
				case COL_TS_REF:        p_i64[i][n] = TsToInt64(rec.ts_ref); break;
				case COL_INSTR:         p_u32[i][n] = rec.hd.instrument_id; break;
				case COL_PUB:           p_u16[i][n] = rec.hd.publisher_id; break;
				case COL_PRICE:         p_dbl[i][n] = static_cast<double>(rec.price) / 1e9; break;
				case COL_QUANTITY:      p_i64[i][n] = rec.quantity; break;
				case COL_SEQUENCE:      p_u32[i][n] = rec.sequence; break;
				case COL_TS_IN_DELTA:   p_i32[i][n] = static_cast<int32_t>(rec.ts_in_delta.count()); break;
				case COL_STAT_TYPE:     p_u16[i][n] = static_cast<uint16_t>(rec.stat_type); break;
				case COL_CHANNEL_ID:    p_u16[i][n] = rec.channel_id; break;
				case COL_UPDATE_ACTION: p_u8[i][n]  = static_cast<uint8_t>(rec.update_action); break;
				case COL_STAT_FLAGS:    p_u8[i][n]  = rec.stat_flags; break;
				default: break;
				}
			}
			++n;
		}
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// definition — RType::InstrumentDef, InstrumentDefMsg.
// Version-aware: v1 = 360 bytes, v2 = 400 bytes, v3 = 520 bytes. Canonical
// column layout is the union of all fields seen across versions. Fields not
// present in the source file's DBN version are emitted as SQL NULL — the
// 8 leg_* fields and a few v3 additions are NULL for v1/v2 sources; the
// 4 deprecated fields (trading_reference_price, trading_reference_date,
// md_security_trading_status, settl_price_type) are NULL for v3 sources.
// ════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> DefinitionBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	names = {"ts_event", "ts_recv", "instrument_id", "publisher_id",
	         "raw_symbol", "instrument_class", "security_type", "exchange",
	         "asset", "cfi", "currency", "settl_currency",
	         "secsubtype", "group", "underlying", "strike_price_currency",
	         "unit_of_measure", "expiration", "activation", "min_price_increment",
	         "display_factor", "high_limit_price", "low_limit_price", "max_price_variation",
	         "strike_price", "unit_of_measure_qty", "min_price_increment_amount", "price_ratio",
	         "raw_instrument_id", "underlying_id", "inst_attrib_value", "market_depth_implied",
	         "market_depth", "market_segment_id", "max_trade_vol", "min_lot_size",
	         "min_lot_size_block", "min_lot_size_round_lot", "min_trade_vol", "contract_multiplier",
	         "decay_quantity", "original_contract_size", "appl_id", "maturity_year",
	         "decay_start_date", "channel_id", "match_algorithm", "main_fraction",
	         "price_display_format", "sub_fraction", "underlying_product", "security_update_action",
	         "maturity_month", "maturity_day", "maturity_week", "user_defined_instrument",
	         "contract_multiplier_unit", "flow_schedule_type", "tick_rule", "leg_count",
	         "leg_index", "leg_instrument_id", "leg_raw_symbol", "leg_instrument_class",
	         "leg_side", "leg_price", "leg_delta", "leg_ratio_price_numerator",
	         "leg_ratio_price_denominator", "leg_ratio_qty_numerator", "leg_ratio_qty_denominator", "leg_underlying_id",
	         "trading_reference_price", "trading_reference_date", "md_security_trading_status", "settl_price_type"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,
	                LogicalType::USMALLINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP_NS,
	                LogicalType::TIMESTAMP_NS, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::UBIGINT, LogicalType::UINTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::INTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::UINTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::SMALLINT, LogicalType::USMALLINT, LogicalType::USMALLINT,
	                LogicalType::USMALLINT, LogicalType::VARCHAR, LogicalType::UTINYINT,
	                LogicalType::UTINYINT, LogicalType::UTINYINT, LogicalType::UTINYINT,
	                LogicalType::VARCHAR, LogicalType::UTINYINT, LogicalType::UTINYINT,
	                LogicalType::UTINYINT, LogicalType::VARCHAR, LogicalType::TINYINT,
	                LogicalType::TINYINT, LogicalType::UTINYINT, LogicalType::USMALLINT,
	                LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::UINTEGER,
	                LogicalType::DOUBLE, LogicalType::USMALLINT, LogicalType::UTINYINT,
	                LogicalType::UTINYINT};
	return std::move(bd);
}

static void DefinitionScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0, COL_TS_RECV = 1, COL_INSTR = 2, COL_PUB = 3,
		COL_RAW_SYMBOL = 4, COL_INSTRUMENT_CLASS = 5, COL_SECURITY_TYPE = 6, COL_EXCHANGE = 7,
		COL_ASSET = 8, COL_CFI = 9, COL_CURRENCY = 10, COL_SETTL_CURRENCY = 11,
		COL_SECSUBTYPE = 12, COL_GROUP = 13, COL_UNDERLYING = 14, COL_STRIKE_PRICE_CURRENCY = 15,
		COL_UNIT_OF_MEASURE = 16, COL_EXPIRATION = 17, COL_ACTIVATION = 18, COL_MIN_PRICE_INCREMENT = 19,
		COL_DISPLAY_FACTOR = 20, COL_HIGH_LIMIT_PRICE = 21, COL_LOW_LIMIT_PRICE = 22, COL_MAX_PRICE_VARIATION = 23,
		COL_STRIKE_PRICE = 24, COL_UNIT_OF_MEASURE_QTY = 25, COL_MIN_PRICE_INCREMENT_AMOUNT = 26, COL_PRICE_RATIO = 27,
		COL_RAW_INSTRUMENT_ID = 28, COL_UNDERLYING_ID = 29, COL_INST_ATTRIB_VALUE = 30, COL_MARKET_DEPTH_IMPLIED = 31,
		COL_MARKET_DEPTH = 32, COL_MARKET_SEGMENT_ID = 33, COL_MAX_TRADE_VOL = 34, COL_MIN_LOT_SIZE = 35,
		COL_MIN_LOT_SIZE_BLOCK = 36, COL_MIN_LOT_SIZE_ROUND_LOT = 37, COL_MIN_TRADE_VOL = 38, COL_CONTRACT_MULTIPLIER = 39,
		COL_DECAY_QUANTITY = 40, COL_ORIGINAL_CONTRACT_SIZE = 41, COL_APPL_ID = 42, COL_MATURITY_YEAR = 43,
		COL_DECAY_START_DATE = 44, COL_CHANNEL_ID = 45, COL_MATCH_ALGORITHM = 46, COL_MAIN_FRACTION = 47,
		COL_PRICE_DISPLAY_FORMAT = 48, COL_SUB_FRACTION = 49, COL_UNDERLYING_PRODUCT = 50, COL_SECURITY_UPDATE_ACTION = 51,
		COL_MATURITY_MONTH = 52, COL_MATURITY_DAY = 53, COL_MATURITY_WEEK = 54, COL_USER_DEFINED_INSTRUMENT = 55,
		COL_CONTRACT_MULTIPLIER_UNIT = 56, COL_FLOW_SCHEDULE_TYPE = 57, COL_TICK_RULE = 58, COL_LEG_COUNT = 59,
		COL_LEG_INDEX = 60, COL_LEG_INSTRUMENT_ID = 61, COL_LEG_RAW_SYMBOL = 62, COL_LEG_INSTRUMENT_CLASS = 63,
		COL_LEG_SIDE = 64, COL_LEG_PRICE = 65, COL_LEG_DELTA = 66, COL_LEG_RATIO_PRICE_NUMERATOR = 67,
		COL_LEG_RATIO_PRICE_DENOMINATOR = 68, COL_LEG_RATIO_QTY_NUMERATOR = 69, COL_LEG_RATIO_QTY_DENOMINATOR = 70, COL_LEG_UNDERLYING_ID = 71,
		COL_TRADING_REFERENCE_PRICE = 72, COL_TRADING_REFERENCE_DATE = 73, COL_MD_SECURITY_TRADING_STATUS = 74, COL_SETTL_PRICE_TYPE = 75,
	};

	const auto version = st.reader->Version();
	idx_t n = 0;

	if (version >= 3) {
		databento::InstrumentDefMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::InstrumentDef)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			st.NoteSymbol(n, rec.hd.instrument_id);
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT: FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.hd.ts_event).time_since_epoch().count()); break;
				case COL_TS_RECV:  FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.ts_recv).time_since_epoch().count()); break;
				case COL_INSTR:    FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.hd.instrument_id; break;
				case COL_PUB:      FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.hd.publisher_id; break;
				case COL_RAW_SYMBOL:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.RawSymbol(), 64); break;
				case COL_INSTRUMENT_CLASS:   FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.instrument_class)); break;
				case COL_SECURITY_TYPE:      FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SecurityType(), 7); break;
				case COL_EXCHANGE:           FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Exchange(), 5); break;
				case COL_ASSET:              FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Asset(), 11); break;
				case COL_CFI:                FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Cfi(), 7); break;
				case COL_CURRENCY:           FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Currency(), 4); break;
				case COL_SETTL_CURRENCY:     FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SettlCurrency(), 4); break;
				case COL_SECSUBTYPE:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SecSubType(), 6); break;
				case COL_GROUP:              FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Group(), 21); break;
				case COL_UNDERLYING:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Underlying(), 21); break;
				case COL_STRIKE_PRICE_CURRENCY: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.StrikePriceCurrency(), 4); break;
				case COL_UNIT_OF_MEASURE:    FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.UnitOfMeasure(), 31); break;
				case COL_EXPIRATION:         FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.expiration).time_since_epoch().count()); break;
				case COL_ACTIVATION:         FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.activation).time_since_epoch().count()); break;
				case COL_MIN_PRICE_INCREMENT: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.min_price_increment) / 1e9; break;
				case COL_DISPLAY_FACTOR:     FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.display_factor) / 1e9; break;
				case COL_HIGH_LIMIT_PRICE:   FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.high_limit_price) / 1e9; break;
				case COL_LOW_LIMIT_PRICE:    FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.low_limit_price) / 1e9; break;
				case COL_MAX_PRICE_VARIATION: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.max_price_variation) / 1e9; break;
				case COL_STRIKE_PRICE:       FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.strike_price) / 1e9; break;
				case COL_UNIT_OF_MEASURE_QTY: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.unit_of_measure_qty) / 1e9; break;
				case COL_MIN_PRICE_INCREMENT_AMOUNT: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.min_price_increment_amount) / 1e9; break;
				case COL_PRICE_RATIO:        FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.price_ratio) / 1e9; break;
				case COL_RAW_INSTRUMENT_ID:  FlatVector::GetData<uint64_t>(out.data[i])[n] = rec.raw_instrument_id; break;
				case COL_UNDERLYING_ID:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.underlying_id; break;
				case COL_INST_ATTRIB_VALUE:  FlatVector::GetData<int32_t>(out.data[i])[n] = rec.inst_attrib_value; break;
				case COL_MARKET_DEPTH_IMPLIED: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.market_depth_implied; break;
				case COL_MARKET_DEPTH:       FlatVector::GetData<int32_t>(out.data[i])[n] = rec.market_depth; break;
				case COL_MARKET_SEGMENT_ID:  FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.market_segment_id; break;
				case COL_MAX_TRADE_VOL:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.max_trade_vol; break;
				case COL_MIN_LOT_SIZE:       FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size; break;
				case COL_MIN_LOT_SIZE_BLOCK: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size_block; break;
				case COL_MIN_LOT_SIZE_ROUND_LOT: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size_round_lot; break;
				case COL_MIN_TRADE_VOL:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.min_trade_vol; break;
				case COL_CONTRACT_MULTIPLIER: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.contract_multiplier; break;
				case COL_DECAY_QUANTITY:     FlatVector::GetData<int32_t>(out.data[i])[n] = rec.decay_quantity; break;
				case COL_ORIGINAL_CONTRACT_SIZE: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.original_contract_size; break;
				case COL_APPL_ID:            FlatVector::GetData<int16_t>(out.data[i])[n] = rec.appl_id; break;
				case COL_MATURITY_YEAR:      FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.maturity_year; break;
				case COL_DECAY_START_DATE:   FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.decay_start_date; break;
				case COL_CHANNEL_ID:         FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.channel_id; break;
				case COL_MATCH_ALGORITHM:    FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.match_algorithm)); break;
				case COL_MAIN_FRACTION:      FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.main_fraction; break;
				case COL_PRICE_DISPLAY_FORMAT: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.price_display_format; break;
				case COL_SUB_FRACTION:       FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.sub_fraction; break;
				case COL_UNDERLYING_PRODUCT: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.underlying_product; break;
				case COL_SECURITY_UPDATE_ACTION: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.security_update_action)); break;
				case COL_MATURITY_MONTH:     FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_month; break;
				case COL_MATURITY_DAY:       FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_day; break;
				case COL_MATURITY_WEEK:      FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_week; break;
				case COL_USER_DEFINED_INSTRUMENT: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.user_defined_instrument)); break;
				case COL_CONTRACT_MULTIPLIER_UNIT: FlatVector::GetData<int8_t>(out.data[i])[n] = static_cast<int8_t>(rec.contract_multiplier_unit); break;
				case COL_FLOW_SCHEDULE_TYPE: FlatVector::GetData<int8_t>(out.data[i])[n] = static_cast<int8_t>(rec.flow_schedule_type); break;
				case COL_TICK_RULE:          FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.tick_rule; break;
				case COL_LEG_COUNT:          FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.leg_count; break;
				case COL_LEG_INDEX:          FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.leg_index; break;
				case COL_LEG_INSTRUMENT_ID:  FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.leg_instrument_id; break;
				case COL_LEG_RAW_SYMBOL:     FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.LegRawSymbol(), 64); break;
				case COL_LEG_INSTRUMENT_CLASS: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.leg_instrument_class)); break;
				case COL_LEG_SIDE:           FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.leg_side)); break;
				case COL_LEG_PRICE:          FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.leg_price) / 1e9; break;
				case COL_LEG_DELTA:          FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.leg_delta) / 1e9; break;
				case COL_LEG_RATIO_PRICE_NUMERATOR:   FlatVector::GetData<int32_t>(out.data[i])[n] = rec.leg_ratio_price_numerator; break;
				case COL_LEG_RATIO_PRICE_DENOMINATOR: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.leg_ratio_price_denominator; break;
				case COL_LEG_RATIO_QTY_NUMERATOR:     FlatVector::GetData<int32_t>(out.data[i])[n] = rec.leg_ratio_qty_numerator; break;
				case COL_LEG_RATIO_QTY_DENOMINATOR:   FlatVector::GetData<int32_t>(out.data[i])[n] = rec.leg_ratio_qty_denominator; break;
				case COL_LEG_UNDERLYING_ID:           FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.leg_underlying_id; break;
				case COL_TRADING_REFERENCE_PRICE:
				case COL_TRADING_REFERENCE_DATE:
				case COL_MD_SECURITY_TRADING_STATUS:
				case COL_SETTL_PRICE_TYPE:
					FlatVector::Validity(out.data[i]).SetInvalid(n); break;
				default: break;
				}
			}
			++n;
		}
	} else if (version == 2) {
		databento::v2::InstrumentDefMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::InstrumentDef)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			st.NoteSymbol(n, rec.hd.instrument_id);
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT: FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.hd.ts_event).time_since_epoch().count()); break;
				case COL_TS_RECV:  FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.ts_recv).time_since_epoch().count()); break;
				case COL_INSTR:    FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.hd.instrument_id; break;
				case COL_PUB:      FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.hd.publisher_id; break;
				case COL_RAW_SYMBOL:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.RawSymbol(), 64); break;
				case COL_INSTRUMENT_CLASS:   FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.instrument_class)); break;
				case COL_SECURITY_TYPE:      FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SecurityType(), 7); break;
				case COL_EXCHANGE:           FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Exchange(), 5); break;
				case COL_ASSET:              FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Asset(), 11); break;
				case COL_CFI:                FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Cfi(), 7); break;
				case COL_CURRENCY:           FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Currency(), 4); break;
				case COL_SETTL_CURRENCY:     FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SettlCurrency(), 4); break;
				case COL_SECSUBTYPE:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SecSubType(), 6); break;
				case COL_GROUP:              FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Group(), 21); break;
				case COL_UNDERLYING:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Underlying(), 21); break;
				case COL_STRIKE_PRICE_CURRENCY: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.StrikePriceCurrency(), 4); break;
				case COL_UNIT_OF_MEASURE:    FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.UnitOfMeasure(), 31); break;
				case COL_EXPIRATION:         FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.expiration).time_since_epoch().count()); break;
				case COL_ACTIVATION:         FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.activation).time_since_epoch().count()); break;
				case COL_MIN_PRICE_INCREMENT: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.min_price_increment) / 1e9; break;
				case COL_DISPLAY_FACTOR:     FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.display_factor) / 1e9; break;
				case COL_HIGH_LIMIT_PRICE:   FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.high_limit_price) / 1e9; break;
				case COL_LOW_LIMIT_PRICE:    FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.low_limit_price) / 1e9; break;
				case COL_MAX_PRICE_VARIATION: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.max_price_variation) / 1e9; break;
				case COL_STRIKE_PRICE:       FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.strike_price) / 1e9; break;
				case COL_UNIT_OF_MEASURE_QTY: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.unit_of_measure_qty) / 1e9; break;
				case COL_MIN_PRICE_INCREMENT_AMOUNT: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.min_price_increment_amount) / 1e9; break;
				case COL_PRICE_RATIO:        FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.price_ratio) / 1e9; break;
				case COL_RAW_INSTRUMENT_ID:  FlatVector::GetData<uint64_t>(out.data[i])[n] = static_cast<uint64_t>(rec.raw_instrument_id); break;
				case COL_UNDERLYING_ID:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.underlying_id; break;
				case COL_INST_ATTRIB_VALUE:  FlatVector::GetData<int32_t>(out.data[i])[n] = rec.inst_attrib_value; break;
				case COL_MARKET_DEPTH_IMPLIED: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.market_depth_implied; break;
				case COL_MARKET_DEPTH:       FlatVector::GetData<int32_t>(out.data[i])[n] = rec.market_depth; break;
				case COL_MARKET_SEGMENT_ID:  FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.market_segment_id; break;
				case COL_MAX_TRADE_VOL:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.max_trade_vol; break;
				case COL_MIN_LOT_SIZE:       FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size; break;
				case COL_MIN_LOT_SIZE_BLOCK: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size_block; break;
				case COL_MIN_LOT_SIZE_ROUND_LOT: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size_round_lot; break;
				case COL_MIN_TRADE_VOL:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.min_trade_vol; break;
				case COL_CONTRACT_MULTIPLIER: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.contract_multiplier; break;
				case COL_DECAY_QUANTITY:     FlatVector::GetData<int32_t>(out.data[i])[n] = rec.decay_quantity; break;
				case COL_ORIGINAL_CONTRACT_SIZE: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.original_contract_size; break;
				case COL_APPL_ID:            FlatVector::GetData<int16_t>(out.data[i])[n] = rec.appl_id; break;
				case COL_MATURITY_YEAR:      FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.maturity_year; break;
				case COL_DECAY_START_DATE:   FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.decay_start_date; break;
				case COL_CHANNEL_ID:         FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.channel_id; break;
				case COL_MATCH_ALGORITHM:    FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.match_algorithm)); break;
				case COL_MAIN_FRACTION:      FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.main_fraction; break;
				case COL_PRICE_DISPLAY_FORMAT: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.price_display_format; break;
				case COL_SUB_FRACTION:       FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.sub_fraction; break;
				case COL_UNDERLYING_PRODUCT: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.underlying_product; break;
				case COL_SECURITY_UPDATE_ACTION: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.security_update_action)); break;
				case COL_MATURITY_MONTH:     FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_month; break;
				case COL_MATURITY_DAY:       FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_day; break;
				case COL_MATURITY_WEEK:      FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_week; break;
				case COL_USER_DEFINED_INSTRUMENT: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.user_defined_instrument)); break;
				case COL_CONTRACT_MULTIPLIER_UNIT: FlatVector::GetData<int8_t>(out.data[i])[n] = static_cast<int8_t>(rec.contract_multiplier_unit); break;
				case COL_FLOW_SCHEDULE_TYPE: FlatVector::GetData<int8_t>(out.data[i])[n] = static_cast<int8_t>(rec.flow_schedule_type); break;
				case COL_TICK_RULE:          FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.tick_rule; break;
				case COL_LEG_COUNT:
				case COL_LEG_INDEX:
				case COL_LEG_INSTRUMENT_ID:
				case COL_LEG_RAW_SYMBOL:
				case COL_LEG_INSTRUMENT_CLASS:
				case COL_LEG_SIDE:
				case COL_LEG_PRICE:
				case COL_LEG_DELTA:
				case COL_LEG_RATIO_PRICE_NUMERATOR:
				case COL_LEG_RATIO_PRICE_DENOMINATOR:
				case COL_LEG_RATIO_QTY_NUMERATOR:
				case COL_LEG_RATIO_QTY_DENOMINATOR:
				case COL_LEG_UNDERLYING_ID:
					FlatVector::Validity(out.data[i]).SetInvalid(n); break;
				case COL_TRADING_REFERENCE_PRICE: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.trading_reference_price) / 1e9; break;
				case COL_TRADING_REFERENCE_DATE:  FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.trading_reference_date; break;
				case COL_MD_SECURITY_TRADING_STATUS: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.md_security_trading_status; break;
				case COL_SETTL_PRICE_TYPE:        FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.settl_price_type; break;
				default: break;
				}
			}
			++n;
		}
	} else {
		databento::v1::InstrumentDefMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::InstrumentDef)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			st.NoteSymbol(n, rec.hd.instrument_id);
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT: FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.hd.ts_event).time_since_epoch().count()); break;
				case COL_TS_RECV:  FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.ts_recv).time_since_epoch().count()); break;
				case COL_INSTR:    FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.hd.instrument_id; break;
				case COL_PUB:      FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.hd.publisher_id; break;
				case COL_RAW_SYMBOL:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.RawSymbol(), 64); break;
				case COL_INSTRUMENT_CLASS:   FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.instrument_class)); break;
				case COL_SECURITY_TYPE:      FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SecurityType(), 7); break;
				case COL_EXCHANGE:           FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Exchange(), 5); break;
				case COL_ASSET:              FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Asset(), 11); break;
				case COL_CFI:                FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Cfi(), 7); break;
				case COL_CURRENCY:           FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Currency(), 4); break;
				case COL_SETTL_CURRENCY:     FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SettlCurrency(), 4); break;
				case COL_SECSUBTYPE:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.SecSubType(), 6); break;
				case COL_GROUP:              FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Group(), 21); break;
				case COL_UNDERLYING:         FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.Underlying(), 21); break;
				case COL_STRIKE_PRICE_CURRENCY: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.StrikePriceCurrency(), 4); break;
				case COL_UNIT_OF_MEASURE:    FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitCstr(out.data[i], rec.UnitOfMeasure(), 31); break;
				case COL_EXPIRATION:         FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.expiration).time_since_epoch().count()); break;
				case COL_ACTIVATION:         FlatVector::GetData<int64_t>(out.data[i])[n] = static_cast<int64_t>((rec.activation).time_since_epoch().count()); break;
				case COL_MIN_PRICE_INCREMENT: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.min_price_increment) / 1e9; break;
				case COL_DISPLAY_FACTOR:     FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.display_factor) / 1e9; break;
				case COL_HIGH_LIMIT_PRICE:   FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.high_limit_price) / 1e9; break;
				case COL_LOW_LIMIT_PRICE:    FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.low_limit_price) / 1e9; break;
				case COL_MAX_PRICE_VARIATION: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.max_price_variation) / 1e9; break;
				case COL_STRIKE_PRICE:       FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.strike_price) / 1e9; break;
				case COL_UNIT_OF_MEASURE_QTY: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.unit_of_measure_qty) / 1e9; break;
				case COL_MIN_PRICE_INCREMENT_AMOUNT: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.min_price_increment_amount) / 1e9; break;
				case COL_PRICE_RATIO:        FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.price_ratio) / 1e9; break;
				case COL_RAW_INSTRUMENT_ID:  FlatVector::GetData<uint64_t>(out.data[i])[n] = static_cast<uint64_t>(rec.raw_instrument_id); break;
				case COL_UNDERLYING_ID:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.underlying_id; break;
				case COL_INST_ATTRIB_VALUE:  FlatVector::GetData<int32_t>(out.data[i])[n] = rec.inst_attrib_value; break;
				case COL_MARKET_DEPTH_IMPLIED: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.market_depth_implied; break;
				case COL_MARKET_DEPTH:       FlatVector::GetData<int32_t>(out.data[i])[n] = rec.market_depth; break;
				case COL_MARKET_SEGMENT_ID:  FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.market_segment_id; break;
				case COL_MAX_TRADE_VOL:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.max_trade_vol; break;
				case COL_MIN_LOT_SIZE:       FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size; break;
				case COL_MIN_LOT_SIZE_BLOCK: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size_block; break;
				case COL_MIN_LOT_SIZE_ROUND_LOT: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.min_lot_size_round_lot; break;
				case COL_MIN_TRADE_VOL:      FlatVector::GetData<uint32_t>(out.data[i])[n] = rec.min_trade_vol; break;
				case COL_CONTRACT_MULTIPLIER: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.contract_multiplier; break;
				case COL_DECAY_QUANTITY:     FlatVector::GetData<int32_t>(out.data[i])[n] = rec.decay_quantity; break;
				case COL_ORIGINAL_CONTRACT_SIZE: FlatVector::GetData<int32_t>(out.data[i])[n] = rec.original_contract_size; break;
				case COL_APPL_ID:            FlatVector::GetData<int16_t>(out.data[i])[n] = rec.appl_id; break;
				case COL_MATURITY_YEAR:      FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.maturity_year; break;
				case COL_DECAY_START_DATE:   FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.decay_start_date; break;
				case COL_CHANNEL_ID:         FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.channel_id; break;
				case COL_MATCH_ALGORITHM:    FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.match_algorithm)); break;
				case COL_MAIN_FRACTION:      FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.main_fraction; break;
				case COL_PRICE_DISPLAY_FORMAT: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.price_display_format; break;
				case COL_SUB_FRACTION:       FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.sub_fraction; break;
				case COL_UNDERLYING_PRODUCT: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.underlying_product; break;
				case COL_SECURITY_UPDATE_ACTION: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.security_update_action)); break;
				case COL_MATURITY_MONTH:     FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_month; break;
				case COL_MATURITY_DAY:       FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_day; break;
				case COL_MATURITY_WEEK:      FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.maturity_week; break;
				case COL_USER_DEFINED_INSTRUMENT: FlatVector::GetData<string_t>(out.data[i])[n] = duckdb_dbn::EmitChar1(out.data[i], static_cast<char>(rec.user_defined_instrument)); break;
				case COL_CONTRACT_MULTIPLIER_UNIT: FlatVector::GetData<int8_t>(out.data[i])[n] = static_cast<int8_t>(rec.contract_multiplier_unit); break;
				case COL_FLOW_SCHEDULE_TYPE: FlatVector::GetData<int8_t>(out.data[i])[n] = static_cast<int8_t>(rec.flow_schedule_type); break;
				case COL_TICK_RULE:          FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.tick_rule; break;
				case COL_LEG_COUNT:
				case COL_LEG_INDEX:
				case COL_LEG_INSTRUMENT_ID:
				case COL_LEG_RAW_SYMBOL:
				case COL_LEG_INSTRUMENT_CLASS:
				case COL_LEG_SIDE:
				case COL_LEG_PRICE:
				case COL_LEG_DELTA:
				case COL_LEG_RATIO_PRICE_NUMERATOR:
				case COL_LEG_RATIO_PRICE_DENOMINATOR:
				case COL_LEG_RATIO_QTY_NUMERATOR:
				case COL_LEG_RATIO_QTY_DENOMINATOR:
				case COL_LEG_UNDERLYING_ID:
					FlatVector::Validity(out.data[i]).SetInvalid(n); break;
				case COL_TRADING_REFERENCE_PRICE: FlatVector::GetData<double>(out.data[i])[n] = static_cast<double>(rec.trading_reference_price) / 1e9; break;
				case COL_TRADING_REFERENCE_DATE:  FlatVector::GetData<uint16_t>(out.data[i])[n] = rec.trading_reference_date; break;
				case COL_MD_SECURITY_TRADING_STATUS: FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.md_security_trading_status; break;
				case COL_SETTL_PRICE_TYPE:        FlatVector::GetData<uint8_t>(out.data[i])[n] = rec.settl_price_type; break;
				default: break;
				}
			}
			++n;
		}
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// Polymorphic read_dbn(path) — probes metadata.schema at bind time and
// dispatches to the right per-schema bind/scan pair.
// ════════════════════════════════════════════════════════════════════════════

struct SchemaHandler {
	const char *display_name;
	table_function_bind_t bind;
	table_function_t scan;
};

static const SchemaHandler *LookupSchemaHandler(databento::Schema s) {
	switch (s) {
	case databento::Schema::Trades: {
		static const SchemaHandler h = {"trades", TradesBind, TradesScan};
		return &h;
	}
	case databento::Schema::Mbo: {
		static const SchemaHandler h = {"mbo", MboBind, MboScan};
		return &h;
	}
	case databento::Schema::Mbp1: {
		static const SchemaHandler h = {"mbp-1", Mbp1Bind, Mbp1Scan};
		return &h;
	}
	case databento::Schema::Mbp10: {
		static const SchemaHandler h = {"mbp-10", Mbp10Bind, Mbp10Scan};
		return &h;
	}
	case databento::Schema::Tbbo: {
		static const SchemaHandler h = {"tbbo", TbboBind, TbboScan};
		return &h;
	}
	case databento::Schema::Bbo1S: {
		static const SchemaHandler h = {"bbo-1s", BboBind, Bbo1sScan};
		return &h;
	}
	case databento::Schema::Bbo1M: {
		static const SchemaHandler h = {"bbo-1m", BboBind, Bbo1mScan};
		return &h;
	}
	case databento::Schema::Cbbo1S: {
		static const SchemaHandler h = {"cbbo-1s", CbboBind, Cbbo1sScan};
		return &h;
	}
	case databento::Schema::Cmbp1: {
		static const SchemaHandler h = {"cmbp-1", Cmbp1Bind, Cmbp1Scan};
		return &h;
	}
	case databento::Schema::Ohlcv1S: {
		static const SchemaHandler h = {"ohlcv-1s", OhlcvBind, Ohlcv1sScan};
		return &h;
	}
	case databento::Schema::Ohlcv1M: {
		static const SchemaHandler h = {"ohlcv-1m", OhlcvBind, Ohlcv1mScan};
		return &h;
	}
	case databento::Schema::Ohlcv1H: {
		static const SchemaHandler h = {"ohlcv-1h", OhlcvBind, Ohlcv1hScan};
		return &h;
	}
	case databento::Schema::Ohlcv1D: {
		static const SchemaHandler h = {"ohlcv-1d", OhlcvBind, Ohlcv1dScan};
		return &h;
	}
	case databento::Schema::Status: {
		static const SchemaHandler h = {"status", StatusBind, StatusScan};
		return &h;
	}
	case databento::Schema::Imbalance: {
		static const SchemaHandler h = {"imbalance", ImbalanceBind, ImbalanceScan};
		return &h;
	}
	case databento::Schema::Statistics: {
		static const SchemaHandler h = {"statistics", StatisticsBind, StatisticsScan};
		return &h;
	}
	case databento::Schema::Definition: {
		static const SchemaHandler h = {"definition", DefinitionBind, DefinitionScan};
		return &h;
	}
	case databento::Schema::Cbbo1M: {
		static const SchemaHandler h = {"cbbo-1m", CbboBind, Cbbo1mScan};
		return &h;
	}
	case databento::Schema::Tcbbo: {
		static const SchemaHandler h = {"tcbbo", Cmbp1Bind, TcbboScan};
		return &h;
	}
	case databento::Schema::OhlcvEod: {
		static const SchemaHandler h = {"ohlcv-eod", OhlcvBind, OhlcvEodScan};
		return &h;
	}
	default:
		return nullptr;
	}
}

static const char *SchemaToCstr(databento::Schema s) {
	switch (s) {
	case databento::Schema::Mbo:        return "mbo";
	case databento::Schema::Mbp1:       return "mbp-1";
	case databento::Schema::Mbp10:      return "mbp-10";
	case databento::Schema::Tbbo:       return "tbbo";
	case databento::Schema::Trades:     return "trades";
	case databento::Schema::Ohlcv1S:    return "ohlcv-1s";
	case databento::Schema::Ohlcv1M:    return "ohlcv-1m";
	case databento::Schema::Ohlcv1H:    return "ohlcv-1h";
	case databento::Schema::Ohlcv1D:    return "ohlcv-1d";
	case databento::Schema::Definition: return "definition";
	case databento::Schema::Statistics: return "statistics";
	case databento::Schema::Status:     return "status";
	case databento::Schema::Imbalance:  return "imbalance";
	case databento::Schema::OhlcvEod:   return "ohlcv-eod";
	case databento::Schema::Cmbp1:      return "cmbp-1";
	case databento::Schema::Cbbo1S:     return "cbbo-1s";
	case databento::Schema::Cbbo1M:     return "cbbo-1m";
	case databento::Schema::Bbo1S:      return "bbo-1s";
	case databento::Schema::Bbo1M:      return "bbo-1m";
	default:                            return "unknown";
	}
}

static const char *STypeToCstr(databento::SType s) {
	switch (s) {
	case databento::SType::InstrumentId:  return "instrument_id";
	case databento::SType::RawSymbol:     return "raw_symbol";
	case databento::SType::Smart:         return "smart";
	case databento::SType::Continuous:    return "continuous";
	case databento::SType::Parent:        return "parent";
	case databento::SType::NasdaqSymbol:  return "nasdaq_symbol";
	case databento::SType::CmsSymbol:     return "cms_symbol";
	case databento::SType::Isin:          return "isin";
	case databento::SType::UsCode:        return "us_code";
	case databento::SType::BbgCompId:     return "bbg_comp_id";
	case databento::SType::BbgCompTicker: return "bbg_comp_ticker";
	case databento::SType::Figi:          return "figi";
	case databento::SType::FigiTicker:    return "figi_ticker";
	default:                              return "unknown";
	}
}

static const char *SystemCodeToCstr(databento::SystemCode c) {
	switch (c) {
	case databento::SystemCode::Heartbeat:         return "heartbeat";
	case databento::SystemCode::SubscriptionAck:   return "subscription_ack";
	case databento::SystemCode::SlowReaderWarning: return "slow_reader_warning";
	case databento::SystemCode::ReplayCompleted:   return "replay_completed";
	case databento::SystemCode::EndOfInterval:     return "end_of_interval";
	case databento::SystemCode::Unset:             return "unset";
	default:                                       return "unknown";
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// symbol_mapping — SymbolMappingMsg, RType::SymbolMapping (0x16).
//   v2/v3 (176 B): hd + stype_in + stype_in_symbol[71] + stype_out +
//                  stype_out_symbol[71] + start_ts + end_ts
//   v1     (80 B): hd + stype_in_symbol[22] + stype_out_symbol[22] +
//                  _dummy[4] + start_ts + end_ts (no stype_in/stype_out enums)
// Live-session record interleaved alongside market data; carries the
// raw-symbol ↔ instrument_id binding that metadata.symbols is missing
// for live captures. Layout: kHeaderLayoutOhlcv (no ts_recv).
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> SymbolMappingBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	bd->header_layout = kHeaderLayoutOhlcv;
	names = {"ts_event", "instrument_id", "publisher_id",
	         "stype_in", "stype_in_symbol",
	         "stype_out", "stype_out_symbol",
	         "start_ts", "end_ts"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER,    LogicalType::USMALLINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS};
	return std::move(bd);
}

static void SymbolMappingScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0,  COL_INSTR = 1,           COL_PUB = 2,
		COL_STYPE_IN = 3,  COL_STYPE_IN_SYM = 4,
		COL_STYPE_OUT = 5, COL_STYPE_OUT_SYM = 6,
		COL_START_TS = 7,  COL_END_TS = 8,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<string_t *, 16> p_str {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT:      p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:         p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:           p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_STYPE_IN:      p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_STYPE_IN_SYM:  p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_STYPE_OUT:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_STYPE_OUT_SYM: p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_START_TS:      p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_END_TS:        p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		default: break;
		}
	}

	const auto version = st.reader->Version();
	idx_t n = 0;
	if (version >= 2) {
		databento::SymbolMappingMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::SymbolMapping)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT:      p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
				case COL_INSTR:         p_u32[i][n] = rec.hd.instrument_id; break;
				case COL_PUB:           p_u16[i][n] = rec.hd.publisher_id; break;
				case COL_STYPE_IN:
					// 0xFF is Databento's "unset" sentinel — live captures use it
					// because the writer doesn't always know the stype.
					if (static_cast<uint8_t>(rec.stype_in) == 0xFF) {
						FlatVector::Validity(out.data[i]).SetInvalid(n);
					} else {
						p_str[i][n] = StringVector::AddString(out.data[i], STypeToCstr(rec.stype_in));
					}
					break;
				case COL_STYPE_IN_SYM:  p_str[i][n] = duckdb_dbn::EmitCstr(out.data[i], rec.stype_in_symbol.data(), rec.stype_in_symbol.size()); break;
				case COL_STYPE_OUT:
					if (static_cast<uint8_t>(rec.stype_out) == 0xFF) {
						FlatVector::Validity(out.data[i]).SetInvalid(n);
					} else {
						p_str[i][n] = StringVector::AddString(out.data[i], STypeToCstr(rec.stype_out));
					}
					break;
				case COL_STYPE_OUT_SYM: p_str[i][n] = duckdb_dbn::EmitCstr(out.data[i], rec.stype_out_symbol.data(), rec.stype_out_symbol.size()); break;
				case COL_START_TS:      p_i64[i][n] = TsToInt64(rec.start_ts); break;
				case COL_END_TS:        p_i64[i][n] = TsToInt64(rec.end_ts); break;
				default: break;
				}
			}
			++n;
		}
	} else {
		databento::v1::SymbolMappingMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::SymbolMapping)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT:      p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
				case COL_INSTR:         p_u32[i][n] = rec.hd.instrument_id; break;
				case COL_PUB:           p_u16[i][n] = rec.hd.publisher_id; break;
				case COL_STYPE_IN:      FlatVector::Validity(out.data[i]).SetInvalid(n); break;
				case COL_STYPE_IN_SYM:  p_str[i][n] = duckdb_dbn::EmitCstr(out.data[i], rec.stype_in_symbol.data(), rec.stype_in_symbol.size()); break;
				case COL_STYPE_OUT:     FlatVector::Validity(out.data[i]).SetInvalid(n); break;
				case COL_STYPE_OUT_SYM: p_str[i][n] = duckdb_dbn::EmitCstr(out.data[i], rec.stype_out_symbol.data(), rec.stype_out_symbol.size()); break;
				case COL_START_TS:      p_i64[i][n] = TsToInt64(rec.start_ts); break;
				case COL_END_TS:        p_i64[i][n] = TsToInt64(rec.end_ts); break;
				default: break;
				}
			}
			++n;
		}
	}
	out.SetCardinality(n);
}

// ═════════════════════════════════════════════════════════════════════════════
// system — SystemMsg, RType::System (0x17).
//   v2/v3 (320 B): hd + msg[303] + code (SystemCode enum)
//   v1     (80 B): hd + msg[64] (no code; code emitted as SQL NULL)
// Gateway session messages: heartbeat, subscription_ack, slow_reader_warning,
// replay_completed, end_of_interval. Layout: kHeaderLayoutOhlcv.
// ═════════════════════════════════════════════════════════════════════════════

static unique_ptr<FunctionData> SystemBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<ReadDbnBindData>();
	bd->file_paths = ExpandPaths(context, GetFilePathArg(input));
	bd->header_layout = kHeaderLayoutOhlcv;
	names = {"ts_event", "instrument_id", "publisher_id", "msg", "code"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::UINTEGER, LogicalType::USMALLINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR};
	return std::move(bd);
}

static void SystemScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0, COL_INSTR = 1, COL_PUB = 2,
		COL_MSG = 3,      COL_CODE = 4,
	};

	std::array<int64_t *,  16> p_i64 {};
	std::array<uint32_t *, 16> p_u32 {};
	std::array<uint16_t *, 16> p_u16 {};
	std::array<string_t *, 16> p_str {};
	D_ASSERT(projected_count <= 16);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_MSG:      p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		case COL_CODE:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		default: break;
		}
	}

	const auto version = st.reader->Version();
	idx_t n = 0;
	if (version >= 2) {
		databento::SystemMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::System)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT: p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
				case COL_INSTR:    p_u32[i][n] = rec.hd.instrument_id; break;
				case COL_PUB:      p_u16[i][n] = rec.hd.publisher_id; break;
				case COL_MSG:      p_str[i][n] = duckdb_dbn::EmitCstr(out.data[i], rec.msg.data(), rec.msg.size()); break;
				case COL_CODE:
					// SystemCode::Unset (255) means the field wasn't set by the
					// writer — semantically equivalent to NULL.
					if (rec.code == databento::SystemCode::Unset) {
						FlatVector::Validity(out.data[i]).SetInvalid(n);
					} else {
						p_str[i][n] = StringVector::AddString(out.data[i], SystemCodeToCstr(rec.code));
					}
					break;
				default: break;
				}
			}
			++n;
		}
	} else {
		databento::v1::SystemMsg rec {};
		while (n < STANDARD_VECTOR_SIZE && st.reader->NextAs(rec, databento::RType::System)) {
			if (!st.header_filter.Matches(rec.hd)) { continue; }
			for (idx_t i = 0; i < projected_count; ++i) {
				const auto cid = col_ids[i];
				switch (cid) {
				case COL_TS_EVENT: p_i64[i][n] = TsToInt64(rec.hd.ts_event); break;
				case COL_INSTR:    p_u32[i][n] = rec.hd.instrument_id; break;
				case COL_PUB:      p_u16[i][n] = rec.hd.publisher_id; break;
				case COL_MSG:      p_str[i][n] = duckdb_dbn::EmitCstr(out.data[i], rec.msg.data(), rec.msg.size()); break;
				case COL_CODE:     FlatVector::Validity(out.data[i]).SetInvalid(n); break;
				default: break;
				}
			}
			++n;
		}
	}
	out.SetCardinality(n);
}

// Append the opt-in `symbol` VARCHAR column when symbols := true was passed.
// Shared by the per-schema bind wrapper (BindWithSymbols) and the polymorphic
// read_dbn bind. The column is appended last, so its logical id equals the
// prior column count. No-op when the parameter is absent/false.
static void MaybeAppendSymbolColumn(TableFunctionBindInput &input, ReadDbnBindData &bd,
                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto entry = input.named_parameters.find("symbols");
	if (entry != input.named_parameters.end() && !entry->second.IsNull() &&
	    BooleanValue::Get(entry->second)) {
		bd.with_symbols = true;
		bd.symbol_col_id = names.size();
		names.emplace_back("symbol");
		return_types.emplace_back(LogicalType::VARCHAR);
	}
}

struct ReadDbnBindDataPolymorphic : public ReadDbnBindData {
	table_function_t dispatched_scan = nullptr;
};

static unique_ptr<FunctionData> ReadDbnBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	const auto pattern = GetFilePathArg(input);
	auto paths = ExpandPaths(context, pattern);
	duckdb_dbn::DbnFileReader probe(paths.front());
	const auto &md = probe.GetMetadata();
	if (!md.schema.has_value()) {
		throw InvalidInputException("dbn: file '%s' has no schema in metadata (mixed-schema file); "
		                            "polymorphic read_dbn() can't dispatch — use a specific "
		                            "read_dbn_<schema>() function instead.",
		                            paths.front());
	}
	const auto *handler = LookupSchemaHandler(*md.schema);
	if (handler == nullptr) {
		throw InvalidInputException(
		    "dbn: file '%s' has schema '%s', which the polymorphic read_dbn() dispatcher "
		    "does not yet recognize. File a bug.",
		    paths.front(), SchemaToCstr(*md.schema));
	}
	(void)handler->bind(context, input, return_types, names);
	auto bd = make_uniq<ReadDbnBindDataPolymorphic>();
	bd->file_paths = std::move(paths);
	switch (*md.schema) {
	case databento::Schema::Ohlcv1S:
	case databento::Schema::Ohlcv1M:
	case databento::Schema::Ohlcv1H:
	case databento::Schema::Ohlcv1D:
	case databento::Schema::OhlcvEod:
		bd->header_layout = kHeaderLayoutOhlcv;
		break;
	case databento::Schema::Statistics:
		bd->header_layout = DbnHeaderColumnLayout{0, 3, 4};
		break;
	default:
		bd->header_layout = kHeaderLayoutStandard;
		break;
	}
	bd->dispatched_scan = handler->scan;
	// Opt-in symbol resolution. The dispatched per-schema scan already
	// snapshots symbols via NoteSymbol; here we just surface the column and
	// flag the bind data so ReadDbnInitGlobal enables tracking.
	MaybeAppendSymbolColumn(input, *bd, return_types, names);
	return std::move(bd);
}

static void ReadDbnScan(ClientContext &ctx, TableFunctionInput &input, DataChunk &out) {
	auto &bd = input.bind_data->Cast<ReadDbnBindDataPolymorphic>();
	bd.dispatched_scan(ctx, input, out);
}

// ════════════════════════════════════════════════════════════════════════════
// dbn_metadata(path) — single-row introspection helper.
// ════════════════════════════════════════════════════════════════════════════

struct DbnMetadataBindData : public TableFunctionData {
	std::string file_path;
};

struct DbnMetadataGlobalState : public GlobalTableFunctionState {
	bool emitted = false;
};

static unique_ptr<FunctionData> DbnMetadataBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<DbnMetadataBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"version",  "dataset",  "schema",   "start_ts", "end_ts",
	         "limit",    "stype_in", "stype_out","ts_out",   "symbol_cstr_len"};
	return_types = {LogicalType::UTINYINT,    LogicalType::VARCHAR,      LogicalType::VARCHAR,
	                LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_NS, LogicalType::UBIGINT,
	                LogicalType::VARCHAR,      LogicalType::VARCHAR,      LogicalType::BOOLEAN,
	                LogicalType::UINTEGER};
	return std::move(bd);
}

static unique_ptr<GlobalTableFunctionState> DbnMetadataInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<DbnMetadataGlobalState>();
}

static void DbnMetadataScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &state = input.global_state->Cast<DbnMetadataGlobalState>();
	if (state.emitted) {
		out.SetCardinality(0);
		return;
	}
	state.emitted = true;

	auto &bd = input.bind_data->Cast<DbnMetadataBindData>();
	duckdb_dbn::DbnFileReader reader(bd.file_path);
	const auto &md = reader.GetMetadata();

	FlatVector::GetData<uint8_t>(out.data[0])[0] = md.version;
	FlatVector::GetData<string_t>(out.data[1])[0] =
	    StringVector::AddString(out.data[1], md.dataset.data(), md.dataset.size());
	if (md.schema.has_value()) {
		const char *s = SchemaToCstr(*md.schema);
		FlatVector::GetData<string_t>(out.data[2])[0] = StringVector::AddString(out.data[2], s);
	} else {
		FlatVector::Validity(out.data[2]).SetInvalid(0);
	}
	FlatVector::GetData<int64_t>(out.data[3])[0] = md.start_ns;
	FlatVector::GetData<int64_t>(out.data[4])[0] = md.end_ns;
	FlatVector::GetData<uint64_t>(out.data[5])[0] = md.limit;
	if (md.stype_in.has_value()) {
		const char *s = STypeToCstr(*md.stype_in);
		FlatVector::GetData<string_t>(out.data[6])[0] = StringVector::AddString(out.data[6], s);
	} else {
		FlatVector::Validity(out.data[6]).SetInvalid(0);
	}
	{
		const char *s = STypeToCstr(md.stype_out);
		FlatVector::GetData<string_t>(out.data[7])[0] = StringVector::AddString(out.data[7], s);
	}
	FlatVector::GetData<bool>(out.data[8])[0] = md.ts_out;
	FlatVector::GetData<uint32_t>(out.data[9])[0] = static_cast<uint32_t>(md.symbol_cstr_len);
	out.SetCardinality(1);
}

// ════════════════════════════════════════════════════════════════════════════
// dbn_records(path) — raw rtype-agnostic view, for mixed-schema files where
// metadata.schema is NULL (system messages mixed with data records, etc.).
// Single-file (no glob). Emits header fields + record body as BLOB.
// ════════════════════════════════════════════════════════════════════════════

struct DbnRecordsBindData : public TableFunctionData {
	std::string file_path;
};

struct DbnRecordsGlobalState : public GlobalTableFunctionState {
	DbnRecordsGlobalState(const std::string &path,
	                      std::vector<column_t> column_ids_p,
	                      DbnHeaderFilter header_filter_p)
	    : reader(std::make_unique<duckdb_dbn::DbnFileReader>(path)),
	      column_ids(std::move(column_ids_p)),
	      header_filter(std::move(header_filter_p)) {}
	std::unique_ptr<duckdb_dbn::DbnFileReader> reader;
	std::vector<column_t> column_ids;
	DbnHeaderFilter header_filter;
	// Pushed-down filter set + cached body-filter Expression
	// (see ReadDbnGlobalState for the lazy-init pattern).
	unique_ptr<TableFilterSet> filters;
	unique_ptr<Expression> body_filter_expr;
	bool body_filter_built = false;
};

static unique_ptr<FunctionData> DbnRecordsBind(ClientContext &, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bd = make_uniq<DbnRecordsBindData>();
	bd->file_path = GetFilePathArg(input);
	names = {"ts_event", "rtype", "length", "publisher_id", "instrument_id", "body"};
	return_types = {LogicalType::TIMESTAMP_NS, LogicalType::UTINYINT,  LogicalType::UTINYINT,
	                LogicalType::USMALLINT,    LogicalType::UINTEGER,  LogicalType::BLOB};
	return std::move(bd);
}

static unique_ptr<GlobalTableFunctionState> DbnRecordsInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto &bd = input.bind_data->Cast<DbnRecordsBindData>();
	std::vector<column_t> col_ids = input.column_ids;
	auto hf = BuildHeaderFilter(input.filters, col_ids, kHeaderLayoutRecords);
	auto state = make_uniq<DbnRecordsGlobalState>(bd.file_path, std::move(col_ids), std::move(hf));
	if (input.filters) {
		state->filters = input.filters->Copy();
	}
	return std::move(state);
}

static void DbnRecordsScan(ClientContext &, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<DbnRecordsGlobalState>();
	const auto &col_ids = st.column_ids;
	const idx_t projected_count = col_ids.size();
	D_ASSERT(out.data.size() == projected_count);

	enum : column_t {
		COL_TS_EVENT = 0,
		COL_RTYPE    = 1,
		COL_LENGTH   = 2,
		COL_PUB      = 3,
		COL_INSTR    = 4,
		COL_BODY     = 5,
	};

	std::array<int64_t *,  8> p_i64 {};
	std::array<uint8_t *,  8> p_u8  {};
	std::array<uint16_t *, 8> p_u16 {};
	std::array<uint32_t *, 8> p_u32 {};
	std::array<string_t *, 8> p_str {};
	D_ASSERT(projected_count <= 8);

	for (idx_t i = 0; i < projected_count; ++i) {
		const auto cid = col_ids[i];
		if (cid == COLUMN_IDENTIFIER_ROW_ID) { continue; }
		switch (cid) {
		case COL_TS_EVENT: p_i64[i] = FlatVector::GetData<int64_t>(out.data[i]); break;
		case COL_RTYPE:    p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_LENGTH:   p_u8[i]  = FlatVector::GetData<uint8_t>(out.data[i]); break;
		case COL_PUB:      p_u16[i] = FlatVector::GetData<uint16_t>(out.data[i]); break;
		case COL_INSTR:    p_u32[i] = FlatVector::GetData<uint32_t>(out.data[i]); break;
		case COL_BODY:     p_str[i] = FlatVector::GetData<string_t>(out.data[i]); break;
		default: break;
		}
	}

	alignas(8) std::byte buf[duckdb_dbn::DbnFileReader::kMaxRecordLen];
	databento::RecordHeader hdr {};
	std::size_t rec_len = 0;
	idx_t n = 0;
	while (n < STANDARD_VECTOR_SIZE && st.reader->NextRecordRaw(buf, &hdr, &rec_len, nullptr)) {
		if (!st.header_filter.Matches(hdr)) { continue; }
		for (idx_t i = 0; i < projected_count; ++i) {
			const auto cid = col_ids[i];
			switch (cid) {
			case COL_TS_EVENT: p_i64[i][n] = static_cast<int64_t>(hdr.ts_event.time_since_epoch().count()); break;
			case COL_RTYPE:    p_u8[i][n]  = static_cast<uint8_t>(hdr.rtype); break;
			case COL_LENGTH:   p_u8[i][n]  = hdr.length; break;
			case COL_PUB:      p_u16[i][n] = hdr.publisher_id; break;
			case COL_INSTR:    p_u32[i][n] = hdr.instrument_id; break;
			case COL_BODY:     p_str[i][n] = StringVector::AddStringOrBlob(out.data[i], reinterpret_cast<const char *>(buf), rec_len); break;
			default: break;
			}
		}
		++n;
	}
	out.SetCardinality(n);
}

// ════════════════════════════════════════════════════════════════════════════
// Replacement scan — `SELECT * FROM 'foo.dbn'` rewrites to read_dbn('foo.dbn').
// `.dbn.zst` is NOT matched here: DuckDB's built-in parse-time path intercepts
// any `.zst` suffix and routes it through the parquet extension before any
// replacement scan runs. For compressed files, call read_dbn('foo.dbn.zst')
// directly — Phase 3-B's ZstdInput handles decompression transparently.
// ════════════════════════════════════════════════════════════════════════════

static unique_ptr<TableRef> ReadDbnReplacement(ClientContext &, ReplacementScanInput &input,
                                               optional_ptr<ReplacementScanData>) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!StringUtil::EndsWith(table_name, ".dbn")) {
		return nullptr;
	}
	auto fn = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	fn->function = make_uniq<FunctionExpression>("read_dbn", std::move(children));
	return std::move(fn);
}

// ════════════════════════════════════════════════════════════════════════════
// Registration
// ════════════════════════════════════════════════════════════════════════════

// Wraps an inner per-schema scan with the body-column filter pass. The inner
// scan emits records that survive DbnHeaderFilter's ts_event/instrument_id/
// publisher_id pre-screen during decode; this wrapper then evaluates every
// remaining (body-column) TableFilter against the chunk via
// ExpressionExecutor. With this in place, filter_pushdown=true is safe —
// header filters get the per-record early-skip fast path and body filters
// still take effect (no silent drop).
// Materialize the appended `symbol` column from the per-row snapshots the scan
// stashed in sym_scratch. No-op when the column was projected away. Must run
// after the inner scan (which fills sym_scratch) and before the body-filter
// pass (so `WHERE symbol = '…'` sees populated values). Rows whose snapshot is
// null — no mapping seen for that instrument — emit SQL NULL.
static void FillSymbolColumn(ReadDbnGlobalState &st, DataChunk &out) {
	idx_t slot = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < st.column_ids.size(); ++i) {
		if (st.column_ids[i] == st.symbol_col_id) {
			slot = i;
			break;
		}
	}
	if (slot == DConstants::INVALID_INDEX) {
		return; // symbol column not projected
	}
	auto &vec = out.data[slot];
	auto data = FlatVector::GetData<string_t>(vec);
	auto &validity = FlatVector::Validity(vec);
	const idx_t count = out.size();
	for (idx_t n = 0; n < count; ++n) {
		const std::string *s = st.sym_scratch[n];
		if (s) {
			data[n] = StringVector::AddString(vec, *s);
		} else {
			validity.SetInvalid(n);
		}
	}
}

template <table_function_t Inner>
static void ScanWithBodyFilter(ClientContext &c, TableFunctionInput &input, DataChunk &out) {
	auto &st = input.global_state->Cast<ReadDbnGlobalState>();
	// Reset per-chunk snapshots so a row the inner scan fails to annotate
	// degrades to SQL NULL rather than reusing a stale pointer from the
	// previous chunk.
	if (st.with_symbols) {
		st.sym_scratch.fill(nullptr);
	}
	Inner(c, input, out);
	if (out.size() == 0) {
		return;
	}
	if (st.with_symbols) {
		FillSymbolColumn(st, out);
	}
	if (!st.filters) {
		return;
	}
	// Build the Expression once on the first chunk that has data (need a
	// populated DataChunk for projected column types), then reuse.
	if (!st.body_filter_built) {
		auto &bd = input.bind_data->Cast<ReadDbnBindData>();
		st.body_filter_expr = BuildBodyFilterExpr(*st.filters, st.column_ids,
		                                          bd.header_layout, out);
		st.body_filter_built = true;
	}
	if (st.body_filter_expr) {
		EvalBodyFilterExpr(c, *st.body_filter_expr, out);
	}
}

template <table_function_t Inner>
static void Register(ExtensionLoader &loader, const char *name,
                     table_function_bind_t bind) {
	TableFunction f(name, {LogicalType::VARCHAR}, ScanWithBodyFilter<Inner>, bind, ReadDbnInitGlobal);
	f.projection_pushdown = true;
	f.filter_pushdown = true;
	loader.RegisterFunction(f);
}

// Bind wrapper that adds opt-in symbol resolution to a market-data reader.
// Delegates to the schema's own bind, then — if symbols := true — appends a
// `symbol` VARCHAR column and records the flag on the bind data. Keeps the
// per-schema binds untouched.
template <table_function_bind_t Bind>
static unique_ptr<FunctionData> BindWithSymbols(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto fd = Bind(context, input, return_types, names);
	MaybeAppendSymbolColumn(input, fd->Cast<ReadDbnBindData>(), return_types, names);
	return fd;
}

// Like Register, but also exposes the `symbols` named parameter; passing
// symbols := true resolves each record's instrument_id to its live raw symbol
// (an extra `symbol` column) in a single in-order pass — see DbnFileReader's
// symbol-tracking notes.
template <table_function_t Inner, table_function_bind_t Bind>
static void RegisterWithSymbols(ExtensionLoader &loader, const char *name) {
	TableFunction f(name, {LogicalType::VARCHAR}, ScanWithBodyFilter<Inner>, BindWithSymbols<Bind>,
	                ReadDbnInitGlobal);
	f.projection_pushdown = true;
	f.filter_pushdown = true;
	f.named_parameters["symbols"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(f);
}

static void LoadInternal(ExtensionLoader &loader) {
	// read_dbn dispatches to a per-schema scan chosen from metadata.schema; it
	// supports symbols := true the same way (the dispatched scan resolves), so
	// it gets the named parameter directly rather than via RegisterWithSymbols
	// (whose bind wrapper is templated on a single fixed schema bind).
	{
		TableFunction f("read_dbn", {LogicalType::VARCHAR}, ScanWithBodyFilter<ReadDbnScan>, ReadDbnBind,
		                ReadDbnInitGlobal);
		f.projection_pushdown = true;
		f.filter_pushdown = true;
		f.named_parameters["symbols"] = LogicalType::BOOLEAN;
		loader.RegisterFunction(f);
	}
	RegisterWithSymbols<TradesScan, TradesBind>(loader, "read_dbn_trades");
	RegisterWithSymbols<MboScan, MboBind>(loader, "read_dbn_mbo");
	RegisterWithSymbols<Mbp1Scan, Mbp1Bind>(loader, "read_dbn_mbp1");
	RegisterWithSymbols<Mbp10Scan, Mbp10Bind>(loader, "read_dbn_mbp10");
	RegisterWithSymbols<Bbo1sScan, BboBind>(loader, "read_dbn_bbo_1s");
	RegisterWithSymbols<Bbo1mScan, BboBind>(loader, "read_dbn_bbo_1m");
	RegisterWithSymbols<Cbbo1sScan, CbboBind>(loader, "read_dbn_cbbo_1s");
	RegisterWithSymbols<Cmbp1Scan, Cmbp1Bind>(loader, "read_dbn_cmbp1");
	RegisterWithSymbols<TbboScan, TbboBind>(loader, "read_dbn_tbbo");
	RegisterWithSymbols<Ohlcv1sScan, OhlcvBind>(loader, "read_dbn_ohlcv_1s");
	RegisterWithSymbols<Ohlcv1mScan, OhlcvBind>(loader, "read_dbn_ohlcv_1m");
	RegisterWithSymbols<Ohlcv1hScan, OhlcvBind>(loader, "read_dbn_ohlcv_1h");
	RegisterWithSymbols<Ohlcv1dScan, OhlcvBind>(loader, "read_dbn_ohlcv_1d");
	RegisterWithSymbols<StatusScan, StatusBind>(loader, "read_dbn_status");
	RegisterWithSymbols<ImbalanceScan, ImbalanceBind>(loader, "read_dbn_imbalance");
	RegisterWithSymbols<StatisticsScan, StatisticsBind>(loader, "read_dbn_statistics");
	RegisterWithSymbols<DefinitionScan, DefinitionBind>(loader, "read_dbn_definition");
	RegisterWithSymbols<Cbbo1mScan, CbboBind>(loader, "read_dbn_cbbo_1m");
	RegisterWithSymbols<OhlcvEodScan, OhlcvBind>(loader, "read_dbn_ohlcv_eod");
	RegisterWithSymbols<TcbboScan, Cmbp1Bind>(loader, "read_dbn_tcbbo");
	Register<SymbolMappingScan>(loader, "read_dbn_symbol_mapping", SymbolMappingBind);
	Register<SystemScan>(loader, "read_dbn_system", SystemBind);

	TableFunction mf("dbn_metadata", {LogicalType::VARCHAR}, DbnMetadataScan, DbnMetadataBind, DbnMetadataInitGlobal);
	loader.RegisterFunction(mf);

	// dbn_records uses its own bind/global state and a hardcoded Records layout
	// {ts_event=0, instrument=4, publisher=3}, so it gets its own wrapper.
	auto records_scan = +[](ClientContext &c, TableFunctionInput &input, DataChunk &out) {
		DbnRecordsScan(c, input, out);
		if (out.size() == 0) {
			return;
		}
		auto &st = input.global_state->Cast<DbnRecordsGlobalState>();
		if (!st.filters) {
			return;
		}
		if (!st.body_filter_built) {
			st.body_filter_expr = BuildBodyFilterExpr(*st.filters, st.column_ids,
			                                          kHeaderLayoutRecords, out);
			st.body_filter_built = true;
		}
		if (st.body_filter_expr) {
			EvalBodyFilterExpr(c, *st.body_filter_expr, out);
		}
	};
	TableFunction rf("dbn_records", {LogicalType::VARCHAR}, records_scan, DbnRecordsBind, DbnRecordsInitGlobal);
	rf.projection_pushdown = true;
	rf.filter_pushdown = true;
	loader.RegisterFunction(rf);

	DBConfig::GetConfig(loader.GetDatabaseInstance()).replacement_scans.emplace_back(ReadDbnReplacement);
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
