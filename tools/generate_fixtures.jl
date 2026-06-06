"""
Generate the small set of DBN fixtures that aren't available upstream:

    test_data.cbbo-1m.dbn      (4 CBBO1mMsg records, distinct instrument_ids)
    test_data.tcbbo.dbn        (4 TCBBOMsg records, distinct instrument_ids)
    test_data.ohlcv-eod.dbn    (2 OHLCVMsg records, rtype byte-patched to 0x24)
    test_data.trades.empty.dbn (header only, zero records)

Idempotent: rerun to regenerate. Output goes under test/data/ relative to the
duckdb-dbn repo root (this script lives in duckdb-dbn/tools/).

Run with Julia 1.12 from the DBN.jl repo (writer is upstream from this script):

    julia +release --project=C:/Users/tbeas/Documents/GitHub/DatabentoBinaryEncoding.jl \\
        C:/Users/tbeas/Documents/GitHub/duckdb-dbn/tools/generate_fixtures.jl

Notes on Schema-enum drift: DBN.jl 0.1.3's Schema enum is misaligned with the
canonical Databento Schema enum in the CBBO range. Reverse-engineered from
the upstream fixtures (test_data.*.dbn bytes at offset 24):

      schema name (dbn-cli)    canonical byte    DBN.jl Schema enum value
      ---------------------    --------------    ------------------------
      Mbo .. Status            0..11             matches
      Imbalance                12                12 (matches)
      OhlcvEod                 13                — (absent; DBN.jl has CBBO=13)
      Cmbp1                    14                — (absent; DBN.jl has CBBO_1S=14)
      Cbbo1S                   15                CBBO_1M=15  (off by one)
      Cbbo1M                   16                — (absent)
      Tcbbo                    17                17 (matches)
      Bbo1S, Bbo1M             18, 19            matches

Consequence: when we want Cbbo1M (canonical byte 16) or OhlcvEod (canonical
byte 13), we write with the closest DBN.jl alias and patch the schema byte
post-write. Record rtypes are handled the same way for OhlcvEod (0x24).
Layout offsets are derived from write_header in DBN.jl/src/encode.jl.
"""

using DatabentoBinaryEncoding
const DBN = DatabentoBinaryEncoding
using DatabentoBinaryEncoding: RecordHeader, BidAskPair, Metadata,
    TradeMsg, OHLCVMsg, CBBO1mMsg, TCBBOMsg, SymbolMappingMsg,
    RType, Schema, SType, Action, Side, write_dbn

const REPO = dirname(@__DIR__)
const DATA = joinpath(REPO, "test", "data")

# Anchor timestamps to the same epoch range used by upstream fixtures so any
# cross-fixture queries (joins, unions) compose cleanly. From inspecting
# test_data.cbbo-1s.dbn: start_ts=1609160400000000000 (2020-12-28 13:00 UTC).
const T0 = 1609160400000000000
const DATASET = "GLBX.MDP3"
const PUB_ID = UInt16(1)

base_metadata(schema; symbols=["ESH1"]) = Metadata(
    UInt8(3),                       # version
    DATASET,
    schema,
    T0,                             # start_ts
    T0 + 39_600_000_000_000,        # end_ts (= 1609200000000000000)
    nothing,                        # limit
    SType.RAW_SYMBOL, SType.RAW_SYMBOL,
    false,                          # ts_out
    symbols, String[], String[],
    Tuple{String,String,Int64,Int64}[],
)

# CBBO1m: 11 fields, length unit = sizeof(CBBO1mMsg)/4. Same shape as MBP1Msg.
# sizeof(RecordHeader)=16 + 8(price) + 4(size) + 1(action) + 1(side) + 1(flags)
# + 1(depth) + 8(ts_recv) + 4(ts_in_delta) + 4(sequence) + 24(BidAskPair...)
# Let DBN.jl compute the length via sizeof.
function len_units(::Type{T}) where {T}
    UInt8(div(sizeof(T), 4))
end

function gen_cbbo_1m()
    rectype = CBBO1mMsg
    L = len_units(rectype)
    records = CBBO1mMsg[]
    for (i, iid) in enumerate(UInt32[5482, 6001, 6002, 6003])
        ts = T0 + (i-1) * 60_000_000_000  # 1-minute spacing
        rec = CBBO1mMsg(
            RecordHeader(L, RType.CBBO_1M_MSG, PUB_ID, iid, ts),
            Int64(3720_500_000_000 + (i-1) * 250_000_000),  # price
            UInt32(10 + i),                                  # size
            Action.NONE, Side.NONE,
            UInt8(128), UInt8(0),                            # flags, depth
            ts + 100_000,                                    # ts_recv
            Int32(50_000),                                   # ts_in_delta
            UInt32(1_000_000 + i),                           # sequence
            BidAskPair(
                Int64(3720_250_000_000),  # bid_px
                Int64(3720_500_000_000),  # ask_px
                UInt32(24), UInt32(11), UInt32(1), UInt32(1),
            ),
        )
        push!(records, rec)
    end
    # DBN.jl's Schema.CBBO_1M actually writes byte 15 (= Cbbo1S canonically);
    # write with the closest alias and patch the schema byte to 16.
    md = base_metadata(Schema.CBBO_1M)
    out_path = joinpath(DATA, "test_data.cbbo-1m.dbn")
    write_dbn(out_path, md, records)
    patch_schema_byte!(out_path, CANONICAL_SCHEMA_CBBO_1M)
    println("  ok   test_data.cbbo-1m.dbn (", length(records),
            " records, schema patched to Cbbo1M)")
end

function gen_tcbbo()
    rectype = TCBBOMsg
    L = len_units(rectype)
    records = TCBBOMsg[]
    for (i, iid) in enumerate(UInt32[5482, 6001, 6002, 6003])
        ts = T0 + (i-1) * 1_000_000_000
        rec = TCBBOMsg(
            RecordHeader(L, RType.TCBBO_MSG, PUB_ID, iid, ts),
            Int64(3720_250_000_000 + (i-1) * 125_000_000),
            UInt32(20 + i),
            Action.TRADE, Side.ASK,
            UInt8(129), UInt8(0),
            ts + 250_000,
            Int32(15_000),
            UInt32(2_000_000 + i),
            BidAskPair(
                Int64(3720_250_000_000),
                Int64(3720_500_000_000),
                UInt32(26), UInt32(7), UInt32(16), UInt32(6),
            ),
        )
        push!(records, rec)
    end
    md = base_metadata(Schema.TCBBO)
    write_dbn(joinpath(DATA, "test_data.tcbbo.dbn"), md, records)
    println("  ok   test_data.tcbbo.dbn (", length(records), " records)")
end

function gen_trades_empty()
    md = base_metadata(Schema.TRADES)
    write_dbn(joinpath(DATA, "test_data.trades.empty.dbn"), md, TradeMsg[])
    println("  ok   test_data.trades.empty.dbn (0 records)")
end

# ---------------------------------------------------------------------------
# ohlcv-eod: write as ohlcv-1d, then patch the schema byte (file offset 24)
# and the per-record rtype byte (offset 1 within each record).
#
# DBN v3 file layout (from DBN.jl/src/encode.jl write_header):
#   0..2  "DBN" magic
#   3     version (UInt8)
#   4..7  metadata_length (UInt32 LE)
#   8..   metadata block:
#     8..23   dataset (16 bytes, null-padded)
#     24..25  schema (UInt16 LE)   <-- patch site
#     26..   start_ts, end_ts, limit, stype_in, stype_out, ts_out, ...
#
# OHLCV_EOD constants from the official DBN spec:
# Canonical Databento Schema enum values (see header docstring).
const CANONICAL_SCHEMA_OHLCV_EOD = UInt16(13)
const CANONICAL_SCHEMA_CBBO_1M   = UInt16(16)
const OHLCV_EOD_RTYPE_VAL        = UInt8(0x24)

"""Overwrite the 2-byte UInt16 LE schema field at file offset 24."""
function patch_schema_byte!(path::String, schema_val::UInt16)
    bytes = read(path)
    @assert bytes[1:3] == b"DBN" "expected DBN magic at offset 0"
    bytes[25] = UInt8(schema_val & 0xFF)
    bytes[26] = UInt8((schema_val >> 8) & 0xFF)
    write(path, bytes)
end

function gen_ohlcv_eod()
    rectype = OHLCVMsg
    L = len_units(rectype)
    records = OHLCVMsg[]
    for (i, iid) in enumerate(UInt32[5482, 6001])
        ts = T0 + (i-1) * 86_400_000_000_000  # 1-day spacing
        rec = OHLCVMsg(
            RecordHeader(L, RType.OHLCV_1D_MSG, PUB_ID, iid, ts),
            Int64(3700_000_000_000 + (i-1) * 1_000_000_000),  # open
            Int64(3725_000_000_000 + (i-1) * 1_000_000_000),  # high
            Int64(3695_000_000_000 + (i-1) * 1_000_000_000),  # low
            Int64(3720_000_000_000 + (i-1) * 1_000_000_000),  # close
            UInt64(125_000 + i * 1000),                        # volume
        )
        push!(records, rec)
    end
    md = base_metadata(Schema.OHLCV_1D)
    out_path = joinpath(DATA, "test_data.ohlcv-eod.dbn")
    write_dbn(out_path, md, records)

    # Patch schema byte to canonical OhlcvEod (13).
    patch_schema_byte!(out_path, CANONICAL_SCHEMA_OHLCV_EOD)

    # Patch per-record rtype from OHLCV_1D_MSG (0x23) to OhlcvEod (0x24).
    bytes = read(out_path)
    @assert bytes[4] == 0x03 "expected v3"
    md_len = UInt32(bytes[5]) | (UInt32(bytes[6]) << 8) |
             (UInt32(bytes[7]) << 16) | (UInt32(bytes[8]) << 24)
    body_start = 8 + Int(md_len)  # 0-based byte offset of first record
    rec_size = Int(L) * 4
    for i in 0:length(records)-1
        rtype_idx = body_start + i*rec_size + 2  # +1 for length byte, +1 for 1-based
        @assert bytes[rtype_idx] == UInt8(RType.OHLCV_1D_MSG) "rtype byte at wrong offset"
        bytes[rtype_idx] = OHLCV_EOD_RTYPE_VAL
    end
    write(out_path, bytes)
    println("  ok   test_data.ohlcv-eod.dbn (", length(records),
            " records, schema/rtype patched to OHLCV_EOD)")
end

# ---------------------------------------------------------------------------
# symbol_mapping fixture: 3 SymbolMappingMsg records over a small ESH1 →
# 5482 mapping. DBN.jl's SymbolMappingMsg encoder writes the canonical
# v2/v3 wire layout (1 + 71 + 1 + 71 + 8 + 8 = 160 body bytes), so this is
# a straight write_dbn call. Includes one record with stype=0xFF (the
# "unset" sentinel live captures use) to test the NULL emission path.

function gen_symbol_mapping()
    # Canonical v2/v3 SymbolMappingMsg = 176 bytes = 44 four-byte units. We
    # can't use len_units(SymbolMappingMsg) because the Julia struct holds
    # `String` references (variable size), so sizeof() returns 16, not 176.
    L = UInt8(44)
    records = SymbolMappingMsg[
        # Two records with valid stype enums.
        SymbolMappingMsg(
            RecordHeader(L, RType.SYMBOL_MAPPING_MSG, PUB_ID, UInt32(5482), T0),
            SType.RAW_SYMBOL, "ESH1",
            SType.INSTRUMENT_ID, "5482",
            T0, T0 + 86_400_000_000_000,
        ),
        SymbolMappingMsg(
            RecordHeader(L, RType.SYMBOL_MAPPING_MSG, PUB_ID, UInt32(6001), T0 + 1_000_000_000),
            SType.PARENT, "ES.FUT",
            SType.INSTRUMENT_ID, "6001",
            T0, T0 + 86_400_000_000_000,
        ),
        # One record with stype set to the 0xFF "unset" sentinel — exercises
        # the NULL emission path in the reader.
        SymbolMappingMsg(
            RecordHeader(L, RType.SYMBOL_MAPPING_MSG, PUB_ID, UInt32(6002), T0 + 2_000_000_000),
            reinterpret(SType.T, UInt8(0xFF)), "SPX.OPT",
            reinterpret(SType.T, UInt8(0xFF)), "SPX   271217C02800000",
            T0, T0 + 86_400_000_000_000,
        ),
    ]
    md = base_metadata(Schema.MBO)  # No SymbolMapping schema enum; metadata.schema is informational
    write_dbn(joinpath(DATA, "test_data.symbol_mapping.dbn"), md, records)
    println("  ok   test_data.symbol_mapping.dbn (", length(records), " records)")
end

# ---------------------------------------------------------------------------
# system fixture: hand-write raw bytes since DBN.jl's SystemMsg encoder
# uses a non-canonical layout (msg + NUL + code + NUL padded). Canonical
# v2/v3 SystemMsg is hd(16) + msg[303] + code(1) = 320 bytes. Two records:
# one heartbeat (code=0), one subscription_ack (code=1).

const SYSTEM_RTYPE = UInt8(0x17)         # RType::System
const SYSTEM_REC_BYTES = 320              # canonical v2/v3 record size
const SYSTEM_REC_LEN_UNITS = UInt8(SYSTEM_REC_BYTES ÷ 4)

function write_canonical_system_record(io::IO, instrument_id::UInt32, ts_event::Int64,
                                       msg::String, code::UInt8)
    # RecordHeader (16 bytes): length(1) | rtype(1) | publisher_id(2 LE) |
    # instrument_id(4 LE) | ts_event(8 LE)
    write(io, SYSTEM_REC_LEN_UNITS)
    write(io, SYSTEM_RTYPE)
    write(io, htol(PUB_ID))
    write(io, htol(instrument_id))
    write(io, htol(ts_event))
    # msg[303]: write up to 302 chars + NUL terminator, pad zeros.
    msg_bytes = Vector{UInt8}(msg)
    copy_len = min(length(msg_bytes), 302)
    write(io, view(msg_bytes, 1:copy_len))
    write(io, zeros(UInt8, 303 - copy_len))
    # code(1): the SystemCode enum byte.
    write(io, code)
end

function gen_system()
    metadata = base_metadata(Schema.MBO)
    out_path = joinpath(DATA, "test_data.system.dbn")
    # Write header via DBN.jl's encoder (headers are canonical), then patch
    # the records by reading the file back and overwriting the body.
    write_dbn(out_path, metadata, TradeMsg[])  # header-only with zero records
    bytes = read(out_path)
    @assert bytes[1:3] == b"DBN"
    md_len = UInt32(bytes[5]) | (UInt32(bytes[6]) << 8) |
             (UInt32(bytes[7]) << 16) | (UInt32(bytes[8]) << 24)
    body_start = 8 + Int(md_len)
    @assert body_start == length(bytes) "expected header-only file"
    # Append two canonical SystemMsg records.
    open(out_path, "a") do io
        write_canonical_system_record(io, UInt32(0), T0,
                                      "Heartbeat", UInt8(0))                        # SystemCode::Heartbeat
        write_canonical_system_record(io, UInt32(0), T0 + 1_000_000_000,
                                      "Subscription acknowledged", UInt8(1))         # SystemCode::SubscriptionAck
    end
    println("  ok   test_data.system.dbn (2 records, raw canonical layout)")
end

function main()
    isdir(DATA) || error("data dir does not exist: $DATA")
    gen_trades_empty()
    gen_cbbo_1m()
    gen_tcbbo()
    gen_ohlcv_eod()
    gen_symbol_mapping()
    gen_system()
    return 0
end

main()
