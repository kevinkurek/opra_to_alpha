use anyhow::{anyhow, Result};
use chrono::{SecondsFormat, TimeZone, Utc};
use pcap_parser::{PcapBlock, PcapCapture, Capture};
use rayon::{prelude::*, ThreadPoolBuilder};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const ETHERNET_HEADER_LEN: usize = 14;
const VLAN_HEADER_LEN: usize = 4;
const IPV4_MIN_HEADER_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;

const ETHERTYPE_OFFSET: usize = 12;
const VLAN_INNER_ETHERTYPE_OFFSET: usize = 16;
const IPV4_PROTOCOL_FIELD_OFFSET: usize = 9;
const UDP_LENGTH_FIELD_OFFSET: usize = 4;

const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_VLAN_8021Q: u16 = 0x8100;
const ETHERTYPE_VLAN_8021AD: u16 = 0x88a8;
const IP_PROTOCOL_UDP: u8 = 17;

const OPRA_BLOCK_HEADER_LEN: usize = 21;
const OPRA_BLOCK_SIZE_OFFSET: usize = 1;
const OPRA_MESSAGES_IN_BLOCK_OFFSET: usize = 10;
const OPRA_DATA_FEED_INDICATOR_OFFSET: usize = 3;
const OPRA_DATA_FEED_INDICATOR: u8 = b'O';

#[derive(Debug, Default)]
pub struct DecodeStats {
    pub packets: u64,
    pub messages: u64,
}

#[derive(Debug, Clone)]
pub struct HeaderOpraRow {
    pub packet_index: u64,
    pub udp_payload_len: u64,
    pub block_size: u64,
    pub messages_in_block: u64,
}

#[derive(Debug, Clone)]
pub struct DecodedTradeRow {
    pub packet_index: u64,
    pub block_sequence: u64,
    pub block_timestamp_ns: u64,
    pub block_timestamp_utc: String,
    pub message_index_in_block: u64,
    pub participant: String,
    pub category: String,
    pub type_code: String,
    pub indicator: String,
    pub symbol_root: Option<String>,
    pub osi_symbol: Option<String>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_size: Option<u64>,
    pub ask_size: Option<u64>,
    pub price: Option<f64>,
    pub size: Option<u64>,
    pub side: Option<String>,
    pub action: Option<String>,
    pub flags: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct ParsedBlockHeader {
    block_size: u16,
    messages_in_block: u8,
    block_sequence: u32,
    block_ts_sec: u32,
    block_ts_nsec: u32,
}

#[derive(Debug, Clone, Copy)]
struct ParsedMessageHeader {
    participant: u8,
    category: u8,
    type_code: u8,
    indicator: u8,
}

#[derive(Debug, Clone, Copy)]
struct MessageDispatchKey {
    category: u8,
    type_code: u8,
    indicator: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpraMessageFamily {
    QuoteShort,
    QuoteLong,
    EquityIndexLastSale,
    UnderlyingValueLastSale,
    OpenInterest,
    EndOfDaySummary,
    TimedQuote,
    Recap,
    Administrative,
    Control,
    Other,
}

/// Read the full PCAP file into memory once so decode functions can reuse the bytes.
pub async fn read_pcap_file(path: &str) -> Result<Vec<u8>> {
    let mut file = File::open(path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

/// Parse per-packet OPRA block metadata (header schema) using Rayon for parallel frame decode.
pub fn decode_pcap_headers_schema(
    buffer: &[u8],
    parallel: usize,
) -> Result<(DecodeStats, Vec<HeaderOpraRow>)> {
    let capture = PcapCapture::from_file(buffer)
        .map_err(|error| anyhow!("failed to parse pcap: {error}"))?;

    // Collect frames first because `capture.iter()` is an iterator; Rayon needs a splittable collection.
    // These are borrowed slices into `buffer`, so this is not copying packet bytes.
    let legacy_frames: Vec<&[u8]> = capture
        .iter()
        .filter_map(|block| match block {
            PcapBlock::Legacy(legacy) => Some(legacy.data),
            _ => None,
        })
        .collect();

    let (stats, mut rows) =
        ThreadPoolBuilder::new()
            .num_threads(parallel)
            .build()
            .map_err(|error| anyhow!("failed to build rayon pool: {error}"))?
            .install(|| {
                legacy_frames
                    .par_iter()
                    .enumerate()
                    .map(|(packet_index, &frame)| {
                        let (stats, row) = decode_legacy_frame(packet_index, frame);
                        let rows = row.into_iter().collect::<Vec<_>>();
                        (stats, rows)
                    })
                    .reduce(
                        || (DecodeStats::default(), Vec::new()),
                        |(mut acc_stats, mut acc_rows), (local_stats, mut local_rows)| {
                            acc_stats.packets = acc_stats.packets.saturating_add(local_stats.packets);
                            acc_stats.messages =
                                acc_stats.messages.saturating_add(local_stats.messages);
                            acc_rows.append(&mut local_rows);
                            (acc_stats, acc_rows)
                        },
                    )
            });

    // Keep row ordering stable for easier local inspection regardless of parallel execution order.
    rows.sort_unstable_by_key(|row| row.packet_index);

    Ok((stats, rows))
}
/// V2 trades decoder that walks each OPRA block sequentially using per-message lengths.
/// This avoids assuming equal message sizes within a block.
pub fn decode_pcap_trades_schema_v2(
    buffer: &[u8],
    parallel: usize,
) -> Result<Vec<DecodedTradeRow>> {
    let capture = PcapCapture::from_file(buffer)
        .map_err(|error| anyhow!("failed to parse pcap: {error}"))?;

    let legacy_frames: Vec<&[u8]> = capture
        .iter()
        .filter_map(|block| match block {
            PcapBlock::Legacy(legacy) => Some(legacy.data),
            _ => None,
        })
        .collect();

    let mut rows = ThreadPoolBuilder::new()
        .num_threads(parallel)
        .build()
        .map_err(|error| anyhow!("failed to build rayon pool: {error}"))?
        .install(|| {
            legacy_frames
                .par_iter()
                .enumerate()
                .map(|(packet_index, &frame)| decode_trade_rows_from_frame_v2(packet_index, frame))
                .reduce(Vec::new, |mut acc, mut local| {
                    acc.append(&mut local);
                    acc
                })
        });

    rows.sort_unstable_by_key(|row| (row.packet_index, row.message_index_in_block));
    Ok(rows)
}

/// Decode one legacy Ethernet frame into a lightweight header row plus packet/message counters.
fn decode_legacy_frame(packet_index: usize, frame: &[u8]) -> (DecodeStats, Option<HeaderOpraRow>) {
    let mut local = DecodeStats::default();

    // Count the legacy packet regardless of whether it contains IPv4/UDP/OPRA.
    local.packets = local.packets.saturating_add(1);

    let mut parsed_row = None;
    if let Some(udp_payload) = extract_udp_payload(frame) {
        if let Some((block_size, message_count)) = parse_opra_block_header(udp_payload) {
            local.messages = local.messages.saturating_add(u64::from(message_count));
            parsed_row = Some(HeaderOpraRow {
                packet_index: u64::try_from(packet_index).unwrap_or(u64::MAX),
                udp_payload_len: u64::try_from(udp_payload.len()).unwrap_or(u64::MAX),
                block_size: u64::from(block_size),
                messages_in_block: u64::from(message_count),
            });
        }
    }

    (local, parsed_row)
}

/// Decode all trade-like rows from a single frame by walking OPRA messages one-by-one.
fn decode_trade_rows_from_frame_v2(packet_index: usize, frame: &[u8]) -> Vec<DecodedTradeRow> {
    let Some(udp_payload) = extract_udp_payload(frame) else {
        return Vec::new();
    };
    let Some(block_header) = parse_block_header(udp_payload) else {
        return Vec::new();
    };
    let Some(messages_slice) = udp_payload.get(OPRA_BLOCK_HEADER_LEN..usize::from(block_header.block_size)) else {
        return Vec::new();
    };

    let msg_count = usize::from(block_header.messages_in_block);
    if msg_count == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut cursor = 0_usize;

    for message_index in 0..msg_count {
        let remaining_messages = msg_count.saturating_sub(message_index);
        let remaining_bytes = messages_slice.len().saturating_sub(cursor);
        if remaining_messages == 0 || remaining_bytes < 12 {
            break;
        }

        let Some(message_window) = messages_slice.get(cursor..) else {
            break;
        };
        let Some(header) = parse_message_header(message_window) else {
            break;
        };

        let Some(msg_len) =
            resolve_message_length_v2(header, message_window, remaining_bytes, remaining_messages)
        else {
            break;
        };
        let end = cursor.saturating_add(msg_len);
        let Some(message_bytes) = messages_slice.get(cursor..end) else {
            break;
        };

        if let Some(row) = decode_message_by_spec(
            packet_index,
            message_index,
            block_header,
            header,
            message_bytes,
        ) {
            out.push(row);
        }

        cursor = end;
    }

    out
}

/// Decide how long the current message is based on category and known message layouts.
fn resolve_message_length_v2(
    header: ParsedMessageHeader,
    message_window: &[u8],
    remaining_bytes: usize,
    remaining_messages: usize,
) -> Option<usize> {
    let fixed_len = match header.category {
        b'a' | b'k' => Some(43_usize),
        b'q' => Some(29_usize),
        b'H' => Some(12_usize),
        _ => None,
    };

    if let Some(len) = fixed_len {
        if is_plausible_message_len(len, remaining_bytes, remaining_messages) {
            return Some(len);
        }
    }

    // Administrative messages are variable length and include a data length field.
    if header.category == b'C' {
        if let Some(text_len) = read_be_u16_at(message_window, 12).map(usize::from) {
            let admin_len = 14_usize.saturating_add(text_len);
            if is_plausible_message_len(admin_len, remaining_bytes, remaining_messages) {
                return Some(admin_len);
            }
        }
    }

    fallback_equal_split_len_v2(remaining_bytes, remaining_messages)
}

/// Basic guardrail: ensure a chosen message length leaves room for remaining message headers.
fn is_plausible_message_len(len: usize, remaining_bytes: usize, remaining_messages: usize) -> bool {
    if len < 12 || len > remaining_bytes || remaining_messages == 0 {
        return false;
    }
    let min_tail = remaining_messages.saturating_sub(1).saturating_mul(12);
    remaining_bytes.saturating_sub(len) >= min_tail
}

/// Last-resort length guess used when the message type is unknown.
fn fallback_equal_split_len_v2(remaining_bytes: usize, remaining_messages: usize) -> Option<usize> {
    if remaining_messages == 0 {
        return None;
    }
    let len = remaining_bytes / remaining_messages;
    (len >= 12).then_some(len)
}

#[must_use]
/// Strip Ethernet/IP/UDP headers and return only UDP payload bytes.
fn extract_udp_payload(frame: &[u8]) -> Option<&[u8]> {
    let min_frame_len = ETHERNET_HEADER_LEN
        .checked_add(IPV4_MIN_HEADER_LEN)?
        .checked_add(UDP_HEADER_LEN)?;
    if frame.len() < min_frame_len {
        return None;
    }

    let ethertype = read_be_u16_at(frame, ETHERTYPE_OFFSET)?;
    let ip_start = match ethertype {
        ETHERTYPE_IPV4 => ETHERNET_HEADER_LEN,
        ETHERTYPE_VLAN_8021Q | ETHERTYPE_VLAN_8021AD => {
            let vlan_frame_min = ETHERNET_HEADER_LEN.checked_add(VLAN_HEADER_LEN)?;
            if frame.len() < vlan_frame_min {
                return None;
            }

            let inner_ethertype = read_be_u16_at(frame, VLAN_INNER_ETHERTYPE_OFFSET)?;
            if inner_ethertype != ETHERTYPE_IPV4 {
                return None;
            }

            vlan_frame_min
        }
        _ => return None,
    };

    let ip_and_udp_min = ip_start
        .checked_add(IPV4_MIN_HEADER_LEN)?
        .checked_add(UDP_HEADER_LEN)?;
    if frame.len() < ip_and_udp_min {
        return None;
    }

    let ip_first_byte = read_u8_at(frame, ip_start)?;
    let ihl_words = ip_first_byte & 0x0f;
    let ip_header_len = usize::from(ihl_words).checked_mul(4)?;
    if ip_header_len < IPV4_MIN_HEADER_LEN {
        return None;
    }

    let protocol_offset = ip_start.checked_add(IPV4_PROTOCOL_FIELD_OFFSET)?;
    let protocol = read_u8_at(frame, protocol_offset)?;
    if protocol != IP_PROTOCOL_UDP {
        return None;
    }

    let udp_start = ip_start.checked_add(ip_header_len)?;
    let udp_len_field_offset = udp_start.checked_add(UDP_LENGTH_FIELD_OFFSET)?;
    let udp_len = usize::from(read_be_u16_at(frame, udp_len_field_offset)?);
    if udp_len < UDP_HEADER_LEN {
        return None;
    }

    let payload_start = udp_start.checked_add(UDP_HEADER_LEN)?;
    let payload_len = udp_len.checked_sub(UDP_HEADER_LEN)?;
    let payload_end = payload_start.checked_add(payload_len)?;

    if payload_end > frame.len() {
        return None;
    }

    frame.get(payload_start..payload_end)
}

#[must_use]
/// Return just the OPRA block size and message count for quick header stats collection.
fn parse_opra_block_header(udp_payload: &[u8]) -> Option<(u16, u8)> {
    let block_header = parse_block_header(udp_payload)?;
    Some((block_header.block_size, block_header.messages_in_block))
}

#[must_use]
/// Parse OPRA block-level fields (size, sequence, timestamp, message count).
fn parse_block_header(udp_payload: &[u8]) -> Option<ParsedBlockHeader> {
    if udp_payload.len() < OPRA_BLOCK_HEADER_LEN {
        return None;
    }

    // OPRA packets observed in this feed have:
    // - block size at byte offsets [1..=2] (big-endian)
    // - data feed indicator 'O' at byte offset 3
    // - messages-in-block at byte offset 10
    let block_size = read_be_u16_at(udp_payload, OPRA_BLOCK_SIZE_OFFSET)?;
    let block_size_usize = usize::from(block_size);
    if block_size_usize == 0 || block_size_usize > udp_payload.len() {
        return None;
    }

    let feed_indicator = read_u8_at(udp_payload, OPRA_DATA_FEED_INDICATOR_OFFSET)?;
    if feed_indicator != OPRA_DATA_FEED_INDICATOR {
        return None;
    }

    let messages_in_block = read_u8_at(udp_payload, OPRA_MESSAGES_IN_BLOCK_OFFSET)?;
    let block_sequence_hi = u32::from(read_be_u16_at(udp_payload, 6)?);
    let block_sequence_lo = u32::from(read_be_u16_at(udp_payload, 8)?);
    let block_sequence = (block_sequence_hi << 16) | block_sequence_lo;
    let block_ts_sec_hi = u32::from(read_be_u16_at(udp_payload, 11)?);
    let block_ts_sec_lo = u32::from(read_be_u16_at(udp_payload, 13)?);
    let block_ts_sec = (block_ts_sec_hi << 16) | block_ts_sec_lo;
    let block_ts_nsec_hi = u32::from(read_be_u16_at(udp_payload, 15)?);
    let block_ts_nsec_lo = u32::from(read_be_u16_at(udp_payload, 17)?);
    let block_ts_nsec = (block_ts_nsec_hi << 16) | block_ts_nsec_lo;

    Some(ParsedBlockHeader {
        block_size,
        messages_in_block,
        block_sequence,
        block_ts_sec,
        block_ts_nsec,
    })
}

#[must_use]
/// Parse the fixed 12-byte OPRA message header.
fn parse_message_header(message: &[u8]) -> Option<ParsedMessageHeader> {
    if message.len() < 12 {
        return None;
    }
    Some(ParsedMessageHeader {
        participant: *message.first()?,
        category: *message.get(1)?,
        type_code: *message.get(2)?,
        indicator: *message.get(3)?,
    })
}

#[must_use]
/// Map raw message category/type/indicator into a parser family.
fn classify_message_family(key: MessageDispatchKey) -> OpraMessageFamily {
    let _ = key.type_code;
    let _ = key.indicator;
    match key.category {
        b'q' => OpraMessageFamily::QuoteShort,
        b'k' => OpraMessageFamily::QuoteLong,
        b'a' => OpraMessageFamily::EquityIndexLastSale,
        b'd' => OpraMessageFamily::UnderlyingValueLastSale,
        b'f' => OpraMessageFamily::OpenInterest,
        b'n' => OpraMessageFamily::EndOfDaySummary,
        b't' => OpraMessageFamily::TimedQuote,
        b'r' => OpraMessageFamily::Recap,
        b'C' => OpraMessageFamily::Administrative,
        b'H' => OpraMessageFamily::Control,
        _ => OpraMessageFamily::Other,
    }
}

/// Route one message to the correct family parser and return a normalized row when supported.
fn decode_message_by_spec(
    packet_index: usize,
    message_index: usize,
    block: ParsedBlockHeader,
    header: ParsedMessageHeader,
    message: &[u8],
) -> Option<DecodedTradeRow> {
    let key = MessageDispatchKey {
        category: header.category,
        type_code: header.type_code,
        indicator: header.indicator,
    };

    match classify_message_family(key) {
        OpraMessageFamily::QuoteShort => {
            parse_short_quote_row(packet_index, message_index, block, header, message)
        }
        OpraMessageFamily::QuoteLong => {
            parse_long_quote_row(packet_index, message_index, block, header, message)
        }
        OpraMessageFamily::EquityIndexLastSale => {
            parse_equity_index_last_sale_row(packet_index, message_index, block, header, message)
        }
        // Explicitly routed but not implemented yet.
        OpraMessageFamily::UnderlyingValueLastSale
        | OpraMessageFamily::OpenInterest
        | OpraMessageFamily::EndOfDaySummary
        | OpraMessageFamily::TimedQuote
        | OpraMessageFamily::Recap
        | OpraMessageFamily::Administrative
        | OpraMessageFamily::Control
        | OpraMessageFamily::Other => None,
    }
}

/// Parse OPRA category `a` (equity/index last sale) into a trade row.
fn parse_equity_index_last_sale_row(
    packet_index: usize,
    message_index: usize,
    block: ParsedBlockHeader,
    header: ParsedMessageHeader,
    message: &[u8],
) -> Option<DecodedTradeRow> {
    // OPRA category 'a' base layout:
    // 12-byte message header + 31-byte body = 43 bytes total.
    if message.len() < 43 {
        return None;
    }
    let body = message.get(12..43)?;
    let symbol_raw = body.get(0..5)?;
    let symbol_root = std::str::from_utf8(symbol_raw).ok()?.trim_end().to_string();
    let exp = body.get(6..9)?;
    let strike_den = *body.get(9)?;
    let strike_raw = read_be_u32_at(body, 10)?;
    let volume = read_be_u32_at(body, 14)?;
    let premium_den = *body.get(18)?;
    let premium_raw = read_be_u32_at(body, 19)?;
    let trade_identifier = read_be_u32_at(body, 23)?;

    let (yymmdd, cp) = decode_exp_block(exp);
    let strike = as_price_u32(strike_raw, strike_den);
    let osi_symbol = build_osi_symbol(&symbol_root, &yymmdd, cp, strike);
    let block_timestamp_ns = u64::from(block.block_ts_sec)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(block.block_ts_nsec));
    let message_type = char::from(header.type_code);
    let action = Some(message_type.to_string());

    Some(DecodedTradeRow {
        packet_index: u64::try_from(packet_index).unwrap_or(u64::MAX),
        block_sequence: u64::from(block.block_sequence),
        block_timestamp_ns,
        block_timestamp_utc: format_unix_ns_to_utc(block_timestamp_ns),
        message_index_in_block: u64::try_from(message_index).unwrap_or(u64::MAX),
        participant: char::from(header.participant).to_string(),
        category: char::from(header.category).to_string(),
        type_code: message_type.to_string(),
        indicator: char::from(header.indicator).to_string(),
        symbol_root: Some(symbol_root),
        osi_symbol: Some(osi_symbol),
        bid: None,
        ask: None,
        bid_size: None,
        ask_size: None,
        price: Some(as_price_u32(premium_raw, premium_den)),
        size: Some(u64::from(volume)),
        side: None,
        action,
        // Use Trade Identifier as a stable per-trade flag-style field for research joins.
        flags: Some(u64::from(trade_identifier)),
    })
}

/// Parse OPRA short quote messages (`q`) into normalized quote fields.
fn parse_short_quote_row(
    packet_index: usize,
    message_index: usize,
    block: ParsedBlockHeader,
    header: ParsedMessageHeader,
    message: &[u8],
) -> Option<DecodedTradeRow> {
    // 12-byte OPRA message header + 17-byte short quote body in observed data.
    if message.len() < 29 {
        return None;
    }
    let body = message.get(12..29)?;

    let symbol_raw = body.get(0..4)?;
    let symbol_root = std::str::from_utf8(symbol_raw).ok()?.trim_end().to_string();
    let exp = body.get(4..7)?;
    let strike_raw = read_be_u16_at(body, 7)?;
    let bid_raw = read_be_u16_at(body, 9)?;
    let bid_size_raw = read_be_u16_at(body, 11)?;
    let ask_raw = read_be_u16_at(body, 13)?;
    let ask_size_raw = read_be_u16_at(body, 15)?;

    let (yymmdd, cp) = decode_exp_block(exp);
    let strike = as_price_u16(strike_raw, b'A');
    let osi_symbol = build_osi_symbol(&symbol_root, &yymmdd, cp, strike);
    let block_timestamp_ns = u64::from(block.block_ts_sec)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(block.block_ts_nsec));

    Some(DecodedTradeRow {
        packet_index: u64::try_from(packet_index).unwrap_or(u64::MAX),
        block_sequence: u64::from(block.block_sequence),
        block_timestamp_ns,
        block_timestamp_utc: format_unix_ns_to_utc(block_timestamp_ns),
        message_index_in_block: u64::try_from(message_index).unwrap_or(u64::MAX),
        participant: char::from(header.participant).to_string(),
        category: char::from(header.category).to_string(),
        type_code: char::from(header.type_code).to_string(),
        indicator: char::from(header.indicator).to_string(),
        symbol_root: Some(symbol_root),
        osi_symbol: Some(osi_symbol),
        bid: Some(as_price_u16(bid_raw, b'B')),
        ask: Some(as_price_u16(ask_raw, b'B')),
        bid_size: Some(u64::from(bid_size_raw)),
        ask_size: Some(u64::from(ask_size_raw)),
        // Leave trade print fields empty for quote records.
        price: None,
        size: None,
        side: None,
        action: None,
        // No separate flags byte is decoded yet for short quote body in this parser.
        flags: None,
    })
}

/// Parse OPRA long quote messages (`k`) into normalized quote fields.
fn parse_long_quote_row(
    packet_index: usize,
    message_index: usize,
    block: ParsedBlockHeader,
    header: ParsedMessageHeader,
    message: &[u8],
) -> Option<DecodedTradeRow> {
    // 12-byte OPRA header + 31-byte long quote body in notebook parser.
    if message.len() < 43 {
        return None;
    }
    let body = message.get(12..43)?;
    let symbol_raw = body.get(0..5)?;
    let symbol_root = std::str::from_utf8(symbol_raw).ok()?.trim_end().to_string();
    let exp = body.get(6..9)?;
    let strike_den = *body.get(9)?;
    let strike_raw = read_be_u32_at(body, 10)?;
    let bid_raw = read_be_u32_at(body, 15)?;
    let bid_size_raw = read_be_u32_at(body, 19)?;
    let ask_raw = read_be_u32_at(body, 23)?;
    let ask_size_raw = read_be_u32_at(body, 27)?;

    let (yymmdd, cp) = decode_exp_block(exp);
    let strike = as_price_u32(strike_raw, strike_den);
    let osi_symbol = build_osi_symbol(&symbol_root, &yymmdd, cp, strike);
    let block_timestamp_ns = u64::from(block.block_ts_sec)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(block.block_ts_nsec));

    Some(DecodedTradeRow {
        packet_index: u64::try_from(packet_index).unwrap_or(u64::MAX),
        block_sequence: u64::from(block.block_sequence),
        block_timestamp_ns,
        block_timestamp_utc: format_unix_ns_to_utc(block_timestamp_ns),
        message_index_in_block: u64::try_from(message_index).unwrap_or(u64::MAX),
        participant: char::from(header.participant).to_string(),
        category: char::from(header.category).to_string(),
        type_code: char::from(header.type_code).to_string(),
        indicator: char::from(header.indicator).to_string(),
        symbol_root: Some(symbol_root),
        osi_symbol: Some(osi_symbol),
        bid: Some(as_price_u32(bid_raw, b'B')),
        ask: Some(as_price_u32(ask_raw, b'B')),
        bid_size: Some(u64::from(bid_size_raw)),
        ask_size: Some(u64::from(ask_size_raw)),
        price: None,
        size: None,
        side: None,
        action: None,
        flags: None,
    })
}

/// Decode OPRA expiration block bytes into `YYMMDD` and call/put code.
fn decode_exp_block(exp_bytes: &[u8]) -> (String, char) {
    let (month, cp) = match exp_bytes.first().copied().map(char::from) {
        Some('A') => (1, 'C'),
        Some('B') => (2, 'C'),
        Some('C') => (3, 'C'),
        Some('D') => (4, 'C'),
        Some('E') => (5, 'C'),
        Some('F') => (6, 'C'),
        Some('G') => (7, 'C'),
        Some('H') => (8, 'C'),
        Some('I') => (9, 'C'),
        Some('J') => (10, 'C'),
        Some('K') => (11, 'C'),
        Some('L') => (12, 'C'),
        Some('M') => (1, 'P'),
        Some('N') => (2, 'P'),
        Some('O') => (3, 'P'),
        Some('P') => (4, 'P'),
        Some('Q') => (5, 'P'),
        Some('R') => (6, 'P'),
        Some('S') => (7, 'P'),
        Some('T') => (8, 'P'),
        Some('U') => (9, 'P'),
        Some('V') => (10, 'P'),
        Some('W') => (11, 'P'),
        Some('X') => (12, 'P'),
        _ => (0, '?'),
    };

    let day = exp_bytes.get(1).copied().unwrap_or_default();
    let year = exp_bytes.get(2).copied().unwrap_or_default();
    let yymmdd = format!("{year:02}{month:02}{day:02}");
    (yymmdd, cp)
}

/// Build a padded 21-character OSI option symbol from normalized parts.
fn build_osi_symbol(root: &str, yymmdd: &str, cp: char, strike: f64) -> String {
    let strike_int = (strike * 1000.0).round();
    let strike_int = if strike_int.is_finite() && strike_int >= 0.0 {
        strike_int as u64
    } else {
        0
    };
    format!("{root}   {yymmdd}{cp}{strike_int:08}")
}

/// Convert 16-bit raw price plus denominator code into decimal price.
fn as_price_u16(raw: u16, den_code: u8) -> f64 {
    let den = match den_code {
        b'A' => 1_u32,
        b'B' => 2_u32,
        b'C' => 3_u32,
        b'D' => 4_u32,
        _ => 0_u32,
    };
    if den == 0 {
        return f64::from(raw);
    }
    f64::from(raw) / 10_f64.powi(i32::try_from(den).unwrap_or(0))
}

/// Convert 32-bit raw price plus denominator code into decimal price.
fn as_price_u32(raw: u32, den_code: u8) -> f64 {
    let den = match den_code {
        b'A' => 1_u32,
        b'B' => 2_u32,
        b'C' => 3_u32,
        b'D' => 4_u32,
        _ => 0_u32,
    };
    if den == 0 {
        return f64::from(raw);
    }
    f64::from(raw) / 10_f64.powi(i32::try_from(den).unwrap_or(0))
}

/// Convert nanoseconds since epoch into RFC3339 UTC string.
fn format_unix_ns_to_utc(timestamp_ns: u64) -> String {
    let secs = i64::try_from(timestamp_ns / 1_000_000_000).unwrap_or(i64::MAX);
    let nanos = u32::try_from(timestamp_ns % 1_000_000_000).unwrap_or(0);
    if let Some(dt) = Utc.timestamp_opt(secs, nanos).single() {
        dt.to_rfc3339_opts(SecondsFormat::Nanos, true)
    } else {
        String::new()
    }
}

#[must_use]
/// Safely read one byte at an offset.
fn read_u8_at(data: &[u8], offset: usize) -> Option<u8> {
    data.get(offset).copied()
}

#[must_use]
/// Safely read a big-endian `u16` at an offset.
fn read_be_u16_at(data: &[u8], offset: usize) -> Option<u16> {
    let end = offset.checked_add(2)?;
    let bytes = data.get(offset..end)?;
    let array: [u8; 2] = bytes.try_into().ok()?;
    Some(u16::from_be_bytes(array))
}

#[must_use]
/// Safely read a big-endian `u32` at an offset.
fn read_be_u32_at(data: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let bytes = data.get(offset..end)?;
    let array: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_block_header_known_payload() {
        let payload = hex::decode("06006c4f20001530b0030364e4c6673b97bf001e7043714141410bcc433b932cc853505920541e17108600300136003201a243714141410bcc433b932cc8535059205416171126004e002d005001b243714141410bcc433b932cc853505920541917111c00e1007900e400c1").expect("hex decode");
        let header = parse_block_header(&payload).expect("header");
        assert_eq!(header.block_size, 108);
        assert_eq!(header.messages_in_block, 3);
        assert_eq!(header.block_sequence, 355_512_323);
        assert_eq!(header.block_ts_sec, 1_692_714_599);
    }

    #[test]
    fn decodes_exp_block_put() {
        let (yymmdd, cp) = decode_exp_block(&[b'T', 30, 23]);
        assert_eq!(yymmdd, "230830");
        assert_eq!(cp, 'P');
    }

    #[test]
    fn price_denominator_conversion() {
        assert!((as_price_u16(423, b'B') - 4.23).abs() < 1e-9);
        assert!((as_price_u32(123456, b'A') - 12345.6).abs() < 1e-9);
    }

    #[test]
    fn formats_block_timestamp_to_utc() {
        let ts = format_unix_ns_to_utc(1_692_714_600_005_955_584);
        assert_eq!(ts, "2023-08-22T14:30:00.005955584Z");
    }

    #[test]
    fn classifies_message_families() {
        let q = MessageDispatchKey {
            category: b'q',
            type_code: b' ',
            indicator: b'A',
        };
        let k = MessageDispatchKey {
            category: b'k',
            type_code: b' ',
            indicator: b'A',
        };
        let a = MessageDispatchKey {
            category: b'a',
            type_code: b'A',
            indicator: b' ',
        };
        assert_eq!(classify_message_family(q), OpraMessageFamily::QuoteShort);
        assert_eq!(classify_message_family(k), OpraMessageFamily::QuoteLong);
        assert_eq!(
            classify_message_family(a),
            OpraMessageFamily::EquityIndexLastSale
        );
    }

    #[test]
    fn parses_equity_index_last_sale_row() {
        let mut message = vec![0_u8; 43];
        // Message header
        message[0] = b'C'; // participant
        message[1] = b'a'; // category
        message[2] = b'A'; // type
        message[3] = b' '; // indicator

        // Body at offset 12
        message[12..17].copy_from_slice(b"SPY  ");
        // reserved at 17
        message[18] = b'T'; // expiration month code (put, Aug)
        message[19] = 30; // day
        message[20] = 23; // year
        message[21] = b'A'; // strike denominator (1 dp)
        message[22..26].copy_from_slice(&4230_u32.to_be_bytes()); // strike 423.0
        message[26..30].copy_from_slice(&271_u32.to_be_bytes()); // volume
        message[30] = b'B'; // premium denominator (2 dp)
        message[31..35].copy_from_slice(&49_u32.to_be_bytes()); // premium 0.49
        message[35..39].copy_from_slice(&194_u32.to_be_bytes()); // trade identifier

        let block = ParsedBlockHeader {
            block_size: 43,
            messages_in_block: 1,
            block_sequence: 123,
            block_ts_sec: 1_692_714_600,
            block_ts_nsec: 5_955_584,
        };
        let header = ParsedMessageHeader {
            participant: b'C',
            category: b'a',
            type_code: b'A',
            indicator: b' ',
        };

        let row = parse_equity_index_last_sale_row(10, 0, block, header, &message).expect("row");
        assert_eq!(row.category, "a");
        assert_eq!(row.type_code, "A");
        assert_eq!(row.osi_symbol.as_deref(), Some("SPY   230830P00423000"));
        assert!((row.price.unwrap_or_default() - 0.49).abs() < 1e-9);
        assert_eq!(row.size, Some(271));
        assert_eq!(row.flags, Some(194));
    }

}
