use anyhow::{anyhow, Result};
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
pub struct ParsedOpraRow {
    pub packet_index: u64,
    pub udp_payload_len: u64,
    pub block_size: u64,
    pub messages_in_block: u64,
}

/// Skeleton decoder: replace with real OPRA Pillar parsing.
pub async fn read_pcap_file(path: &str) -> Result<Vec<u8>> {
    let mut file = File::open(path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

/// Synchronous decode over already-loaded PCAP bytes.
pub fn decode_pcap(buffer: &[u8]) -> Result<(DecodeStats, Vec<ParsedOpraRow>)> {
    let parallel = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    decode_pcap_with_parallelism(buffer, parallel)
}

/// Synchronous decode over already-loaded PCAP bytes with explicit thread count.
pub fn decode_pcap_with_parallelism(
    buffer: &[u8],
    parallel: usize,
) -> Result<(DecodeStats, Vec<ParsedOpraRow>)> {
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

    let (stats, mut rows) = if parallel > 1 {
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
            })
    } else {
        legacy_frames
            .iter()
            .enumerate()
            .map(|(packet_index, &frame)| decode_legacy_frame(packet_index, frame))
            .fold(
                (DecodeStats::default(), Vec::new()),
                |(mut acc_stats, mut acc_rows), (local_stats, local_row)| {
                    acc_stats.packets = acc_stats.packets.saturating_add(local_stats.packets);
                    acc_stats.messages = acc_stats.messages.saturating_add(local_stats.messages);
                    if let Some(row) = local_row {
                        acc_rows.push(row);
                    }
                    (acc_stats, acc_rows)
                },
            )
    };

    // Keep row ordering stable for easier local inspection regardless of parallel execution order.
    rows.sort_unstable_by_key(|row| row.packet_index);

    Ok((stats, rows))
}

fn decode_legacy_frame(packet_index: usize, frame: &[u8]) -> (DecodeStats, Option<ParsedOpraRow>) {
    let mut local = DecodeStats::default();

    // Count the legacy packet regardless of whether it contains IPv4/UDP/OPRA.
    local.packets = local.packets.saturating_add(1);

    let mut parsed_row = None;
    if let Some(udp_payload) = extract_udp_payload(frame) {
        if let Some((block_size, message_count)) = parse_opra_block_header(udp_payload) {
            local.messages = local.messages.saturating_add(u64::from(message_count));
            parsed_row = Some(ParsedOpraRow {
                packet_index: u64::try_from(packet_index).unwrap_or(u64::MAX),
                udp_payload_len: u64::try_from(udp_payload.len()).unwrap_or(u64::MAX),
                block_size: u64::from(block_size),
                messages_in_block: u64::from(message_count),
            });
        }
    }

    (local, parsed_row)
}

#[must_use]
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
fn parse_opra_block_header(udp_payload: &[u8]) -> Option<(u16, u8)> {
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

    let message_count = read_u8_at(udp_payload, OPRA_MESSAGES_IN_BLOCK_OFFSET)?;
    Some((block_size, message_count))
}

#[must_use]
fn read_u8_at(data: &[u8], offset: usize) -> Option<u8> {
    data.get(offset).copied()
}

#[must_use]
fn read_be_u16_at(data: &[u8], offset: usize) -> Option<u16> {
    let end = offset.checked_add(2)?;
    let bytes = data.get(offset..end)?;
    let array: [u8; 2] = bytes.try_into().ok()?;
    Some(u16::from_be_bytes(array))
}
