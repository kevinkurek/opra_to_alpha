use anyhow::{anyhow, Result};
use pcap_parser::{PcapBlock, PcapCapture, Capture};
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
const OPRA_MESSAGES_IN_BLOCK_OFFSET: usize = 9;

#[derive(Debug, Default)]
pub struct DecodeStats {
    pub packets: u64,
    pub messages: u64,
}

/// Skeleton decoder: replace with real OPRA Pillar parsing.
pub async fn decode_pcap(path: &str, _parallel: usize) -> Result<DecodeStats> {
    let mut file = File::open(path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    let capture = PcapCapture::from_file(&buffer)
        .map_err(|error| anyhow!("failed to parse pcap: {error}"))?;

    let mut stats = DecodeStats::default();

    for block in capture.iter() {
        if let PcapBlock::Legacy(legacy) = block {
            stats.packets = stats.packets.saturating_add(1);

            if let Some(udp_payload) = extract_udp_payload(legacy.data) {
                if let Some(message_count) = count_opra_messages(udp_payload) {
                    stats.messages = stats.messages.saturating_add(u64::from(message_count));
                }

                // parse_opra_block(udp_payload, &mut stats)
            }
        }
    }

    Ok(stats)
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
fn count_opra_messages(udp_payload: &[u8]) -> Option<u8> {
    if udp_payload.len() < OPRA_BLOCK_HEADER_LEN {
        return None;
    }

    let block_size = usize::from(read_be_u16_at(udp_payload, 0)?);
    if block_size == 0 || block_size > udp_payload.len() {
        return None;
    }

    read_u8_at(udp_payload, OPRA_MESSAGES_IN_BLOCK_OFFSET)
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