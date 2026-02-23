use anyhow::{anyhow, Result};
use pcap_parser::{Capture, PcapBlock, PcapCapture};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Debug, Default)]
pub struct DecodeStats {
    pub packets: u64,
    pub messages: u64,
}

/// Skeleton decoder: replace with real OPRA Pillar parsing.
pub async fn decode_pcap(path: &str, _parallel: usize) -> Result<DecodeStats> {
    // Read file into memory
    let mut f = File::open(path).await?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).await?;

    // Parse legacy PCAP in-memory
    let capture = PcapCapture::from_file(&buf)
        .map_err(|e| anyhow!("failed to parse pcap: {e:?}"))?;

    let mut stats = DecodeStats::default();

    for block in capture.iter() {
        if let PcapBlock::Legacy(legacy) = block {
            stats.packets += 1;

            // legacy.data is the captured L2 frame:
            // Ethernet [+ optional VLAN] + IPv4 + UDP + payload
            if let Some(udp_payload) = extract_udp_payload(legacy.data) {
                if let Some(msgs) = count_opra_messages(udp_payload) {
                    stats.messages += msgs as u64;
                }

                // TODO: call a real OPRA parser here
                // parse_opra_block(udp_payload, &mut stats)
            }
        }
    }

    Ok(stats)
}

/// Extract the UDP payload (OPRA block) from a raw Ethernet frame.
///
/// Handles:
/// - Plain Ethernet + IPv4 + UDP
/// - Ethernet + 802.1Q/802.1ad VLAN + IPv4 + UDP
fn extract_udp_payload(frame: &[u8]) -> Option<&[u8]> {
    // Need at least Ethernet (14) + IPv4 min (20) + UDP (8).
    if frame.len() < 14 + 20 + 8 {
        return None;
    }

    // EtherType at bytes 12-13
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);

    // Determine where the IPv4 header actually starts.
    // - 0x0800: plain IPv4 immediately after Ethernet (offset 14)
    // - 0x8100 / 0x88a8: VLAN tag present, inner EtherType at bytes 16-17
    let ip_start = match ethertype {
        0x0800 => {
            // Ethernet type = IPv4, no VLAN
            14
        }
        0x8100 | 0x88a8 => {
            // 802.1Q / 802.1ad VLAN tag: 4-byte VLAN header after Ethernet
            if frame.len() < 18 {
                return None;
            }
            let inner_ethertype = u16::from_be_bytes([frame[16], frame[17]]);
            if inner_ethertype != 0x0800 {
                // VLAN present but not carrying IPv4
                return None;
            }
            // IPv4 header starts after Ethernet (14) + VLAN (4)
            18
        }
        _ => {
            // Not IPv4 and not VLAN-with-IPv4
            return None;
        }
    };

    if frame.len() < ip_start + 20 + 8 {
        // Not enough bytes for IPv4 min header + UDP header
        return None;
    }

    // IPv4 header length (low 4 bits of first byte, in 32-bit words)
    let ihl_words = frame[ip_start] & 0x0f;
    let ip_header_len = (ihl_words as usize) * 4;

    if frame.len() < ip_start + ip_header_len + 8 {
        return None;
    }

    // Protocol field at offset 9 of IPv4 header
    let protocol = frame[ip_start + 9];
    if protocol != 17 {
        // Not UDP
        return None;
    }

    let udp_start = ip_start + ip_header_len;

    // UDP length field (includes UDP header)
    let udp_len =
        u16::from_be_bytes([frame[udp_start + 4], frame[udp_start + 5]]) as usize;
    if udp_len < 8 {
        return None;
    }

    let payload_start = udp_start + 8;
    let payload_len = udp_len - 8;

    // Clamp to frame length just in case snaplen < full UDP_length
    let payload_end = payload_start.checked_add(payload_len)?;
    if payload_end > frame.len() {
        return None;
    }

    Some(&frame[payload_start..payload_end])
}

/// Read the OPRA Block header just enough to get the message count.
///
/// Layout (all integers big-endian):
///   0..2   Block Size (u16)
///   2      Data Feed Indicator (u8, ASCII 'O')
///   3      Retransmission Indicator (u8, ' ' or 'V')
///   4      Session Indicator (u8)
///   5..9   Block Sequence Number (u32)
///   9      Messages In Block (u8)
///   10..18 Block Timestamp (seconds + nanoseconds, u32 + u32)
///   18..21 Block Checksum (u16)
fn count_opra_messages(udp_payload: &[u8]) -> Option<u8> {
    const OPRA_BLOCK_HEADER_LEN: usize = 21;

    if udp_payload.len() < OPRA_BLOCK_HEADER_LEN {
        return None;
    }

    // Block size is number of bytes in this OPRA block (header + data + optional pad).
    let block_size = u16::from_be_bytes([udp_payload[0], udp_payload[1]]) as usize;
    if block_size == 0 || block_size > udp_payload.len() {
        return None;
    }

    let messages_in_block = udp_payload[9];

    Some(messages_in_block)
}