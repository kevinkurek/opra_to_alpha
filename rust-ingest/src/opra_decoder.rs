use anyhow::Result;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Debug, Default)]
pub struct DecodeStats {
    pub packets: u64,
    pub messages: u64,
}

/// Skeleton decoder: replace with real OPRA Pillar parsing.
pub async fn decode_pcap(path: &str, _parallel: usize) -> Result<DecodeStats> {
    // We just read the file to prove plumbing; no real parsing yet.
    let mut f = File::open(path).await?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).await?;

    // TODO: parse global header, per-packet headers, UDP payload, OPRA message framing.
    Ok(DecodeStats { packets: 0, messages: 0 })
}
