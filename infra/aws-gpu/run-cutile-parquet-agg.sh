#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if ! command -v terraform >/dev/null 2>&1; then
  echo "Terraform is required. Install Terraform and retry."
  exit 1
fi
if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync is required. Install rsync and retry."
  exit 1
fi
if ! command -v ssh >/dev/null 2>&1; then
  echo "ssh is required."
  exit 1
fi

PUBLIC_IP="$(terraform -chdir="${SCRIPT_DIR}" output -raw public_ip)"
SSH_KEY_PATH="$(terraform -chdir="${SCRIPT_DIR}" output -raw ssh_private_key_path)"
REMOTE_DIR="$(terraform -chdir="${SCRIPT_DIR}" output -raw remote_workdir)"
SSH_OPTS=(-i "${SSH_KEY_PATH}" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)
PREFERRED_SSH_USER="$(terraform -chdir="${SCRIPT_DIR}" output -raw ssh_user 2>/dev/null || echo "ubuntu")"

PARQUET_LOCAL="${1:-${ROOT_DIR}/rust-ingest/pcap_samples/ny4-small-10m_trades.parquet}"
MAX_ROWS="${2:-}"

if [[ ! -f "${PARQUET_LOCAL}" ]]; then
  echo "Parquet file not found: ${PARQUET_LOCAL}"
  echo "Usage: ./run-cutile-parquet-agg.sh [local_parquet_path] [max_rows]"
  exit 1
fi

resolve_ssh_user() {
  local -a candidates=("${PREFERRED_SSH_USER}" "ubuntu" "ec2-user" "admin")
  local user
  for user in "${candidates[@]}"; do
    if ssh "${SSH_OPTS[@]}" -o BatchMode=yes -o ConnectTimeout=8 "${user}@${PUBLIC_IP}" "true" >/dev/null 2>&1; then
      echo "${user}"
      return 0
    fi
  done
  return 1
}

if ! SSH_USER="$(resolve_ssh_user)"; then
  echo "Unable to authenticate via SSH with any known user."
  echo "Tried: ${PREFERRED_SSH_USER}, ubuntu, ec2-user, admin"
  exit 1
fi
echo "Using SSH user: ${SSH_USER}"

echo "Syncing rust-ingest sources..."
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${PUBLIC_IP}" "mkdir -p ${REMOTE_DIR}/rust-ingest/pcap_samples"
rsync -az --delete \
  --exclude 'target/' \
  --exclude '.DS_Store' \
  --exclude 'pcap_samples/' \
  "${ROOT_DIR}/rust-ingest/" "${SSH_USER}@${PUBLIC_IP}:${REMOTE_DIR}/rust-ingest/"

echo "Uploading parquet: ${PARQUET_LOCAL}"
REMOTE_PARQUET="${REMOTE_DIR}/rust-ingest/pcap_samples/$(basename "${PARQUET_LOCAL}")"
rsync -az "${PARQUET_LOCAL}" "${SSH_USER}@${PUBLIC_IP}:${REMOTE_PARQUET}"

RUN_CMD="cargo run --release --bin cutile_parquet_agg --features gpu-cutile -- pcap_samples/$(basename "${PARQUET_LOCAL}")"
if [[ -n "${MAX_ROWS}" ]]; then
  RUN_CMD="${RUN_CMD} ${MAX_ROWS}"
fi

echo "Running GPU parquet aggregation on EC2..."
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${PUBLIC_IP}" "bash -lc '
set -euo pipefail
source ~/.cargo/env
export CUDA_TOOLKIT_PATH=/usr/local/cuda-13.2
export CUDA_TILE_USE_LLVM_INSTALL_DIR=/usr/lib/llvm-21
export PATH=/usr/local/cuda-13.2/bin:\$PATH
cd ${REMOTE_DIR}/rust-ingest
nvidia-smi >/dev/null
tileiras --version | head -n 1 || true
${RUN_CMD}
'"
