#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

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

ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PUBLIC_IP="$(terraform -chdir="${SCRIPT_DIR}" output -raw public_ip)"
SSH_KEY_PATH="$(terraform -chdir="${SCRIPT_DIR}" output -raw ssh_private_key_path)"
REMOTE_DIR="$(terraform -chdir="${SCRIPT_DIR}" output -raw remote_workdir)"
SSH_OPTS=(-i "${SSH_KEY_PATH}" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)
PREFERRED_SSH_USER="$(terraform -chdir="${SCRIPT_DIR}" output -raw ssh_user 2>/dev/null || echo "ubuntu")"

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
  echo "Check key path, instance state, security group ingress CIDR, and AMI SSH user."
  exit 1
fi
echo "Using SSH user: ${SSH_USER}"

echo "Syncing rust-ingest to EC2 (${PUBLIC_IP})..."
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${PUBLIC_IP}" "mkdir -p ${REMOTE_DIR}/rust-ingest"
rsync -az --delete \
  --exclude 'target/' \
  --exclude '.DS_Store' \
  --exclude 'pcap_samples/' \
  "${ROOT_DIR}/rust-ingest/" "${SSH_USER}@${PUBLIC_IP}:${REMOTE_DIR}/rust-ingest/"

echo "Installing CUDA/cuTile prerequisites on EC2 (idempotent)..."
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${PUBLIC_IP}" "bash -s" <<'EOF'
set -euo pipefail

sudo apt-get update
sudo apt-get install -y build-essential git curl wget jq pkg-config ca-certificates unzip

if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env"
rustup default nightly

if ! command -v llvm-config >/dev/null 2>&1 || [[ "$(llvm-config --version | cut -d. -f1)" != "21" ]]; then
  wget -q https://apt.llvm.org/llvm.sh -O /tmp/llvm.sh
  chmod +x /tmp/llvm.sh
  sudo /tmp/llvm.sh 21
  sudo apt-get install -y libmlir-21-dev mlir-21-tools
  sudo update-alternatives --install /usr/bin/llvm-config llvm-config /usr/lib/llvm-21/bin/llvm-config 1
fi

if ! command -v nvcc >/dev/null 2>&1 || ! nvcc --version | grep -q "release 13.2"; then
  wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb -O /tmp/cuda-keyring.deb
  sudo dpkg -i /tmp/cuda-keyring.deb
  sudo apt-get update
  sudo apt-get install -y cuda-toolkit-13-2
fi

grep -q "CUDA_TOOLKIT_PATH=/usr/local/cuda-13.2" "$HOME/.bashrc" || echo 'export CUDA_TOOLKIT_PATH=/usr/local/cuda-13.2' >> "$HOME/.bashrc"
grep -q "CUDA_TILE_USE_LLVM_INSTALL_DIR=/usr/lib/llvm-21" "$HOME/.bashrc" || echo 'export CUDA_TILE_USE_LLVM_INSTALL_DIR=/usr/lib/llvm-21' >> "$HOME/.bashrc"
grep -q "PATH=/usr/local/cuda-13.2/bin" "$HOME/.bashrc" || echo 'export PATH=/usr/local/cuda-13.2/bin:$PATH' >> "$HOME/.bashrc"
EOF

echo "Running cutile smoke binary on EC2..."
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${PUBLIC_IP}" "bash -lc '
set -euo pipefail
source ~/.cargo/env
export CUDA_TOOLKIT_PATH=/usr/local/cuda-13.2
export CUDA_TILE_USE_LLVM_INSTALL_DIR=/usr/lib/llvm-21
export PATH=/usr/local/cuda-13.2/bin:\$PATH
cd ${REMOTE_DIR}/rust-ingest
if ! command -v nvidia-smi >/dev/null 2>&1; then
  echo \"nvidia-smi missing. Ensure AMI has NVIDIA driver or install it before running cutile.\"
  exit 1
fi
if ! command -v tileiras >/dev/null 2>&1; then
  echo \"tileiras not found on PATH. Expected in /usr/local/cuda-13.2/bin.\"
  echo \"Check CUDA 13.2 install or install CUDA components that include tileiras.\"
  exit 1
fi
nvidia-smi
nvcc --version | head -n 1
llvm-config --version
tileiras --version || true
cargo run --bin cutile_smoke --features gpu-cutile
'"
