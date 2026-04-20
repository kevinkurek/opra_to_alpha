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

echo "Syncing repo to EC2 (${PUBLIC_IP})..."
rsync -az --delete \
  --exclude '.git/' \
  --exclude 'target/' \
  --exclude '.terraform/' \
  --exclude '*.tfstate*' \
  --exclude '.DS_Store' \
  "${ROOT_DIR}/" "ubuntu@${PUBLIC_IP}:${REMOTE_DIR}/"

echo "Installing CUDA/cuTile prerequisites on EC2 (idempotent)..."
ssh "${SSH_OPTS[@]}" "ubuntu@${PUBLIC_IP}" "bash -s" <<'EOF'
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
EOF

echo "Running cutile smoke binary on EC2..."
ssh "${SSH_OPTS[@]}" "ubuntu@${PUBLIC_IP}" "bash -lc '
set -euo pipefail
source ~/.cargo/env
export CUDA_TOOLKIT_PATH=/usr/local/cuda-13.2
export CUDA_TILE_USE_LLVM_INSTALL_DIR=/usr/lib/llvm-21
cd ${REMOTE_DIR}/rust-ingest
if ! command -v nvidia-smi >/dev/null 2>&1; then
  echo \"nvidia-smi missing. Ensure AMI has NVIDIA driver or install it before running cutile.\"
  exit 1
fi
nvidia-smi
nvcc --version | head -n 1
llvm-config --version
cargo run --bin cutile_smoke --features gpu-cutile
'"
