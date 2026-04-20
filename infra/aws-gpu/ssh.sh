#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if ! command -v terraform >/dev/null 2>&1; then
  echo "Terraform is required. Install Terraform and retry."
  exit 1
fi

SSH_KEY_PATH="$(terraform -chdir="${SCRIPT_DIR}" output -raw ssh_private_key_path)"
PUBLIC_IP="$(terraform -chdir="${SCRIPT_DIR}" output -raw public_ip)"

exec ssh -i "${SSH_KEY_PATH}" -o StrictHostKeyChecking=accept-new "ubuntu@${PUBLIC_IP}"
