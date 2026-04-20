# AWS GPU (Terraform)

This folder provisions a temporary NVIDIA GPU EC2 host for running:

```bash
cargo run --bin cutile_smoke --features gpu-cutile
```

## Quick Start

```bash
cp terraform.tfvars.example terraform.tfvars
# edit values as needed

./up.sh
./run-cutile-smoke.sh
./down.sh
```

## Notes

- SSH key pair is generated automatically by Terraform and written to `.ssh/`.
- `down.sh` maps to `terraform destroy -auto-approve`; always run it when done.
