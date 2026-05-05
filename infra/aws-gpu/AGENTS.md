# AGENTS.md — driving the AWS GPU stand-up end to end

**Audience:** an LLM agent (Claude Code, Cursor, Codex, etc.) helping a contributor
spin up a temporary NVIDIA GPU on AWS to run the `cutile_smoke` binary.
Doubles as a checklist for a human walking through the same flow.

The companion `README.md` documents the *Terraform* setup. This file documents
the *agent-driven workflow*: how to onboard someone who has never touched AWS,
how to handle credentials safely, and what the agent must never do.

---

## Hard rules (non-negotiable)

These are also enforced by the PreToolUse hook in `.claude/settings.json`
(`secret-guard.sh`). Obey them whether or not the hook is active — defense in
depth.

1. **Never read secret files.** No `Read`, `cat`, `head`, `tail`, `less`, `bat`,
   `xxd`, `base64`, `cp`, `mv`, `tee` against:
   - `infra/aws-gpu/terraform.tfvars`
   - `infra/aws-gpu/.ssh/*.pem`
   - `~/.aws/credentials`
   - `~/.aws/config`
   - any `*.pem`

   To confirm a file exists, use `test -f <path> && echo exists`.

2. **Never put credentials on stdout.** Do not run `aws iam create-access-key`,
   `printenv AWS_*`, `env | grep AWS_*`, `echo $AWS_SECRET_ACCESS_KEY`, or
   `terraform output -raw <sensitive_value>` directly. The hook blocks the
   common cases; you are still responsible for not inventing new ones.

3. **Never set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
   in subprocess env.** Pass `--profile <name>` to the AWS CLI; Terraform reads
   the named profile via `var.aws_profile`. The agent only ever knows the
   profile *name*, never the key material.

4. **Never ask the user to paste their access key or secret into the chat.**
   Have them run `aws configure --profile <name>` in their own terminal.

5. **Never run `terraform apply` or `up.sh` without explicit user approval.**
   `up.sh` runs `apply -auto-approve`, so getting the user's go-ahead first is
   the actual safety gate. *Optionally* offer to run `terraform plan -out=tfplan`
   beforehand if the user is new to Terraform or wants to preview the change
   set — but **don't make it a hard requirement**, since it adds a step that
   discourages users who already know what `up.sh` does. The canonical flow is
   "user approves → run `./up.sh`."

6. **Always end with a teardown reminder.** A `g5.xlarge` runs ~$1/hr. Prompt
   the user to run `./down.sh` after success or failure.

---

## Persona triage — start here

Before doing anything, ask the user (one `AskUserQuestion`) which case they're
in:

| Case | Description | Skip to |
|------|-------------|---------|
| A | I have an AWS account, an IAM user with access keys, and `aws configure --profile <name>` already works. `aws sts get-caller-identity --profile <name>` returns my account. | [Configure terraform.tfvars](#configure-terraformtfvars) |
| B | I have an AWS account but no IAM user / access keys / AWS CLI yet. | [Persona B: Bootstrap an IAM user](#persona-b-bootstrap-an-iam-user) |
| C | I don't have an AWS account at all. | [Persona C: Create an AWS account](#persona-c-create-an-aws-account) |

---

## Persona C: Create an AWS account

The agent cannot do this — it requires a payment method and identity
verification. Tell the user, in order:

1. Go to <https://aws.amazon.com/> → **Create an AWS Account**.
2. Provide email, password, payment method, identity verification.
3. Sign in to the AWS Management Console as the **root user**.
4. **Immediately** harden the root account:
   - IAM → Users → root user → Security credentials → **Assign MFA device**.
   - Billing → Budgets → **Create budget** with a monthly cap (~$20) and email
     alert at 80%. A forgotten `g5.xlarge` is the realistic cost risk here
     (~$24/day if left running).
5. Then proceed to **Persona B**.

---

## Persona B: Bootstrap an IAM user

The whole bootstrap is automated by `infra/aws-gpu/bootstrap-iam.sh`. The only
manual step is creating one short-lived access key in the AWS Console — the
chicken-and-egg problem (you can't call IAM without already-authenticated
credentials). The script then creates the long-lived `cutile-runner` user and
its access key, then prompts you to revoke the bootstrap key.

### Why one manual step is unavoidable

You cannot call any IAM API without existing credentials. Terraform can't
solve this either — `aws_iam_access_key` writes the secret into Terraform
state, which is its own leakage problem. So someone has to make the first
credential. We make it short-lived and the same script that uses it offers to
revoke it as its final step.

### Install the CLI tools

```bash
# macOS
brew install awscli terraform jq

# verify
aws --version
terraform -version
jq --version
```

Linux: use the system package manager or the official installers. The agent
should ask the user's distro rather than guessing.

### The flow

1. **In the AWS Console (one time, ~30 seconds):**
   Sign in as your root user. Top-right account menu → **Security
   credentials** → scroll to **Access keys** → **Create access key**. Accept
   the "this is your root user" warning checkbox. Copy the Access Key ID and
   Secret Access Key. **Do not paste them into the agent chat.**

2. **In your own terminal (NOT through the agent):**
   ```bash
   aws configure --profile bootstrap-admin
   # AWS Access Key ID:     <paste>
   # AWS Secret Access Key: <paste>
   # Default region:        us-east-1
   # Default output format: json
   ```

3. **Run the bootstrap script (in your own terminal, also NOT through the agent):**
   ```bash
   cd infra/aws-gpu
   ./bootstrap-iam.sh
   ```
   Output (paraphrased):
   ```
   Configuration:
     bootstrap profile : bootstrap-admin
     bootstrap caller  : arn:aws:iam::123456789012:root
     ...
   Creating policy cuTileTerraformLeastPrivilege ...
   Creating user cutile-runner ...
   Attaching policy cuTileTerraformLeastPrivilege to cutile-runner
   Creating access key for cutile-runner and writing it to ~/.aws/credentials [cutile]
     Wrote access key AKIA... to profile [cutile] (secret not shown)
   Verifying new profile [cutile]:
   { "Account": "...", "Arn": "arn:aws:iam::...:user/cutile-runner", ... }
   Bootstrap complete.
   ```

4. **Confirm the revoke prompt:**
   ```
   ===== Revoke the bootstrap access key =====
   The bootstrap profile [bootstrap-admin] is using access key: AKIA...
   This is your AWS ROOT USER access key. Revoking it now is strongly recommended.

   Revoke this access key now? [y/N]
   ```
   Type `y`. The script calls `aws iam delete-access-key`. The
   `bootstrap-admin` profile is now non-functional. (You can leave the dead
   `[bootstrap-admin]` section in `~/.aws/credentials` or remove it manually.)

5. **Tell the agent "bootstrap done".** It will verify with
   `aws sts get-caller-identity --profile cutile` (returns Account/Arn/UserId,
   no secrets) and proceed to the GPU quota check.

### About the script

* `./bootstrap-iam.sh --help` shows all flags.
* **Idempotent** — re-running is safe; each step checks for existing state.
* `--dry-run` prints every API call it would make, without calling AWS. Use
  this to inspect behavior before running for real.
* `--destroy` removes the `cuTileTerraformLeastPrivilege` policy and the
  `cutile-runner` user's access keys. Two modes:
  * **bootstrap-admin** (preferred, fully clean): if `BOOTSTRAP_PROFILE` is
    still configured with valid admin credentials, the script also deletes
    the user object. Result: zero leftover IAM resources.
  * **self-destruct** (automatic fallback): if `BOOTSTRAP_PROFILE` is dead
    (the recommended state after initial setup), the script falls back to
    the cutile profile's own narrowly-scoped IAM permissions. Detaches and
    deletes the policy, deletes the access key — but leaves the
    `cutile-runner` user object behind, because AWS revokes the cutile
    profile's auth the moment its access key is deleted, before
    `DeleteUser` can run. The leftover user is harmless ($0/mo, no keys,
    no policies). To fully delete it, re-create admin credentials and
    re-run `--destroy`, or delete it in the IAM Console.
* The least-privilege policy lives at `infra/aws-gpu/iam-policy.json` (the
  script reads it). Edit there if you need to widen scope. It grants:
  EC2 lifecycle + describe + tags, security group + key pair management,
  SSM parameter read for the DLAMI lookup, service-quota read + request,
  STS GetCallerIdentity, and narrowly-scoped self-destruct
  (`DetachUserPolicy`, `DeleteAccessKey`, `DeleteUser`, `DeletePolicy`)
  limited to the `cutile-runner` user and the
  `cuTileTerraformLeastPrivilege` policy ARNs. No other IAM, no S3, no
  billing, no admin.

### Why the agent does not run the script

Two reasons:

1. The script captures the new IAM user's secret access key in shell variables
   and writes it to `~/.aws/credentials`. Even though it never echoes the
   secret, running it through the agent's Bash tool is unnecessary
   indirection — credentials should originate in your shell and stay there.
2. The `secret-guard` PreToolUse hook (defined in
   `.claude/hooks/secret-guard.sh`) blocks `aws iam create-access-key` when
   invoked by the agent directly. The script's normal mode is "you run it;
   the agent watches output." If a future agent must invoke it (e.g. for
   automated CI bootstrap), invoke as `bash bootstrap-iam.sh` from a
   subprocess that doesn't surface stdout to a transcript.

---

## Configure `terraform.tfvars`

Use `AskUserQuestion` to gather:

- `aws_profile` — must match the profile name they used in `aws configure`.
- `aws_region` — default `us-east-1`.
- `instance_type` — default `g5.xlarge` (1× A10G, ~23 GB VRAM, ~$1/hr).

Write `infra/aws-gpu/terraform.tfvars` using the `Write` tool. Template:

```hcl
aws_region    = "us-east-1"
aws_profile   = "<profile-name-the-user-gave>"
instance_type = "g5.xlarge"
project_name  = "opra_to_alpha"
ssh_user      = "ubuntu"
```

The file is gitignored (`.gitignore` line 82 — `infra/aws-gpu/terraform.tfvars`),
so it will not be committed. Do **not** read the file back to "verify" it.
Confirm only that it was written:

```bash
test -f infra/aws-gpu/terraform.tfvars && echo "tfvars written ($(wc -l < infra/aws-gpu/terraform.tfvars) lines)"
```

---

## Pre-apply: verify the GPU vCPU quota

Brand-new AWS accounts default to **0 vCPUs** for the *Running On-Demand G and VT
instances* quota (code `L-DB2E81BA`). A `g5.xlarge` is 4 vCPUs, so `terraform
apply` will fail with `VcpuLimitExceeded` if the quota hasn't been raised. Check
*before* applying — much faster than waiting for Terraform to fail.

```bash
aws service-quotas get-service-quota \
  --service-code ec2 \
  --quota-code L-DB2E81BA \
  --region us-east-1 \
  --profile cutile \
  --query 'Quota.[Value,Adjustable]' --output text
```

Interpret the output:

| Output | Meaning | Action |
|--------|---------|--------|
| `4.0  True` (or higher) | Quota covers at least one g5.xlarge. | Proceed to "Stand up the GPU". |
| `0.0  True` | Quota is zero. New account. | Request an increase (below). |
| `<n>  False` | Quota is non-adjustable in this account/region. | Switch region or contact AWS support. |

### Request a quota increase

Request 8 vCPUs (room for one re-launch without re-requesting):

```bash
aws service-quotas request-service-quota-increase \
  --service-code ec2 \
  --quota-code L-DB2E81BA \
  --desired-value 8 \
  --region us-east-1 \
  --profile cutile \
  --query 'RequestedQuota.[Status,Id]' --output text
```

Status will start as `PENDING`. Small increases on accounts with billing
history are often auto-approved within minutes; brand-new accounts may go to
manual review (24–48 hr — AWS emails the root account when it changes).

Poll the request status (no need to spam — every few minutes is plenty):

```bash
aws service-quotas list-requested-service-quota-change-history-by-quota \
  --service-code ec2 \
  --quota-code L-DB2E81BA \
  --region us-east-1 \
  --profile cutile \
  --query 'RequestedQuotas[0].[Status,DesiredValue,LastUpdatedAt]' --output text
```

When `Status` becomes `CASE_CLOSED` and the `get-service-quota` call from
above returns `8.0` (or whatever was approved), you're cleared to apply. If
the request is `DENIED`, AWS includes a reason — typically the account needs
some billing history first; running a smaller non-GPU instance for a few
hours, then re-requesting, usually works.

---

## Stand up the GPU

The canonical command is `./up.sh`. Get explicit user approval first, then:

```bash
cd infra/aws-gpu
./up.sh
```

`up.sh` runs `terraform init -upgrade` then `terraform apply -auto-approve`.
Takes ~2–3 minutes. When complete it prints:

```
GPU instance is up.
Public IP:  <ip>
SSH cmd:    ssh -i <path>.pem ubuntu@<ip>
```

The `.pem` file is at `infra/aws-gpu/.ssh/<key-name>.pem` (gitignored, mode
0600). The `ssh_command` output shows the path because the path itself is not
secret — only the file's contents are. The `ssh_private_key_path` output is
marked `sensitive = true` in `outputs.tf` so Terraform redacts it in plan
output.

### Optional: preview the plan first

If the user is new to Terraform or wants to see exactly what will be created
before any resources are billed, *offer* this as an extra pre-flight (don't
require it — it adds a step that discourages experienced users):

```bash
cd infra/aws-gpu
terraform init -upgrade            # cheap to re-run; first invocation downloads providers
terraform plan -out=tfplan
terraform show -no-color tfplan | grep -E '^Plan:|^\s+# ' | head -40
```

Typical output is `Plan: 5 to add, 0 to change, 0 to destroy` (EC2 instance,
security group, key pair, TLS private key, local pem file). After review the
user runs `./up.sh`. The stale `tfplan` file is ignored by `up.sh` and can be
deleted (`rm tfplan`).

---

## Run the smoke test

```bash
cd infra/aws-gpu
./run-cutile-smoke.sh
```

This script syncs the repo to the EC2 host and runs
`cargo run --bin cutile_smoke --features gpu-cutile`. Report success/failure
and the GPU vs CPU timings to the user. Expected runtime: a few minutes for
the first build (compiles cuTile-rs against CUDA 13.x on the host).

---

## Tear down — always offer

```bash
./down.sh
```

`down.sh` runs `terraform destroy`. **Always** offer this at the end of the
session, even on success. If the smoke test failed and the user wants to
investigate on the host, give them the SSH command and set an expectation:
*"Run `./down.sh` when you're finished — the instance bills hourly."*

---

## Optional cleanup of the bootstrap access key

After the user has confirmed everything works and they're not standing up GPUs
frequently, recommend they reduce ongoing risk:

- **Console:** IAM → Users → `cutile-runner` → Security credentials → Delete
  the access key. They can recreate it before the next run.
- **Or:** keep it but rotate every 90 days (IAM Access Analyzer will nag).

The realistic risk surface for this whole workflow is the long-lived access
key sitting in `~/.aws/credentials` on the user's laptop. The IAM policy above
limits the blast radius to "spin up and tear down EC2," not "drain the
account."

---

## Quick reference for the agent

```
Persona C (no AWS account)        → console signup → Persona B
Persona B (no IAM user / CLI)     → console IAM steps → aws configure (user) → verify
All personas                      → Write tfvars → terraform plan → confirm → apply
                                  → run-cutile-smoke.sh → report → offer ./down.sh
```

When in doubt: do not read the file, do not echo the variable, ask the user.
