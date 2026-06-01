# OpenShell

Run OAB inside an [NVIDIA OpenShell](https://github.com/NVIDIA/OpenShell) sandbox for isolated, policy-enforced execution with credential injection.

## Prerequisites

- Docker running on the host
- [OpenShell CLI](https://github.com/NVIDIA/OpenShell#install) installed

```bash
curl -LsSf https://raw.githubusercontent.com/NVIDIA/OpenShell/main/install.sh | sh
```

## Quick Start (Local Docker)

### 1. Create a sandbox

```bash
openshell sandbox create --name oab -- bash
```

### 2. Connect and install OAB

```bash
openshell sandbox connect oab
```

Inside the sandbox:

```bash
git clone https://github.com/openabdev/openab.git
cd openab
cargo build --release
```

### 3. Configure credentials

On the host, create providers for the tokens OAB needs:

```bash
# Discord bot token
export DISCORD_BOT_TOKEN="your-token"
openshell provider create --name discord --env DISCORD_BOT_TOKEN

# GitHub token
export GITHUB_TOKEN="your-token"
openshell provider create --name github --env GITHUB_TOKEN

# LLM provider (pick one)
export ANTHROPIC_API_KEY="your-key"
openshell provider create --name anthropic --env ANTHROPIC_API_KEY
```

Then recreate the sandbox with providers attached:

```bash
openshell sandbox delete oab
openshell sandbox create --name oab \
  --provider discord \
  --provider github \
  --provider anthropic \
  -- bash
```

### 4. Apply network policy

Create `oab-policy.yaml`:

```yaml
network:
  egress:
    - destination: "discord.com"
      ports: [443]
    - destination: "gateway.discord.gg"
      ports: [443]
    - destination: "api.github.com"
      ports: [443]
    - destination: "github.com"
      ports: [443]
    - destination: "api.anthropic.com"
      ports: [443]
```

Apply it:

```bash
openshell policy set oab --policy oab-policy.yaml
```

### 5. Run OAB

Inside the sandbox:

```bash
cd openab
./target/release/openab serve --config config.toml
```

## Port Forwarding

If OAB exposes a webhook endpoint (e.g., for GitHub webhooks):

```bash
openshell sandbox create --name oab \
  --forward 3000 \
  --provider discord \
  --provider github \
  -- bash
```

`localhost:3000` on the host reaches port 3000 inside the sandbox.

## BYOC (Custom Image)

For a pre-built OAB sandbox image, create a `Dockerfile`:

```dockerfile
FROM ubuntu:24.04

RUN groupadd -g 1000660000 sandbox && \
    useradd -u 1000660000 -g sandbox -m sandbox

RUN apt-get update && apt-get install -y \
    curl git iproute2 ca-certificates build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    su sandbox -c 'sh -s -- -y'

WORKDIR /home/sandbox
USER sandbox
```

```bash
openshell sandbox create --name oab \
  --from ./Dockerfile \
  --provider discord \
  --provider github \
  -- bash
```

## Cleanup

```bash
openshell sandbox delete oab
```
