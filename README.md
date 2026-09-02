# MartinStrategy-Lighter

A production-hardened, event-driven Martingale perpetual futures trading bot for **Lighter Exchange**, built in pure Go.

## Overview

MartinStrategy-Lighter implements a Martingale grid strategy using an **Event-Driven Finite State Machine (ED-FSM)** architecture optimized for 24/7 unattended operation. It trades Lighter perpetual contracts via WebSocket market data with REST API fallback.

**Key design principles:**
- **Go-native concurrency** — zero CGO, pure Go; WebSocket-primary with REST degradation
- **ED-FSM architecture** — strictly sequential FSM transitions eliminate race conditions
- **Three-layer connection stability** — active heartbeat → exponential-backoff reconnect → REST resync with FSM freeze
- **Lighter Integration** — USDC settlement, dynamic size and price precision via API, off-chain transaction signing (Schnorr/Goldilocks) using the Go SDK, SkipNonce validation mode.
- **Container-Ready Configuration** — parameter parsing powered by `kong`, supporting both environment variables and direct command-line arguments (Flags) for easy deployment using `ko` or traditional Docker setups.

## Architecture

```
                             Lighter Exchange
                         ┌─────────────────────┐
                         │  REST API  │  WS API │
                         └─────┬──────┴───┬────┘
                               │           │
                     ┌─────────▼───┐  ┌─────▼──────────┐
                     │ REST Client │  │   WSManager     │
                     │ (degraded)  │  │                  │
                     │             │  │ ┌──────────────┐ │
                     │ GetPosition │  │ │ 3-Layer      │ │
                     │ GetBalance  │  │ │ Stability    │ │
                     │ CreateOrder │  │ │ ──────────── │ │
                     │ CancelOrder │  │ │ ① Heartbeat  │ │
                     │ OpenOrders  │  │ │ ② Reconnect  │ │
                     └──────┬──────┘  │ │ ③ REST Resync│ │
                            │         │ └──────────────┘ │
                            │         │                    │
               IsWSActive() │         │ ┌──────────────┐ │
               ┌────────────┤         │ │ Dual Channel  │ │
               │ Skip REST  │         │ │ ──────────── │ │
               │ when WS up │         │ │ priceEventCh │ │
               │            │         │ │ orderEventCh │ │
               └────────────┘         │ └──────┬───────┘ │
                                      └────────┼─────────┘
                                               │
                               ┌───────────────┼───────────────┐
                               │               │               │
                      EventTick      EventOrderUpdate   EventResyncStart/End
                               │               │               │
                               ▼               ▼               ▼
                     ┌─────────────────────────────────────────────┐
                     │            EventBus (1000 buffer)           │
                     │                                             │
                     │  ★ Strict sequential execution (no races)   │
                     │  ★ defer recover() + 5s self-healing        │
                     └──────────────────┬──────────────────────────┘
                                        │
                                        ▼
                     ┌─────────────────────────────────────────────┐
                     │          MartingaleStrategy (FSM)            │
                     │                                             │
                     │   IDLE ──▶ PLACING_GRID ──▶ IN_POSITION ──┐ │
                     │      ▲                                  ◀─┘ │
                     │      └─── TP fill / manual close ──────────┘│
                     │                                             │
                     │  ★ frozen — pause during REST resync        │
                     │  │ initialSyncDone — filter historical fills│
                     │  ★ IsStale(2s) — discard stale prices       │
                     │  ★ 3-retry jittered backoff on orders       │
                     │  ★ FloorToDecimals — floor-quantity safety  │
                     └─────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- **Go 1.25+**
- A Lighter account (master or sub-account)
- API Key Private Key + Account Index (or L1 Wallet Address)

### Configuration

Parameters can be passed through **environment variables** or as **command-line arguments (Flags)**.

| Parameter | Command Flag | Env Variable | Default | Description |
|-----------|--------------|--------------|---------|-------------|
| API Key | `--api-key` | `MARTIN_EXCHANGE_API_KEY` | *(Required)* | Lighter API Key private key (hex) |
| Account | `--account` | `MARTIN_EXCHANGE_ACCOUNT` | *(Required)* | Lighter Account Index or L1 Wallet Address |
| API Key Index | `--api-key-index` | `MARTIN_EXCHANGE_API_KEY_INDEX` | `2` | Lighter API Key Index (created with API Key) |
| Testnet | `--use-testnet` | `MARTIN_EXCHANGE_USE_TESTNET` | `false` | Set to true to run on Lighter Testnet |
| Log Level | `--log-level` | `MARTIN_LOG_LEVEL` | `info` | Logging level (`debug`, `info`, `warn`, `error`) |
| Health Addr | `--health-addr` | `MARTIN_HEALTH_ADDR` | `:8080` | Liveness & Readiness check server address |

### Build & Run (Binary)

```bash
# Install dependencies & tidy modules
go mod tidy

# Build binary
go build -o bot cmd/bot/main.go

# Option A: Run using Command Flags
./bot --api-key="YOUR_HEX_API_KEY" --account="YOUR_ACCOUNT_INDEX" --api-key-index=8 --use-testnet

# Option B: Run using Environment Variables
export MARTIN_EXCHANGE_API_KEY="YOUR_HEX_API_KEY"
export MARTIN_EXCHANGE_ACCOUNT="YOUR_ACCOUNT_INDEX"
export MARTIN_EXCHANGE_API_KEY_INDEX=8
export MARTIN_EXCHANGE_USE_TESTNET="true"
./bot
```

### Containerization with `ko`

You can compile and containerize the bot into a clean, minimal image using `ko`:

```bash
# Build and load image into local Docker daemon
ko build --local ./cmd/bot

# Run container with Flags
docker run --rm ko.local/bot:latest --api-key="YOUR_HEX_API_KEY" --account="YOUR_ACCOUNT_INDEX" --api-key-index=8

# Run container with Environment Variables
docker run --rm -e MARTIN_EXCHANGE_API_KEY="YOUR_HEX_API_KEY" -e MARTIN_EXCHANGE_ACCOUNT="YOUR_ACCOUNT_INDEX" -e MARTIN_EXCHANGE_API_KEY_INDEX=8 ko.local/bot:latest
```

### Health Checks

```bash
curl http://localhost:8080/healthz   # liveness (process alive = 200)
curl http://localhost:8080/readyz    # readiness (WS active + FSM not frozen = 200)
```

## Strategy

### Core Trading Logic

1. **Entry** — When IDLE and a tick arrives, place a market buy order (simulated via IOC limit order, with the protected price computed from the local orderbook depth + 1 tick) for instant fill. If the local orderbook is unavailable, the order fails fast and is retried — there is no naive 5% slippage fallback.
2. **Grid deployment** — After the base order fills, place 9 limit buy orders below the entry price using fixed percentage steps.
3. **Dynamic take-profit** — A single TP sell order always covers the full on-chain position size. TP price = entry price × 1.008 (+0.80%).
4. **Safety-order sync** — Every time a grid order fills, the bot re-fetches the on-chain position and updates both TP quantity and price.
5. **Cycle reset** — When the TP fills (position → 0), all remaining grid orders are cancelled and the bot returns to IDLE.

### FSM States

```
┌──────────┐       tick event      ┌───────────────┐
│   IDLE   │ ────────────────────▶ │ PLACING_GRID  │
│ (no pos) │                       │ (positioning) │
└──────────┘                       └───────┬───────┘
     ▲                                     │
     │                                     │ position detected
     │                                     ▼
     │                             ┌───────────────┐
     │       TP fill (SELL)        │ IN_POSITION   │
     └──────────────────────────── │  (in trade)   │
                                   │               │
                                   │ safety fills  │
                                   ▼               │
                              update TP            │
                                   ▲               │
                                   └───────────────┘
```

| State | Description | Entered by | Exited by |
|-------|-------------|------------|-----------|
| `IDLE` | Waiting, no position | Startup / TP fill / manual close | Tick event |
| `PLACING_GRID` | Base order submitted, awaiting fill | Market buy executed | Position detected |
| `IN_POSITION` | Holding position, grid active | Base order filled | TP fill / manual close |

### Grid Spacing

9 levels with fixed percentage steps relative to the **previous level's price**:

| Level | Step Down | Cumulative from Entry |
|-------|-----------|----------------------|
| 1 | −1.0% | −1.0% |
| 2 | −1.0% | −2.0% |
| 3 | −1.0% | −3.0% |
| 4 | −1.1% | −4.0% |
| 5 | −2.1% | −6.0% |
| 6 | −2.2% | −8.1% |
| 7 | −4.5% | −12.3% |
| 8 | −4.8% | −16.5% |
| 9 | −7.7% | −23.0% |

### Position Sizing

Base order = `balance × 6%`, minimum $10 USDC. Safety order sizing is configured explicitly to match specific asset characteristics:

| Level | Asset Allocation % |
|-------|--------------------|
| 1 (base) | 6% |
| 2 (Grid 1) | 3% |
| 3 (Grid 2) | 3% |
| 4 (Grid 3) | 5% |
| 5 (Grid 4) | 5% |
| 6 (Grid 5) | 18% |
| 7 (Grid 6) | 32% |
| 8 (Grid 7) | 56.7% |
| 9 (Grid 8) | 57.8% |
| 10 (Grid 9+) | 116% |

### Take-Profit

- **Size**: `FloorToDecimals(|position.Size|, sizeDecimals)` — always equals the full on-chain position.
- **Price**: `entryPrice × 1.008` (+0.80%), formatted to Lighter price decimals.
- **Update**: triggered every time a safety order fills.
- **Replacement**: prefers `ModifyOrder` (atomic) and falls back to cancel + create on failure.

### Cold-Start with Existing Position

When the bot restarts and detects an on-chain position:

1. **Do not touch existing grid orders** — on-chain orders are the source of truth.
2. **Read real on-chain position** via REST API (entry price and size).
3. **Restore TP** if missing; if present, initialize tracking state to prevent unnecessary re-creation.

> **Safety principle**: Never re-place grid orders on restart. If 5 safety layers have already filled, re-placing 9 orders would result in 14 layers total — extreme leverage and liquidation risk.

## Quantity Precision (Floor Truncation)

All token quantity calculations strictly use **floor truncation** to prevent insufficient-funds rejections and ghost residual positions:

| Function | Purpose | Example |
|----------|---------|---------|
| `FloorToDecimals(qty, decimals)` | Truncate to size decimals | 0.6666 → 0.666 (for 3 decimals) |
| `FloorToTickSize(qty, tick)` | Align to tick size | 0.1666 → 0.16 |

When floor truncation drops the order value below $10, the bot automatically bumps it by one `stepSize`.

## Exchange Integration

### Authentication (Bypass AWS WAF)
Lighter's API is protected by an AWS WAF bot challenge. Datacenter/cloud IPs that send **unauthenticated** requests receive a `405 Human Verification` captcha page. Per [official docs](https://apidocs.lighter.xyz/docs/rate-limits):

> "To bypass IP-based rate limits, clients can authenticate each request so that only L1-based rate limits apply."

The adapter therefore signs an auth token and attaches an `authorization` header to **every** REST request (including read-only queries), which both bypasses the WAF and moves rate limiting from IP-based to L1-based.

### Nonce Management (SkipNonce)
To avoid concurrent nonce collisions when placing multiple grid orders simultaneously, the Lighter adapter uses **SkipNonce=1** mode. Per official docs, with SkipNonce the constraint `2^47-1 > new_nonce > old_nonce` must hold.

- The nonce is a **millisecond timestamp** (≈1.75e12, well below `2^47-1` ≈ 1.4e14).
- An **atomic compare-and-swap counter** guarantees strictly increasing nonces even when multiple orders are placed in the same millisecond (e.g. 9 grid levels).
- ⚠️ Do **not** use microsecond timestamps: they exceed `2^47-1` and the exchange rejects them with error `21104 invalid nonce`.

### Rate Limits & Cooldowns
Key limits from the [official rate-limits docs](https://apidocs.lighter.xyz/docs/rate-limits):

| Limit | Value |
|-------|-------|
| Standard REST API | 60 weighted requests / minute |
| `sendTx` / `sendTxBatch` (Standard) | 60 / minute (part of the same bucket) |
| Transaction type limit (default) | 40 requests / minute |
| Firewall (AWS WAF) cooldown | 60 seconds static |
| WebSocket messages sent / minute | 200 |

The strategy enforces a **60-second entry cooldown** after any failed entry attempt, guaranteeing at most 1 `createOrder` per minute — far below the 40/min transaction limit and fully inside the firewall cooldown window. Rate-limit errors surface as `HTTP 429` / `HTTP 405` or API code `23000 (Too Many Requests)`.

### Market Order Simulation
Lighter does not support native market orders on matching engine. They are simulated using IOC (Immediate-Or-Cancel) limit orders whose protected price is computed by `Orderbook.SimulateMarketOrder()` (depth penetration) padded by 1 `TickSize`. If the local orderbook is unavailable or liquidity is insufficient, the order fails fast and is retried — a naive 5% slippage bound is deliberately avoided. IOC market orders must carry `order_expiry = 0`, otherwise the SDK rejects them with `OrderExpiry is invalid`.

## Tech Stack

| Component | Library |
|-----------|---------|
| Language | Go 1.25+ |
| Parameter Parser | github.com/alecthomas/kong |
| Exchange SDK | github.com/elliottech/lighter-go |
| WebSocket | gorilla/websocket |
| Logging | Zap |

## License

MIT License
