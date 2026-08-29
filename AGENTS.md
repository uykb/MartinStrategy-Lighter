# AGENTS.md

## Build / Verification Commands
```bash
# Build binary
go build -o bot ./cmd/bot/

# Build container image with ko
ko build --local ./cmd/bot

# Code checks
go vet ./...
go fmt ./...

# Run locally with flags
go run cmd/bot/main.go --api-key="YOUR_KEY" --api-secret="YOUR_SECRET"
```
*Note on testing: No `_test.go` files exist yet. When adding new features, write table-driven tests alongside the source and mock exchange/storage clients.*

## Concurrency & Liveness Patterns
- **TryLock Prevention**: `placeGridOrders` and `updateTP` use `gridMu.TryLock()` and `tpMu.TryLock()`. If locked, they skip execution and set dirty flags (`tpDirty`) rather than blocking the main event loop.
- **Lock Boundaries**: Keep all network/exchange API calls outside of locks (`s.mu.Lock`/`RLock`) to prevent deadlocks and network call delays from blocking the event bus.
- **State Rollback**: Roll back FSM states to their previous state if downstream network actions fail.
- **Goroutine Liveness**: All background goroutines must run inside a `defer recover()` wrapper with a 5-second sleep before self-healing restart.
- **Thread-safe Flags**: Cross-goroutine flags (`frozen`, `tpDirty`, `initialSyncDone`) must be managed via `atomic.Bool`.

## Lighter API Quirks & Limits
- **Price Precision Rules**: Price must align to the market's `supported_price_decimals` and size to `supported_size_decimals` as retrieved from `/api/v1/orderBooks`.
- **Epsilon Quantity Flooring**: To counter IEEE 754 float inaccuracies (e.g. 2.53 stored as 2.529999...), add a small epsilon (`+0.00000001`) before calling `FloorToTickSize` or `FloorToDecimals` on quantities.
- **MinNotional Protection**: If a floor-truncated order size falls below the minimum required USD value, bump it by adding exactly one `stepSize`.
- **Market Order Simulation**: Lighter lacks native market orders; simulate them via IOC limit orders using `Orderbook.SimulateMarketOrder()` to calculate depth penetration, padded by 1 `TickSize`. If the local orderbook is unavailable, fail fast and retry instead of defaulting to a naive 5% slippage bound. IOC/Market orders MUST carry `order_expiry = 0`, otherwise the SDK rejects them.
- **SkipNonce & Nonce Management**: Set `SkipNonce=1` and use a **millisecond-timestamp** nonce with an atomic CAS counter to guarantee strict monotonic increase. Per official docs the constraint `2^47-1 > new_nonce > old_nonce` must hold — **never use microsecond timestamps** (they exceed `2^47-1` and the exchange rejects them with code `21104 invalid nonce`).
- **Authentication Required (AWS WAF)**: Lighter's API is behind an AWS WAF bot challenge. Unauthenticated requests from datacenter IPs get a `405 Human Verification` captcha page. Per official docs, authenticate **every** REST request (attach the signed `authorization` header) to bypass IP-based rate limits and move limits to L1-based.
- **Rate Limits & Order Confirmation**: Standard REST = 60 requests/min. **Never use REST polling to confirm order indexes after CreateOrder**, as it quickly triggers AWS WAF 429/405 bans. Use the WebSocket Promise/Future watcher (`wsManager.WatchOrder(clientOrderIndex)`) which returns the `OrderIndex` via WS in milliseconds, with a 10s REST fallback.
- **Order Expiry Validation & Server Time**: Limit orders (GoodTillTime) require a non-zero absolute `order_expiry` timestamp in **milliseconds** (not seconds), between 5 minutes and 30 days. To prevent immediate `code 21711: invalid expiry` errors caused by local clock drift, sync the local time with the Lighter server using the HTTP `Date` header and apply the calculated `timeOffset`.

## FSM & Take Profit (TP) Rules
- **Take Profit Markup**: TP price is a fixed +0.80% (`entryPrice * 1.008`) rounded to price decimals.
- **TP Anti-Chasing (防追价)**: Skip modifying or recreating TP if `tpPrice <= marketPrice`. This prevents restart/reconnect events from immediately filling a recreated TP limit order at market.
- **ReduceOnly Required**: TP orders are SELL LIMIT orders and MUST enforce `ReduceOnly` to prevent opening unintended reverse positions.
- **Delayed TP Placement**: Wait 35s after a grid order fills before placing the TP order. This delay allows the L2 sequencer to commit the new position state to the rollup. Placing a `ReduceOnly` order before the position is fully settled on L2 causes it to be canceled (~38s later) by the L2 Margin Sweep engine.
- **TP Update Priority**: Prefer `ModifyOrder` (atomic cancel/replace). On failure, query the real exchange state using `findLiveTP()` to determine if the order actually succeeded on the exchange before attempting a manual cancel + create.
- **No Restart Grid Replacement**: On startup, query the on-chain position and re-calibrate/claim existing TP. Do not re-place grid orders to avoid doubling leverage and liquidation risk.
- **Cycle Reset**: Upon a SELL FILLED event, poll `GetPosition()` until size is 0 (up to 30s) before resetting the FSM to `IDLE`.
- **Cycle ID Protection**: A sequential `cycleID` tracks FSM generations. Asynchronous cancellations verify that `cycleID` matches before executing to avoid cancelling grid orders placed in a newer cycle.
- **CANCELED Event Recovery**: If a TP order is unexpectedly cancelled by the exchange (e.g., L2 rejection), reset the internal TP tracker `currentTPOrderID` to 0 so the strategy recreates it automatically on the next cycle.

## Connection & Synchronization Safeguards
- **Initial Sync Delay**: Ignore WS `OrderUpdate` events for the first 3 seconds after startup (`initialSyncDone` flag) to drain historical fills sent by the websocket.
- **FSM Freeze**: During WebSocket reconnect resyncs, freeze the FSM (`frozen` flag) to ignore all ticks and order updates, delaying unfreezing by 2 seconds to allow the WS history cache to drain.
- **REST Fallback**: If the WebSocket dies, the REST API polls price every 10s and publishes price ticks with local timestamps.
