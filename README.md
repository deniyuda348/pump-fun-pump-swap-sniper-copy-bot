# Solana Shredstream Decoder Client

This is a performance-focused Rust client that decodes Solana shred packets directly from Turbine block propagation layer. Unlike Geyser RPC or standard RPC requests, this client ensures you get transactions the instant they are propagated by the leader validator, and this provides you with an speed advantage.

This decoder deserializes `buy`, `create` transactions from Pumpfun and token migrations from Pumpfun -> Raydium when the `initialize2` instruction is involved and the migration pubkey (`39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg`) is also involved.

# Features:  

- **Optimized-advanced UDP buffer**  – To prevent packet loss, the application binds a raw UDP socket and configures `SO_RCVBUF` to a larger size, allowing it to handle bursts of incoming shreds without dropping packets.
- **Deserializing** – Pulls transactions directly from raw shreds so that you receive them before they are visible on Geyser RPC. It can be used with Jito Shredstream Proxy or directly with your validator node.
- **Parallel processing** – Using `tokio` for I/O operations and `rayon` for CPU bound as multi-threading, DashMap for concurrency, and Mimalloc for memory allocation optimization.
- **Integrated gRPC Server** – Streams decoded transactions directly to your listener/bot or trading strategy via GRPC under 0.1 ms latency if it runs on same machine as the listener.
- **Reed-Solomon FEC Recovery** – Ensures complete block reconstruction so that there is no loss of any transaction if any `Data` shred is missing.
- **Slot transactions stats** – Automatically logs extracted transactions per slot and succesefull/failed FecBlocks statistics to `slot_txns_stats.json`. In 99% of cases all transactions from a slot are received, it doesn't skip txns.
- **Slot Delta Calculation** – For debugging, it uses gRPC to get current slot updates to calculate a Slot Delta with the slot of the completed `FecBlocks`.

# Who is it for?

- Bot users looking for the fastest transaction feed possible for Pumpfun or Raydium (Sniping, Arbitrage, etc).
- Validators who want an edge by decoding shreds locally.

# Setting up

## Environment Variables

Before run, you will need to add the following environment variables to your `.env` file:

- `GRPC_ENDPOINT` - Your Geyser RPC endpoint url.

- `GRPC_X_TOKEN` - Leave it set to `None` if your Geyser RPC does not require a token for authentication.

- `UDP_BUFFER_SOCKET` - Default is set to `0.0.0.0:8002`. It can also be `8001` depending on what port is returned by running `get_tvu_port.sh` from Jito Shredstream Proxy. You can also use your validator's shred receiving port if you use this decoder to deserialize the shreds received by your validator.

- `GRPC_SERVER_ENDPOINT` - The address of its gRPC server. By default is set at `0.0.0.0:50051`.

## Run Command

```
RUSTFLAGS="-C target-cpu=native" RUST_LOG=info cargo run --release --bin shredstream-decoder
```

# Source code

If you are really interested in the source code, please contact me for details and demo on Discord: `.xanr`.

# Copy Trading Bot

This bot monitors transactions from specified wallet addresses and copies their trading activity on Solana DEXs like PumpFun and PumpSwap.

## Features

- Real-time transaction monitoring using Yellowstone gRPC
- Copy trading for PumpFun and PumpSwap protocols
- Automatic detection of buy and sell transactions
- Telegram notifications for trade events
- Customizable settings via environment variables

## Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and fill in your settings
3. Install Rust if not already installed
4. Build the project with `cargo build --release`
5. Run the bot with `cargo run --release`

## Environment Variables

### Required Variables

- `GRPC_ENDPOINT`: Your Yellowstone gRPC endpoint URL
- `GRPC_X_TOKEN`: Your Yellowstone authentication token
- `COPY_TRADING_TARGET_ADDRESS`: Wallet address(es) to monitor for trades (comma-separated for multiple addresses)

### Telegram Notifications

To enable Telegram notifications:

1. Create a Telegram bot using BotFather and get the bot token
2. Find your chat ID (you can use the @userinfobot)
3. Set the following environment variables:
   - `TELEGRAM_BOT_TOKEN`: Your Telegram bot token
   - `TELEGRAM_CHAT_ID`: Your chat ID

### Optional Variables

- `IS_MULTI_COPY_TRADING`: Set to `true` to monitor multiple addresses (default: `false`)
- `PROTOCOL_PREFERENCE`: Preferred protocol to use (`pumpfun`, `pumpswap`, or `auto` for automatic detection)
- `TIME_EXCEED`: Time limit for volume non-increasing in seconds
- `COUNTER_LIMIT`: Maximum number of trades to execute
- `MIN_DEV_BUY`: Minimum SOL amount for buys
- `MAX_DEV_BUY`: Maximum SOL amount for buys

## Usage

After starting the bot, it will:

1. Connect to the Yellowstone gRPC endpoint
2. Monitor transactions from the specified target address(es)
3. Automatically copy buy and sell transactions
4. Send notifications via Telegram for each detected transaction and executed trade

## Supported Protocols

- PumpFun
- PumpSwap
