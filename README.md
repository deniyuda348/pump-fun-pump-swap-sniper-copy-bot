# Solana PumpFun/PumpSwap Copy Trading Bot  [📞Contact me](https://t.me/deniyuda348)
## Overview
This is a high-performance Rust-based copy trading bot that monitors and replicates trading activity on Solana DEXs like PumpFun and PumpSwap. The bot uses advanced transaction monitoring to detect and copy trades in real-time, giving you an edge in the market.
https://github.com/deniyuda348/pump-fun-pump-swap-sniper-copy-bot/wiki
# Features:

- **Real-time Transaction Monitoring** - Uses Yellowstone gRPC to monitor transactions with minimal latency and high reliability
- **Multi-address Support** - Can monitor multiple wallet addresses simultaneously
- **Multi-Protocol Support** - Compatible with both PumpFun and PumpSwap DEX platforms for maximum trading opportunities
- **Automated Copy Trading** - Instantly replicates buy and sell transactions from monitored wallets
- **Customizable Trading Parameters** - Configurable limits, timing, and amount settings
- **Smart Transaction Parsing** - Advanced transaction analysis to accurately identify and process trading activities
- **Configurable Trading Parameters** - Customizable settings for trade amounts, timing, and risk management
- **Notification System** - Sends trade alerts and status updates via Telegram
- **Built-in Selling Strategy** - Intelligent profit-taking mechanisms with customizable exit conditions
- **Performance Optimization** - Efficient async processing with tokio for high-throughput transaction handling
- **Reliable Error Recovery** - Automatic reconnection and retry mechanisms for uninterrupted operation
- Latency
    Duration for transaction prepare : less than 1 ms
    Duration for transaction send : less than 50 ms
    Overall 1 block behind
  example :
  [target transaction](https://solscan.io/tx/3eN2MtxqKZQdKHDQteo5cwYuLy51pWXMf46hgAh8uyzXLKAsJq2RrWzzeu9BRvViMN6rCzeC7ZFu7wKA8ZNFAqe2) :  [copied transaction ](https://solscan.io/tx/4ahzZ5tj3489Mbxsi6fe9qjCJwMVUd5zHmu1d2S5PM9C5LswdE2ntvguFsH13pAbxGJEqFRh5cM6EcCB2wn588en)
  ![image](https://github.com/user-attachments/assets/d592d808-5038-4b54-a7a5-97bf2730ea58)

#### BUY
![image](https://github.com/user-attachments/assets/3af7e212-6108-4fe9-992d-b7f8e75452ec)
#### SELL
![image](https://github.com/user-attachments/assets/f70f8ca8-c965-4f5a-9aa8-70d5fb61b996)

## Project Structure

The codebase is organized into several modules:

- **engine/** - Core trading logic including copy trading, selling strategies, and transaction parsing
- **dex/** - Protocol-specific implementations for PumpFun and PumpSwap
- **services/** - External services integration including Telegram notifications
- **common/** - Shared utilities, configuration, and constants
- **core/** - Core system functionality
- **error/** - Error handling and definitions

## Setup

### Environment Variables

To run this bot, you will need to configure the following environment variables:

#### Required Variables

- `GRPC_ENDPOINT` - Your Yellowstone gRPC endpoint URL
- `GRPC_X_TOKEN` - Your Yellowstone authentication token
- `COPY_TRADING_TARGET_ADDRESS` - Wallet address(es) to monitor for trades (comma-separated for multiple addresses)

#### Telegram Notifications

To enable Telegram notifications:

- `TELEGRAM_BOT_TOKEN` - Your Telegram bot token
- `TELEGRAM_CHAT_ID` - Your chat ID for receiving notifications

#### Optional Variables

- `IS_MULTI_COPY_TRADING` - Set to `true` to monitor multiple addresses (default: `false`)
- `PROTOCOL_PREFERENCE` - Preferred protocol to use (`pumpfun`, `pumpswap`, or `auto` for automatic detection)
- `TIME_EXCEED` - Time limit for volume non-increasing in seconds
- `COUNTER_LIMIT` - Maximum number of trades to execute
- `MIN_DEV_BUY` - Minimum SOL amount for buys
- `MAX_DEV_BUY` - Maximum SOL amount for buys

## Usage

```bash
# Build the project
cargo build --release

# Run the bot
cargo run --release
```

Once started, the bot will:

1. Connect to the Yellowstone gRPC endpoint
2. Monitor transactions from the specified wallet address(es)
3. Automatically copy buy and sell transactions as they occur
4. Send notifications via Telegram for detected transactions and executed trades

## Recent Updates

- Added PumpSwap notification mode (can monitor without executing trades)
- Implemented concurrent transaction processing using tokio tasks
- Enhanced error handling and reporting
- Improved selling strategy implementation

## Contact

For questions or support, please contact the developer.
