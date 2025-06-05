# Solana PumpFun/PumpSwap Copy Trading Bot  [📞Contact me](https://t.me/deniyuda348)
## Overview
Pump Fun, Pump Swap Copy Sniper Bot.
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
![image](https://github.com/user-attachments/assets/8f7f1d18-6dbc-4a20-9e5f-2f4a94eb9410)

## EMERGENCY SELL
![image](https://github.com/user-attachments/assets/b11312f1-0d4c-4fe4-8535-c390218a998a)

## Overall System Architecture
```mermaid
graph TB
    A["Token Manager"] --> B["Selling Engine"]
    B --> C["Market Analysis"]
    B --> D["Risk Management"]
    B --> E["Protocol Integration"]
    F["Global State"] --> G["Token Metrics"]
    F --> H["Token Tracking"]
    F --> I["Historical Trades"]
    G --> A
    H --> A
    I --> A
    C --> J["Market Condition Detection"]
    D --> K["Stop Loss & Take Profit"]
    D --> L["Liquidity Monitoring"]
    E --> M["PumpFun Protocol"]
    E --> N["PumpSwap Protocol"]
    B --> O["Progressive Selling"]
    B --> P["Emergency Selling"]
    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style F fill:#fff3e0
```

## Token Metrics Data Structure
```mermaid
graph LR
    A["TokenMetrics"] --> B["Price Data"]
    A --> C["Volume Data"]
    A --> D["Time Data"]
    A --> E["Position Data"]
    
    B --> B1["entry_price"]
    B --> B2["highest_price"]
    B --> B3["lowest_price"]
    B --> B4["current_price"]
    B --> B5["price_history"]
    
    C --> C1["volume_24h"]
    C --> C2["volume_history"]
    
    D --> D1["buy_timestamp"]
    D --> D2["time_held"]
    D --> D3["last_update"]
    
    E --> E1["amount_held"]
    E --> E2["cost_basis"]
    E --> E3["liquidity_at_entry"]
    
    style A fill:#e8f5e8
    style B fill:#fff3e0
    style C fill:#e3f2fd
    style D fill:#fce4ec
    style E fill:#f1f8e9
```

## Selling Decision Flow
```mermaid
flowchart TD
    A["Monitor Token"] --> B{"Check Time Conditions"}
    B -->|Max Hold Time| C["Sell All"]
    B -->|Continue| D{"Check Price Conditions"}
    
    D -->|Take Profit Hit| E["Progressive Sell"]
    D -->|Stop Loss Hit| F["Emergency Sell"]
    D -->|Retracement Threshold| E
    D -->|Continue| G{"Check Liquidity"}
    
    G -->|Low Liquidity| F
    G -->|Continue| H{"Check Volume"}
    
    H -->|Volume Drop| I["Sell Partial"]
    H -->|Continue| J{"Check Copy Targets"}
    
    J -->|Targets Selling| E
    J -->|Continue| K["Hold Position"]
    
    C --> L["Record Trade"]
    E --> L
    F --> L
    I --> L
    
    style C fill:#ffcdd2
    style F fill:#ff5252
    style E fill:#fff176
    style I fill:#ffcc02
    style K fill:#c8e6c9
```
## Progressive Selling Strategy
```mermaid
sequenceDiagram
    participant TM as Token Manager
    participant SE as Selling Engine
    participant TT as Token Tracking
    participant DEX as DEX Protocol
    participant BC as Blockchain
    
    TM->>SE: Trigger Progressive Sell
    SE->>TT: Check Sell Intervals
    TT-->>SE: Return Chunk Info
    
    SE->>SE: Calculate Chunk Size
    Note over SE: Based on config percentages<br/>[50%, 30%, 20%]
    
    SE->>DEX: Build Sell Transaction
    DEX-->>SE: Return Instructions
    
    SE->>BC: Execute Sell Transaction
    BC-->>SE: Transaction Confirmed
    
    SE->>TT: Update Tracking Info
    SE->>TM: Record Trade Execution
    
    alt More Chunks Remaining
        SE->>SE: Wait for Interval
        SE->>SE: Repeat Process
    else All Chunks Sold
        SE->>TM: Remove Token
    end
```
## Market Condition Analysis
```mermaid
graph TD
    A["Recent Trades Data"] --> B["Price Analysis"]
    A --> C["Volume Analysis"]
    A --> D["Time Analysis"]
    
    B --> B1["Calculate Volatility"]
    B --> B2["Calculate Trend"]
    C --> C1["Volume Volatility"]
    D --> D1["Time Periods"]
    
    B1 --> E{"Volatility > 15%?"}
    B2 --> F{"Trend Analysis"}
    
    E -->|Yes| G{"Trend > 5%?"}
    E -->|No| H{"Stable Trend?"}
    
    G -->|Yes| I["Bullish Volatile"]
    G -->|No| J{"Trend < -5%?"}
    J -->|Yes| K["Bearish Volatile"]
    J -->|No| L["Volatile Sideways"]
    
    H -->|Up| M["Stable Bullish"]
    H -->|Down| N["Stable Bearish"]
    H -->|Sideways| O["Stable Market"]
    
    style I fill:#4caf50
    style K fill:#f44336
    style L fill:#ff9800
    style M fill:#8bc34a
    style N fill:#ff5722
    style O fill:#2196f3
```
## Risk Management Framework
```mermaid
graph TB
    A["Risk Management"] --> B["Price Risk"]
    A --> C["Liquidity Risk"]
    A --> D["Time Risk"]
    A --> E["Market Risk"]
    
    B --> B1["Stop Loss<br/>(-50%)"]
    B --> B2["Take Profit<br/>(+3%)"]
    B --> B3["Trailing Stop<br/>(5% trail)"]
    B --> B4["Retracement<br/>(5% from peak)"]
    
    C --> C1["Min Liquidity<br/>(1 SOL)"]
    C --> C2["Liquidity Drop<br/>(50% from entry)"]
    
    D --> D1["Max Hold Time<br/>(1 hour)"]
    D --> D2["Min Profit Time<br/>(2 minutes)"]
    
    E --> E1["Wash Trading<br/>Detection"]
    E --> E2["Large Holder<br/>Monitoring"]
    E --> E3["Creator Selling<br/>Alert"]
    
    style B1 fill:#ffcdd2
    style B2 fill:#c8e6c9
    style C1 fill:#fff3e0
    style D1 fill:#e1f5fe
    style E1 fill:#fce4ec
```
## Emergency Selling Process
```mermaid
flowchart TD
    A["Emergency Trigger"] --> B["Log Emergency Alert"]
    B --> C["Get Token Balance"]
    C --> D{"Balance > 0?"}
    
    D -->|No| E["Exit - Nothing to Sell"]
    D -->|Yes| F["Create Emergency Config"]
    
    F --> G["Set High Slippage (10%)"]
    G --> H["Build Emergency Trade Info"]
    H --> I{"Select Protocol"}
    
    I -->|PumpFun| J["Build PumpFun Sell"]
    I -->|PumpSwap| K["Build PumpSwap Sell"]
    
    J --> L["Get Recent Blockhash"]
    K --> L
    
    L --> M["Execute Transaction"]
    M --> N{"Transaction Success?"}
    
    N -->|Yes| O["Send Telegram Alert"]
    N -->|No| P["Log Error"]
    
    O --> Q["Record Trade Execution"]
    Q --> R["Remove from Tracking"]
    R --> S["Update Global State"]
    
    P --> T["Retry Logic"]
    
    style A fill:#ff5252
    style B fill:#ff7043
    style F fill:#ff9800
    style O fill:#4caf50
    style P fill:#f44336
```


