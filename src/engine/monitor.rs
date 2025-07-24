use maplit::hashmap;
use anchor_client::solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{collections::HashSet, time::Duration};
use base64;
use bs58;

use super::swap::{SwapDirection, SwapInType};
use crate::common::config::{
    JUPITER_PROGRAM,
    OKX_DEX_PROGRAM,
    PUMP_FUN_PROGRAM_DATA_PREFIX,
    PUMP_FUN_BUY_LOG_INSTRUCTION,
    PUMP_FUN_BUY_OR_SELL_PROGRAM_DATA_PREFIX,
    PUMP_FUN_SELL_LOG_INSTRUCTION,
    PUMP_SWAP_BUY_LOG_INSTRUCTION,
    PUMP_SWAP_BUY_PROGRAM_DATA_PREFIX,
    PUMP_SWAP_SELL_LOG_INSTRUCTION,
    PUMP_SWAP_SELL_PROGRAM_DATA_PREFIX,
};
use crate::common::{    
    config::{AppState, SwapConfig},
    logger::Logger,
};
use crate::core::tx;
use crate::dex::pump_fun::{Pump, INITIAL_VIRTUAL_SOL_RESERVES, INITIAL_VIRTUAL_TOKEN_RESERVES,   PUMP_FUN_CREATE_IX_DISCRIMINATOR, PUMP_PROGRAM, get_bonding_curve_account};
use anyhow::{Result};
use chrono::{Utc, Local};
use colored::Colorize;
use futures_util::stream::StreamExt;
use futures_util::{SinkExt, Sink};
use futures;
use tokio::{
    time::{self, Instant},
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestPing,
    SubscribeRequestFilterTransactions, SubscribeUpdateTransaction, SubscribeUpdate,
};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum InstructionType {
    PumpMint,
    PumpBuy,
    PumpSell,
    PumpSwapBuy,
    PumpSwapSell
}

#[derive(Clone, Debug)]
pub struct BondingCurveInfo {
    pub bonding_curve: Pubkey,
    pub new_virtual_sol_reserve: u64,
    pub new_virtual_token_reserve: u64,
}

#[derive(Clone, Debug)]
pub struct PoolInfo {
    pub pool_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub pool_base_token_account: Pubkey,
    pub pool_quote_token_account: Pubkey,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub coin_creator: Pubkey,
}

#[derive(Clone, Debug)]
pub struct TradeInfoFromToken {
    pub instruction_type: InstructionType,
    pub slot: u64,
    pub recent_blockhash: Hash,
    pub signature: String,
    pub target: String,
    pub mint: String,
    pub bonding_curve: String,
    pub volume_change: i64,
    pub bonding_curve_info: Option<BondingCurveInfo>,
    pub pool_info: Option<PoolInfo>,
    pub token_amount: f64,
    pub amount: Option<u64>,
    pub max_sol_cost: Option<u64>,
    pub min_sol_output: Option<u64>,
    pub base_amount_in: Option<u64>,
    pub min_quote_amount_out: Option<u64>,
    pub base_amount_out: Option<u64>,
    pub max_quote_amount_in: Option<u64>,
}

pub struct FilterConfig {
    program_ids: Vec<String>,
    _instruction_discriminators: &'static [&'static [u8]],
    copy_trading_target_addresses: Vec<String>,
    is_multi_copy_trading: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct RetracementLevel {
    pub percentage: u64,
    pub threshold: u64,
    pub sell_amount: u64,
}

#[derive(Clone, Debug)]
pub struct TokenTrackingInfo {
    pub top_pnl: f64,
    pub last_sell_time: Instant,
    pub completed_intervals: HashSet<String>,
}

#[derive(Clone, Debug)]
pub struct CopyTradeInfo {
    pub slot: u64,
    pub recent_blockhash: Hash,
    pub signature: String,
    pub target: String,
    pub mint: String,
    pub bonding_curve: String,
    pub volume_change: i64,
    pub bonding_curve_info: Option<BondingCurveInfo>,
}

lazy_static::lazy_static! {
    static ref COUNTER: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    static ref SOLD: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    static ref BOUGHTS: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    static ref LAST_BUY_PAUSE_TIME: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    static ref BUYING_ENABLED: Arc<Mutex<bool>> = Arc::new(Mutex::new(true));
    static ref TOKEN_TRACKING: Arc<Mutex<HashMap<String, TokenTrackingInfo>>> = Arc::new(Mutex::new(HashMap::new()));
    
    static ref THRESHOLD_BUY: Arc<Mutex<u64>> = Arc::new(Mutex::new(
        std::env::var("THRESHOLD_BUY")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1_000_000_000)
    ));
    
    static ref THRESHOLD_SELL: Arc<Mutex<u64>> = Arc::new(Mutex::new(
        std::env::var("THRESHOLD_SELL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1_000_000_000)
    ));
    
    static ref MAX_WAIT_TIME: Arc<Mutex<u64>> = Arc::new(Mutex::new(
        std::env::var("MAX_WAIT_TIME")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60000)
    ));

    static ref TAKE_PROFIT_LEVELS: Vec<(u64, u64)> = vec![
        (2000, 100),
        (1500, 40),
        (1000, 40),
        (800, 20),
        (600, 20),
        (400, 20),
        (300, 20),
        (250, 20),
        (200, 20),
        (120, 20),
        (80, 20),
        (50, 10),
        (20, 10),
    ];
    
    static ref RETRACEMENT_LEVELS: Vec<RetracementLevel> = vec![
        RetracementLevel { percentage: 3, threshold: 2000, sell_amount: 100 },
        RetracementLevel { percentage: 4, threshold: 1500, sell_amount: 50 },
        RetracementLevel { percentage: 5, threshold: 1000, sell_amount: 40 },
        RetracementLevel { percentage: 6, threshold: 800, sell_amount: 35 },
        RetracementLevel { percentage: 6, threshold: 700, sell_amount: 35 },
        RetracementLevel { percentage: 6, threshold: 600, sell_amount: 30 },
        RetracementLevel { percentage: 7, threshold: 500, sell_amount: 30 },
        RetracementLevel { percentage: 7, threshold: 400, sell_amount: 30 },
        RetracementLevel { percentage: 8, threshold: 300, sell_amount: 20 },
        RetracementLevel { percentage: 10, threshold: 200, sell_amount: 15 },
        RetracementLevel { percentage: 12, threshold: 100, sell_amount: 15 },
        RetracementLevel { percentage: 20, threshold: 50, sell_amount: 10 },
        RetracementLevel { percentage: 30, threshold: 30, sell_amount: 10 },
        RetracementLevel { percentage: 42, threshold: 20, sell_amount: 100 },
    ];
    
    static ref DOWNING_PERCENT: Arc<Mutex<u64>> = Arc::new(Mutex::new(
        std::env::var("DOWNING_PERCENT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(42)
    ));
    
    static ref LAST_MESSAGE_TIME: Arc<Mutex<Instant>> = Arc::new(Mutex::new(Instant::now()));
}

fn update_last_message_time() {
    let mut last_time = LAST_MESSAGE_TIME.lock().unwrap();
    *last_time = Instant::now();
}

async fn check_connection_health(logger: &Logger) {
    let last_time = {
        let time = LAST_MESSAGE_TIME.lock().unwrap();
        *time
    };
    
    let now = Instant::now();
    let elapsed = now.duration_since(last_time);
    
    if elapsed > Duration::from_secs(300) {
        logger.log(format!(
            "[CONNECTION WARNING] => No messages received in {:?}. Connection may be stale.",
            elapsed
        ).yellow().to_string());
    }
}

impl TradeInfoFromToken {
    pub fn from_json(txn: SubscribeUpdateTransaction, log_messages: Vec<String>) -> Result<Self> {
        let slot = txn.slot;
            let mut instruction_type = InstructionType::PumpMint;
            let mut encoded_data = String::new();
            let mut amount: Option<u64> = None;
            let mut max_sol_cost: Option<u64> = None;
            let mut min_sol_output: Option<u64> = None;
        let mut base_amount_in: Option<u64> = None;
        let mut min_quote_amount_out: Option<u64> = None;
        let mut base_amount_out: Option<u64> = None;
        let mut max_quote_amount_in: Option<u64> = None;
            
            println!("Searching for instruction type in logs...");
            
            for log in log_messages.iter() {
                println!("Checking log: {}", log);
                
                if log.starts_with(PUMP_FUN_PROGRAM_DATA_PREFIX) {
                    encoded_data = log.split_whitespace().nth(2).unwrap_or("").to_string();
                    instruction_type = InstructionType::PumpMint;
                    println!("DETECTED PumpMint instruction: {}", log);
                    break;
                } else if log.contains(PUMP_FUN_BUY_LOG_INSTRUCTION) && log_messages.iter().any(|l| l.contains(PUMP_FUN_BUY_OR_SELL_PROGRAM_DATA_PREFIX)) {
                    encoded_data = log.split_whitespace().nth(2).unwrap_or("").to_string();
                    instruction_type = InstructionType::PumpBuy;
                    println!("DETECTED PumpBuy instruction: {}", log);
                    break;
                } else if log.contains(PUMP_FUN_SELL_LOG_INSTRUCTION) && log_messages.iter().any(|l| l.contains(PUMP_FUN_BUY_OR_SELL_PROGRAM_DATA_PREFIX)) {
                    encoded_data = log.split_whitespace().nth(2).unwrap_or("").to_string();
                    instruction_type = InstructionType::PumpSell;
                    println!("DETECTED PumpSell instruction: {}", log);
                    break;
                } else if log.contains(PUMP_SWAP_BUY_LOG_INSTRUCTION) && log_messages.iter().any(|l| l.contains(PUMP_SWAP_BUY_PROGRAM_DATA_PREFIX)) {
                    instruction_type = InstructionType::PumpSwapBuy;
                    println!("DETECTED PumpSwapBuy instruction: {}", log);
                    break;
                } else if log.contains(PUMP_SWAP_SELL_LOG_INSTRUCTION) && log_messages.iter().any(|l| l.contains(PUMP_SWAP_SELL_PROGRAM_DATA_PREFIX)) {
                    instruction_type = InstructionType::PumpSwapSell;
                    println!("DETECTED PumpSwapSell instruction: {}", log);
                    break;
                } else if log.contains("Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA") {
                    for other_log in log_messages.iter() {
                        if other_log.contains("BuyEvent") {
                            instruction_type = InstructionType::PumpSwapBuy;
                            println!("DETECTED PumpSwapBuy instruction via fallback: {}", other_log);
                            break;
                        } else if other_log.contains("SellEvent") {
                            instruction_type = InstructionType::PumpSwapSell;
                            println!("DETECTED PumpSwapSell instruction via fallback: {}", other_log);
                            break;
                        }
                    }
                    
                    if matches!(instruction_type, InstructionType::PumpSwapBuy | InstructionType::PumpSwapSell) {
                        break;
                    }
                }
            }
            
            println!("Instruction type detected: {:?}", instruction_type);

            match instruction_type {
                InstructionType::PumpMint => {
                    println!("Processing PumpMint instruction");
                    if !encoded_data.is_empty() {
                        let decoded_data = match base64::decode(&encoded_data) {
                            Ok(data) => {
                                println!("Base64 decoded data successfully, length: {}", data.len());
                                data
                            },
                            Err(e) => {
                                println!("Error decoding base64 data: {}", e);
                                Vec::new()
                            }
                        };
                        
                        if decoded_data.len() > 8 {
                            let mut offset = 8;
                            
                            let mut name: String = String::new();
                            let mut symbol = String::new();
                            let mut uri = String::new();
                            let mut mint = String::new();
                            let mut bonding_curve = String::new();
                            let mut target = String::new();

                            let mut fields = vec![
                                (&mut name, "string"),
                                (&mut symbol, "string"),
                                (&mut uri, "string"),
                                (&mut mint, "publicKey"),
                                (&mut bonding_curve, "publicKey"),
                                (&mut target, "publicKey"),
                            ];

                            for (idx, (value, field_type)) in fields.iter_mut().enumerate() {
                                if offset >= decoded_data.len() {
                                    println!("Data too short at field {}, breaking", idx);
                                    break;
                                }
                                
                                if field_type == &"string" {
                                    if offset + 4 <= decoded_data.len() {
                                        let length: u32 = u32::from_le_bytes(decoded_data[offset..offset + 4].try_into().unwrap());
                                        offset += 4;
                                        
                                        if offset + length as usize <= decoded_data.len() {
                                            let new_value = String::from_utf8(decoded_data[offset..offset + length as usize].to_vec()).ok().unwrap_or_default();
                                        // Update the string directly
                                        **value = new_value;
                                            offset += length as usize;
                                        println!("Parsed string: {}", **value);
                                        } else {
                                            println!("String data out of bounds");
                                        }
                                    }
                                } else if field_type == &"publicKey" {
                                    if offset + 32 <= decoded_data.len() {
                                        let key_bytes: [u8; 32] = decoded_data[offset..offset + 32].try_into().expect("Slice with incorrect length");
                                        let new_value = Pubkey::new_from_array(key_bytes).to_string(); // Convert bytes to Base58
                                    // Update the string directly
                                    **value = new_value;
                                        offset += 32;
                                    println!("Parsed publicKey: {}", **value);
                                    }
                                }
                            }

                            if let Some(transaction) = txn.transaction.clone() {
                                let signature = match Signature::try_from(transaction.signature.clone()) {
                                    Ok(signature) => {
                                        let sig_str = format!("{:?}", signature);
                                        println!("Parsed signature: {}", sig_str);
                                        sig_str
                                    },
                                    Err(_) => "".to_string(),
                                };
                                
                                let recent_blockhash_slice = match transaction.transaction.as_ref()
                                    .and_then(|t| t.message.as_ref())
                                    .map(|m| &m.recent_blockhash) {
                                    Some(hash) => {
                                        println!("Found blockhash");
                                        hash
                                    },
                                    None => {
                                        println!("Failed to get blockhash");
                                        return Err(anyhow::anyhow!("Failed to get recent blockhash"));
                                    }
                                };
                                
                                let recent_blockhash = Hash::new(recent_blockhash_slice);
                
                                let account_keys = match transaction.transaction.as_ref()
                                    .and_then(|t| t.message.as_ref())
                                    .map(|m| &m.account_keys) {
                                    Some(keys) => {
                                        println!("Found account keys, count: {}", keys.len());
                                        keys
                                    },
                                    None => {
                                        println!("Failed to get account keys");
                                        return Err(anyhow::anyhow!("Failed to get account keys"));
                                    }
                                };
                                
                                let mut sol_post_amount = 0_u64;
                                let mut volume_change = 0_i64;    
                
                                if let Some(ref meta) = transaction.meta {
                                    println!("Processing transaction metadata");
                                    
                                    let bonding_curve_index = account_keys
                                        .iter()
                                        .position(|account_key| {
                                            Pubkey::try_from(account_key.clone()).map(|k| k.to_string() == bonding_curve).unwrap_or(false)
                                        })
                                        .unwrap_or(0);
                
                                    println!("Bonding curve index: {}", bonding_curve_index);
                                    
                                    sol_post_amount = *meta
                                        .post_balances
                                        .get(bonding_curve_index)
                                        .unwrap_or(&0_u64);
                
                                    let sol_pre_amount = *meta
                                        .pre_balances
                                        .get(bonding_curve_index)
                                        .unwrap_or(&0_u64);
                
                                    volume_change = (sol_post_amount as i64) - (sol_pre_amount as i64);
                                    
                                    println!("Volume change: {} lamports ({} SOL)", 
                                        volume_change, 
                                        lamports_to_sol(volume_change.abs() as u64));
                                }
                
                                let bonding_curve_pubkey = match Pubkey::from_str(&bonding_curve) {
                                    Ok(pk) => pk,
                                    Err(e) => {
                                        println!("Failed to parse bonding curve pubkey: {}", e);
                                        return Err(anyhow::anyhow!("Invalid bonding curve public key"));
                                    }
                                };
                
                                let new_virtual_sol_reserve = INITIAL_VIRTUAL_SOL_RESERVES + sol_post_amount;
                                let new_virtual_token_reserve = ((INITIAL_VIRTUAL_SOL_RESERVES as u128 * INITIAL_VIRTUAL_TOKEN_RESERVES as u128) as f64 / new_virtual_sol_reserve as f64) as u64;
                                
                                println!("New virtual SOL reserve: {}", new_virtual_sol_reserve);
                                println!("New virtual token reserve: {}", new_virtual_token_reserve);
                                
                                let bonding_curve_info = BondingCurveInfo {
                                    bonding_curve: bonding_curve_pubkey,
                                    new_virtual_sol_reserve,
                                    new_virtual_token_reserve,
                                };
                
                                let mut token_post_amount = 0_f64;
                                
                                if let Some(meta) = &transaction.meta {
                                    token_post_amount = meta
                                        .post_token_balances
                                        .iter()
                                        .filter_map(|token_balance| {
                                            if token_balance.owner == target {
                                                token_balance
                                                    .ui_token_amount
                                                    .as_ref()
                                                    .map(|ui| ui.ui_amount)
                                            } else {
                                                None
                                            }
                                        })
                                        .next()
                                        .unwrap_or(0_f64);
                                    
                                    println!("Token post amount: {}", token_post_amount);
                                }
                                
                                return Ok(Self {
                                    instruction_type,
                                    slot,
                                    recent_blockhash,
                                    signature,
                                    target,
                                    mint,
                                    bonding_curve,
                                    volume_change,
                                bonding_curve_info: Some(bonding_curve_info),
                                pool_info: None,
                                    token_amount: token_post_amount,
                                    amount: None,
                                    max_sol_cost: None,
                                    min_sol_output: None,
                                base_amount_in: None,
                                min_quote_amount_out: None,
                                base_amount_out: None,
                                max_quote_amount_in: None,
                                });
                            } else {
                                println!("Transaction is None, cannot proceed");
                                return Err(anyhow::anyhow!("Transaction is None"));
                            }
                        } else {
                            println!("Decoded data too short to process");
                        }
                    } else {
                        println!("No encoded data found in log");
                    }
                },
                
                InstructionType::PumpBuy => {
                    println!("Processing PumpBuy instruction");
                    let mut buy_amount: u64 = 0;
                    let mut buy_max_sol_cost: u64 = 0;
                    let mut token_mint = String::new();
                    
                    // Find the encoded data in log messages
                    let encoded_data = log_messages.iter()
                        .find(|msg| msg.contains(PUMP_FUN_BUY_OR_SELL_PROGRAM_DATA_PREFIX))
                        .map_or("", |v| v)
                        .split_whitespace()
                        .nth(2)
                        .unwrap_or("")
                        .to_string();
                    
                    if !encoded_data.is_empty() {
                        let decoded_data = match base64::decode(&encoded_data) {
                            Ok(data) => {
                                println!("Base64 decoded buy data successfully, length: {}", data.len());
                                println!("encoded_data: {}", encoded_data);
                                data
                            },
                            Err(e) => {
                                println!("Error decoding buy base64 data: {}", e);
                                Vec::new()
                            }
                        };
                        
                        // Extract mint address if available (at position 8..40)
                        if decoded_data.len() >= 40 {  // 8 (discriminator) + 32 (mint)
                            let mint_bytes = &decoded_data[8..40];
                            token_mint = bs58::encode(mint_bytes).into_string();
                            println!("Extracted mint address: {}", token_mint);
                        }
                        
                        // Extract amount if available
                        if decoded_data.len() >= 48 {  // 8 + 32 + 8 (amount)
                            let amount_bytes = &decoded_data[40..48];
                            buy_amount = u64::from_le_bytes(amount_bytes.try_into().unwrap());
                            println!("Detected transaction amount: {} tokens", buy_amount);
                        }
                        
                        // Extract max_sol_cost if available
                        if decoded_data.len() >= 56 {  // 8 + 32 + 8 + 8 (max_sol_cost)
                            let max_sol_cost_bytes = &decoded_data[48..56];
                            buy_max_sol_cost = u64::from_le_bytes(max_sol_cost_bytes.try_into().unwrap());
                            println!("Max SOL cost: {}", buy_max_sol_cost);
                            println!("Max SOL cost in SOL: {} SOL", lamports_to_sol(buy_max_sol_cost));
                        }
                        
                        amount = Some(buy_amount);
                        max_sol_cost = Some(buy_max_sol_cost);
                    }
                
                // Here we need to use the variables from the PumpMint scope
                if let Some(transaction) = txn.transaction.clone() {
                    let signature = match Signature::try_from(transaction.signature.clone()) {
                        Ok(signature) => {
                            let sig_str = format!("{:?}", signature);
                            println!("Parsed signature: {}", sig_str);
                            sig_str
                        },
                        Err(_) => "".to_string(),
                    };
                    
                    let recent_blockhash_slice = match transaction.transaction.as_ref()
                        .and_then(|t| t.message.as_ref())
                        .map(|m| &m.recent_blockhash) {
                        Some(hash) => {
                            println!("Found blockhash");
                            hash
                        },
                        None => {
                            println!("Failed to get blockhash");
                            return Err(anyhow::anyhow!("Failed to get recent blockhash"));
                        }
                    };
                    
                    let recent_blockhash = Hash::new(recent_blockhash_slice);
                    
                    let account_keys = match transaction.transaction.as_ref()
                        .and_then(|t| t.message.as_ref())
                        .map(|m| &m.account_keys) {
                        Some(keys) => {
                            println!("Found account keys, count: {}", keys.len());
                            keys
                        },
                        None => {
                            println!("Failed to get account keys");
                            return Err(anyhow::anyhow!("Failed to get account keys"));
                        }
                    };
                    
                    // Extract target, mint, bonding_curve, etc. from transaction information
                    let mut target = String::new();
                    let mut mint = token_mint; // Use the extracted mint if available
                    let mut bonding_curve = String::new();
                    let mut sol_post_amount = 0_u64;
                    let mut volume_change = 0_i64;
                    let mut token_post_amount = 0_f64;
                    
                    // Process transaction metadata to extract information
                    if let Some(meta) = &transaction.meta {
                        println!("Processing transaction metadata");
                        
                        // Extract necessary information from metadata and logs
                        for token_balance in &meta.post_token_balances {
                            if mint.is_empty() {
                                mint = token_balance.mint.clone();
                                println!("Found mint: {}", mint);
                            }
                            target = token_balance.owner.clone();
                            println!("Found target: {}", target);
                            
                            // Extract token amount
                            token_post_amount = token_balance
                                .ui_token_amount
                                .as_ref()
                                .map(|ui| ui.ui_amount)
                                .unwrap_or(0_f64);
                            
                            println!("Token post amount: {}", token_post_amount);
                        }
                        
                        // Find bonding curve from logs
                        for log in log_messages.iter() {
                            if log.contains("bonding curve:") {
                                if let Some(bc) = log.split("bonding curve:").nth(1).map(|s| s.trim()) {
                                    bonding_curve = bc.to_string();
                                    println!("Found bonding curve: {}", bonding_curve);
                                }
                            }
                        }
                        
                        // Calculate volume change
                        if !bonding_curve.is_empty() {
                            let bonding_curve_index = account_keys
                                .iter()
                                .position(|account_key| {
                                    Pubkey::try_from(account_key.clone()).map(|k| k.to_string() == bonding_curve).unwrap_or(false)
                                })
                                .unwrap_or(0);
                            
                            sol_post_amount = *meta
                                .post_balances
                                .get(bonding_curve_index)
                                .unwrap_or(&0_u64);
                            
                            let sol_pre_amount = *meta
                                .pre_balances
                                .get(bonding_curve_index)
                                .unwrap_or(&0_u64);
                            
                            volume_change = (sol_post_amount as i64) - (sol_pre_amount as i64);
                            println!("Volume change: {} lamports", volume_change);
                        }
                    }
                    
                    // Create BondingCurveInfo if we have bonding curve information
                    let bonding_curve_info_option = if !bonding_curve.is_empty() {
                        let bonding_curve_pubkey = match Pubkey::from_str(&bonding_curve) {
                            Ok(pk) => pk,
                            Err(e) => {
                                println!("Failed to parse bonding curve pubkey: {}", e);
                                return Err(anyhow::anyhow!("Invalid bonding curve public key"));
                            }
                        };
                        
                        let new_virtual_sol_reserve = INITIAL_VIRTUAL_SOL_RESERVES + sol_post_amount;
                        let new_virtual_token_reserve = ((INITIAL_VIRTUAL_SOL_RESERVES as u128 * INITIAL_VIRTUAL_TOKEN_RESERVES as u128) as f64 / new_virtual_sol_reserve as f64) as u64;
                        
                        println!("New virtual SOL reserve: {}", new_virtual_sol_reserve);
                        println!("New virtual token reserve: {}", new_virtual_token_reserve);
                        
                        Some(BondingCurveInfo {
                            bonding_curve: bonding_curve_pubkey,
                            new_virtual_sol_reserve,
                            new_virtual_token_reserve,
                        })
                    } else {
                        None // We'll let build_swap_ixn_by_mint fetch the bonding curve
                    };
                    
                    return Ok(Self {
                        instruction_type,
                        slot,
                        recent_blockhash,
                        signature,
                        target,
                        mint,
                        bonding_curve,
                        volume_change,
                        bonding_curve_info: bonding_curve_info_option,
                        pool_info: None,
                        token_amount: token_post_amount,
                        amount,
                        max_sol_cost,
                        min_sol_output: None,
                        base_amount_in: None,
                        min_quote_amount_out: None,
                        base_amount_out: None,
                        max_quote_amount_in: None,
                    });
                } else {
                    println!("Transaction is None, cannot proceed");
                    return Err(anyhow::anyhow!("Transaction is None"));
                }
                },
                
                InstructionType::PumpSell => {
                    println!("Processing PumpSell instruction");
                    let mut sell_amount: u64 = 0;
                    let mut sell_min_sol_output: u64 = 0;
                    let mut token_mint = String::new();
                    
                    // Find the encoded data in log messages
                    let encoded_data = log_messages.iter()
                        .find(|msg| msg.contains(PUMP_FUN_BUY_OR_SELL_PROGRAM_DATA_PREFIX))
                        .map_or("", |v| v)
                        .split_whitespace()
                        .nth(2)
                        .unwrap_or("")
                        .to_string();
                    
                    if !encoded_data.is_empty() {
                        let decoded_data = match base64::decode(&encoded_data) {
                            Ok(data) => {
                                println!("Base64 decoded sell data successfully, length: {}", data.len());
                                println!("encoded_data: {}", encoded_data);
                                data
                            },
                            Err(e) => {
                                println!("Error decoding sell base64 data: {}", e);
                                Vec::new()
                            }
                        };
                        
                        // Extract mint address if available (at position 8..40)
                        if decoded_data.len() >= 40 {  // 8 (discriminator) + 32 (mint)
                            let mint_bytes = &decoded_data[8..40];
                            token_mint = bs58::encode(mint_bytes).into_string();
                            println!("Extracted mint address: {}", token_mint);
                        }
                        
                        // Extract amount if available
                        if decoded_data.len() >= 48 {  // 8 + 32 + 8 (amount)
                            let amount_bytes = &decoded_data[40..48];
                            sell_amount = u64::from_le_bytes(amount_bytes.try_into().unwrap());
                            println!("Detected sell amount: {} tokens", sell_amount);
                        }
                        
                        // Extract min_sol_output if available
                        if decoded_data.len() >= 56 {  // 8 + 32 + 8 + 8 (min_sol_output)
                            let min_sol_output_bytes = &decoded_data[48..56];
                            sell_min_sol_output = u64::from_le_bytes(min_sol_output_bytes.try_into().unwrap());
                            println!("Min SOL output: {}", sell_min_sol_output);
                            println!("Min SOL output in SOL: {} SOL", lamports_to_sol(sell_min_sol_output));
                        }
                        
                        amount = Some(sell_amount);
                        min_sol_output = Some(sell_min_sol_output);
                    }
                
                // Here we need to use the variables from the PumpMint scope
                if let Some(transaction) = txn.transaction.clone() {
                    let signature = match Signature::try_from(transaction.signature.clone()) {
                        Ok(signature) => {
                            let sig_str = format!("{:?}", signature);
                            println!("Parsed signature: {}", sig_str);
                            sig_str
                        },
                        Err(_) => "".to_string(),
                    };
                    
                    let recent_blockhash_slice = match transaction.transaction.as_ref()
                        .and_then(|t| t.message.as_ref())
                        .map(|m| &m.recent_blockhash) {
                        Some(hash) => {
                            println!("Found blockhash");
                            hash
                        },
                        None => {
                            println!("Failed to get blockhash");
                            return Err(anyhow::anyhow!("Failed to get recent blockhash"));
                        }
                    };
                    
                    let recent_blockhash = Hash::new(recent_blockhash_slice);
                    
                    let account_keys = match transaction.transaction.as_ref()
                        .and_then(|t| t.message.as_ref())
                        .map(|m| &m.account_keys) {
                        Some(keys) => {
                            println!("Found account keys, count: {}", keys.len());
                            keys
                        },
                        None => {
                            println!("Failed to get account keys");
                            return Err(anyhow::anyhow!("Failed to get account keys"));
                        }
                    };
                    
                    // Extract target, mint, bonding_curve, etc. from transaction information
                    let mut target = String::new();
                    let mut mint = token_mint; // Use the extracted mint if available
                    let mut bonding_curve = String::new();
                    let mut sol_post_amount = 0_u64;
                    let mut volume_change = 0_i64;
                    let mut token_post_amount = 0_f64;
                    
                    // Process transaction metadata to extract information
                    if let Some(meta) = &transaction.meta {
                        println!("Processing transaction metadata");
                        
                        // Extract necessary information from metadata and logs
                        for token_balance in &meta.post_token_balances {
                            if mint.is_empty() {
                                mint = token_balance.mint.clone();
                                println!("Found mint: {}", mint);
                            }
                            target = token_balance.owner.clone();
                            println!("Found target: {}", target);
                            
                            // Extract token amount
                            token_post_amount = token_balance
                                .ui_token_amount
                                .as_ref()
                                .map(|ui| ui.ui_amount)
                                .unwrap_or(0_f64);
                            
                            println!("Token post amount: {}", token_post_amount);
                        }
                        
                        // Find bonding curve from logs
                        for log in log_messages.iter() {
                            if log.contains("bonding curve:") {
                                if let Some(bc) = log.split("bonding curve:").nth(1).map(|s| s.trim()) {
                                    bonding_curve = bc.to_string();
                                    println!("Found bonding curve: {}", bonding_curve);
                                }
                            }
                        }
                        
                        // Calculate volume change
                        if !bonding_curve.is_empty() {
                            let bonding_curve_index = account_keys
                                .iter()
                                .position(|account_key| {
                                    Pubkey::try_from(account_key.clone()).map(|k| k.to_string() == bonding_curve).unwrap_or(false)
                                })
                                .unwrap_or(0);
                            
                            sol_post_amount = *meta
                                .post_balances
                                .get(bonding_curve_index)
                                .unwrap_or(&0_u64);
                            
                            let sol_pre_amount = *meta
                                .pre_balances
                                .get(bonding_curve_index)
                                .unwrap_or(&0_u64);
                            
                            volume_change = (sol_post_amount as i64) - (sol_pre_amount as i64);
                            println!("Volume change: {} lamports", volume_change);
                        }
                    }
                    
                    // Create BondingCurveInfo if we have bonding curve information
                    let bonding_curve_info_option = if !bonding_curve.is_empty() {
                        let bonding_curve_pubkey = match Pubkey::from_str(&bonding_curve) {
                            Ok(pk) => pk,
                            Err(e) => {
                                println!("Failed to parse bonding curve pubkey: {}", e);
                                return Err(anyhow::anyhow!("Invalid bonding curve public key"));
                            }
                        };
                        
                        let new_virtual_sol_reserve = INITIAL_VIRTUAL_SOL_RESERVES + sol_post_amount;
                        let new_virtual_token_reserve = ((INITIAL_VIRTUAL_SOL_RESERVES as u128 * INITIAL_VIRTUAL_TOKEN_RESERVES as u128) as f64 / new_virtual_sol_reserve as f64) as u64;
                        
                        println!("New virtual SOL reserve: {}", new_virtual_sol_reserve);
                        println!("New virtual token reserve: {}", new_virtual_token_reserve);
                        
                        Some(BondingCurveInfo {
                            bonding_curve: bonding_curve_pubkey,
                            new_virtual_sol_reserve,
                            new_virtual_token_reserve,
                        })
                    } else {
                        None // We'll let build_swap_ixn_by_mint fetch the bonding curve
                    };
                
                    return Ok(Self {
                        instruction_type,
                        slot,
                        recent_blockhash,
                        signature,
                        target,
                        mint,
                        bonding_curve,
                        volume_change,
                        bonding_curve_info: bonding_curve_info_option,
                        pool_info: None,
                        token_amount: token_post_amount,
                        amount,
                        max_sol_cost: None,
                        min_sol_output,
                        base_amount_in: None,
                        min_quote_amount_out: None,
                        base_amount_out: None,
                        max_quote_amount_in: None,
                    });
                } else {
                    println!("Transaction is None, cannot proceed");
                    return Err(anyhow::anyhow!("Transaction is None"));
                }
                },
            
            InstructionType::PumpSwapBuy | InstructionType::PumpSwapSell => {
                println!("Processing PumpSwap {:?} instruction", instruction_type);
                
                // Parse PumpSwap transaction information
            if let Some(transaction) = txn.transaction.clone() {
                let signature = match Signature::try_from(transaction.signature.clone()) {
                        Ok(signature) => {
                            let sig_str = format!("{:?}", signature);
                            println!("Parsed signature: {}", sig_str);
                            sig_str
                        },
                    Err(_) => "".to_string(),
                };
                
                let recent_blockhash_slice = match transaction.transaction.as_ref()
                    .and_then(|t| t.message.as_ref())
                    .map(|m| &m.recent_blockhash) {
                        Some(hash) => {
                            println!("Found blockhash");
                            hash
                        },
                        None => {
                            println!("Failed to get blockhash");
                            return Err(anyhow::anyhow!("Failed to get recent blockhash"));
                        }
                };
                
                let recent_blockhash = Hash::new(recent_blockhash_slice);

                let account_keys = match transaction.transaction.as_ref()
                    .and_then(|t| t.message.as_ref())
                    .map(|m| &m.account_keys) {
                        Some(keys) => {
                            println!("Found account keys, count: {}", keys.len());
                            keys
                        },
                        None => {
                            println!("Failed to get account keys");
                            return Err(anyhow::anyhow!("Failed to get account keys"));
                        }
                    };
                    
                    // Get transaction data for PumpSwap specific information
                    let pump_amm_program_id = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
                    
                    // Find the instruction indices for PumpSwap program
                    let mut pump_swap_instruction_indices = Vec::new();
                    if let Some(transaction_info) = transaction.transaction.as_ref() {
                        if let Some(message) = transaction_info.message.as_ref() {
                            for (i, account_indices) in message.instructions.iter().enumerate() {
                                let program_idx = account_indices.program_id_index;
                                if let Some(program_key) = account_keys.get(program_idx as usize) {
                                    if let Ok(program_key_pubkey) = Pubkey::try_from(program_key.clone()) {
                                        if program_key_pubkey.to_string() == pump_amm_program_id {
                                            pump_swap_instruction_indices.push(i);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    println!("PumpSwap instruction indices: {:?}", pump_swap_instruction_indices);
                    
                    // Extract pool, base_mint, and quote_mint information from logs
                    let mut pool_id = Pubkey::default();
                    let mut base_mint = Pubkey::default();
                    let mut quote_mint = Pubkey::default();
                    let mut pool_base_token_account = Pubkey::default();
                    let mut pool_quote_token_account = Pubkey::default();
                    let mut base_reserve = 0u64;
                    let mut quote_reserve = 0u64;
                    
                    // Get mint addresses
                    if !pump_swap_instruction_indices.is_empty() {
                        // By examining the structure of PumpSwap instructions from the IDL
                        // Accounts in a buy/sell instruction:
                        // 0: pool ID
                        // 3: base mint (token)
                        // 4: quote mint (SOL)
                        
                        if let Some(tx_info) = transaction.transaction.as_ref() {
                            if let Some(message) = tx_info.message.as_ref() {
                                if let Some(instr_idx) = pump_swap_instruction_indices.first() {
                                    if let Some(instruction) = message.instructions.get(*instr_idx) {
                                        // Get accounts directly without using as_ref()
                                        let accounts = &instruction.accounts;
                                        // Pool ID is the first account
                                        if accounts.len() > 0 {
                                            if let Some(pool_account_key) = account_keys.get(accounts[0] as usize) {
                                                if let Ok(pubkey) = Pubkey::try_from(pool_account_key.clone()) {
                                                    pool_id = pubkey;
                                                    println!("Pool ID: {}", pool_id);
                                                }
                                            }
                                        }
                                        
                                        // Base mint is the 4th account
                                        if accounts.len() > 3 {
                                            if let Some(base_mint_key) = account_keys.get(accounts[3] as usize) {
                                                if let Ok(pubkey) = Pubkey::try_from(base_mint_key.clone()) {
                                                    base_mint = pubkey;
                                                    println!("Base mint: {}", base_mint);
                                                }
                                            }
                                        }
                                        
                                        // Quote mint is the 5th account
                                        if accounts.len() > 4 {
                                            if let Some(quote_mint_key) = account_keys.get(accounts[4] as usize) {
                                                if let Ok(pubkey) = Pubkey::try_from(quote_mint_key.clone()) {
                                                    quote_mint = pubkey;
                                                    println!("Quote mint: {}", quote_mint);
                                                }
                                            }
                                        }
                                        
                                        // Pool base token account is the 8th account
                                        if accounts.len() > 7 {
                                            if let Some(pool_base_key) = account_keys.get(accounts[7] as usize) {
                                                if let Ok(pubkey) = Pubkey::try_from(pool_base_key.clone()) {
                                                    pool_base_token_account = pubkey;
                                                    println!("Pool base token account: {}", pool_base_token_account);
                                                }
                                            }
                                        }
                                        
                                        // Pool quote token account is the 9th account
                                        if accounts.len() > 8 {
                                            if let Some(pool_quote_key) = account_keys.get(accounts[8] as usize) {
                                                if let Ok(pubkey) = Pubkey::try_from(pool_quote_key.clone()) {
                                                    pool_quote_token_account = pubkey;
                                                    println!("Pool quote token account: {}", pool_quote_token_account);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Extract base_amount_in, min_quote_amount_out, base_amount_out, max_quote_amount_in from logs
                    for log in log_messages.iter() {
                        // Extract parameters from logs - example patterns based on PumpSwap events
                        if instruction_type == InstructionType::PumpSwapBuy {
                            if log.contains("base_amount_out:") {
                                // Example: Extract base_amount_out from a log like "base_amount_out: 1000000"
                                if let Some(value_str) = log.split("base_amount_out:").nth(1).map(|s| s.trim()) {
                                    if let Ok(value) = value_str.parse::<u64>() {
                                        base_amount_out = Some(value);
                                        println!("Extracted base_amount_out: {}", value);
                                    }
                                }
                            }
                            if log.contains("max_quote_amount_in:") {
                                // Example: Extract max_quote_amount_in from a log like "max_quote_amount_in: 2000000"
                                if let Some(value_str) = log.split("max_quote_amount_in:").nth(1).map(|s| s.trim()) {
                                    if let Ok(value) = value_str.parse::<u64>() {
                                        max_quote_amount_in = Some(value);
                                        println!("Extracted max_quote_amount_in: {}", value);
                                    }
                                }
                            }
                        } else if instruction_type == InstructionType::PumpSwapSell {
                            if log.contains("base_amount_in:") {
                                // Example: Extract base_amount_in from a log like "base_amount_in: 1000000"
                                if let Some(value_str) = log.split("base_amount_in:").nth(1).map(|s| s.trim()) {
                                    if let Ok(value) = value_str.parse::<u64>() {
                                        base_amount_in = Some(value);
                                        println!("Extracted base_amount_in: {}", value);
                                    }
                                }
                            }
                            if log.contains("min_quote_amount_out:") {
                                // Example: Extract min_quote_amount_out from a log like "min_quote_amount_out: 500000"
                                if let Some(value_str) = log.split("min_quote_amount_out:").nth(1).map(|s| s.trim()) {
                                    if let Ok(value) = value_str.parse::<u64>() {
                                        min_quote_amount_out = Some(value);
                                        println!("Extracted min_quote_amount_out: {}", value);
                                    }
                                }
                            }
                        }
                        
                        // Extract pool balances
                        if log.contains("pool_base_token_reserves:") {
                            if let Some(value_str) = log.split("pool_base_token_reserves:").nth(1).map(|s| s.trim()) {
                                if let Ok(value) = value_str.parse::<u64>() {
                                    base_reserve = value;
                                    println!("Extracted pool_base_token_reserves: {}", value);
                                }
                            }
                        }
                        if log.contains("pool_quote_token_reserves:") {
                            if let Some(value_str) = log.split("pool_quote_token_reserves:").nth(1).map(|s| s.trim()) {
                                if let Ok(value) = value_str.parse::<u64>() {
                                    quote_reserve = value;
                                    println!("Extracted pool_quote_token_reserves: {}", value);
                                }
                            }
                        }
                    }
                    
                    // Find target wallet and token amount
                    let mut target = String::new();
                    let mut volume_change = 0_i64;
                    let mut token_post_amount = 0_f64;
                    
                    if let Some(meta) = &transaction.meta {
                        // Find the user account from instruction accounts
                        // In PumpSwap instructions, the user is typically the 2nd account
                        if !pump_swap_instruction_indices.is_empty() {
                            if let Some(tx_info) = transaction.transaction.as_ref() {
                                if let Some(message) = tx_info.message.as_ref() {
                                    if let Some(instr_idx) = pump_swap_instruction_indices.first() {
                                        if let Some(instruction) = message.instructions.get(*instr_idx) {
                                            // Get accounts directly without using as_ref()
                                            let accounts = &instruction.accounts;
                                            // User is the 2nd account
                                            if accounts.len() > 1 {
                                                if let Some(user_account_key) = account_keys.get(accounts[1] as usize) {
                                                    if let Ok(pubkey) = Pubkey::try_from(user_account_key.clone()) {
                                                        target = pubkey.to_string();
                                                        println!("Target user: {}", target);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Calculate volume change for pool quote token account
                        let pool_quote_index = account_keys
                        .iter()
                        .position(|account_key| {
                                Pubkey::try_from(account_key.clone()).map(|k| k == pool_quote_token_account).unwrap_or(false)
                        })
                        .unwrap_or(0);

                        let quote_post_amount = *meta
                        .post_balances
                            .get(pool_quote_index)
                        .unwrap_or(&0_u64);

                        let quote_pre_amount = *meta
                        .pre_balances
                            .get(pool_quote_index)
                        .unwrap_or(&0_u64);

                        volume_change = (quote_post_amount as i64) - (quote_pre_amount as i64);
                        
                        // Get token amount from post token balances
                    token_post_amount = meta
                        .post_token_balances
                        .iter()
                        .filter_map(|token_balance| {
                            if token_balance.owner == target {
                                token_balance
                                    .ui_token_amount
                                    .as_ref()
                                    .map(|ui| ui.ui_amount)
                            } else {
                                None
                            }
                        })
                        .next()
                        .unwrap_or(0_f64);
                    
                    println!("Token post amount: {}", token_post_amount);
                    }
                    
                    // Create pool info
                    let pool_info = PoolInfo {
                        pool_id,
                        base_mint,
                        quote_mint,
                        pool_base_token_account,
                        pool_quote_token_account,
                        base_reserve,
                        quote_reserve,
                        coin_creator: Pubkey::default(),
                    };
                    
                    // Determine bonding curve (in PumpSwap case, we don't have one)
                    let bonding_curve = String::new();
                    
                    // Get mint
                    let mint = base_mint.to_string();
                    
                    return Ok(Self {
                    instruction_type,
                        slot,
                    recent_blockhash,
                    signature,
                    target,
                    mint,
                    bonding_curve,
                    volume_change,
                        bonding_curve_info: None,
                        pool_info: Some(pool_info),
                        token_amount: token_post_amount,
                        amount: None,
                        max_sol_cost: None,
                        min_sol_output: None,
                        base_amount_in,
                        min_quote_amount_out,
                        base_amount_out,
                        max_quote_amount_in,
                    });
            } else {
                    println!("Transaction is None, cannot proceed");
                return Err(anyhow::anyhow!("Transaction is None"));
            }
            }
        }
        
        // If we reach here, we failed to parse the transaction
        println!("Failed to parse transaction");
        Err(anyhow::anyhow!("Failed to parse transaction"))
    }
}

/**
 * The following functions implement a ping-pong mechanism to keep the gRPC connection alive:
 * 
 * - process_stream_message: Handles incoming messages, responding to pings and logging pongs
 * - handle_ping_message: Sends a pong response when a ping is received
 * - send_heartbeat_ping: Proactively sends pings every 30 seconds
 * 
 * This ensures the connection stays active even during periods of inactivity,
 * preventing timeouts from the server or network infrastructure.
 */

/// Send a ping response when we receive a ping
async fn handle_ping_message(
    subscribe_tx: &Arc<tokio::sync::Mutex<impl Sink<SubscribeRequest, Error = impl std::fmt::Debug> + Unpin>>,
    logger: &Logger,
) -> Result<(), String> {
    let ping_request = SubscribeRequest {
        ping: Some(SubscribeRequestPing { id: 1 }),
        ..Default::default()
    };

    // Get a lock on the mutex
    let mut locked_tx = subscribe_tx.lock().await;
    
    // Send the ping response
    match locked_tx.send(ping_request).await {
        Ok(_) => {
            Ok(())
        },
        Err(e) => {
            Err(format!("Failed to send ping response: {:?}", e))
        }
    }
}

/// Process stream messages including ping-pong for keepalive
async fn process_stream_message(
    msg: &SubscribeUpdate,
    subscribe_tx: &Arc<tokio::sync::Mutex<impl Sink<SubscribeRequest, Error = impl std::fmt::Debug> + Unpin>>,
    logger: &Logger,
) -> Result<(), String> {
    update_last_message_time();
    match &msg.update_oneof {
        Some(UpdateOneof::Ping(_)) => {
            handle_ping_message(subscribe_tx, logger).await?;
        }
        Some(UpdateOneof::Pong(_)) => {
        }
        _ => {
        }
    }
    Ok(())
}

// Heartbeat function to periodically send pings
async fn send_heartbeat_ping(
    subscribe_tx: &Arc<tokio::sync::Mutex<impl Sink<SubscribeRequest, Error = impl std::fmt::Debug> + Unpin>>,
    logger: &Logger
) -> Result<(), String> {
    let ping_request = SubscribeRequest {
        ping: Some(SubscribeRequestPing { id: 1 }),
        ..Default::default()
    };
    
    // Get a lock on the mutex
    let mut locked_tx = subscribe_tx.lock().await;
    
    // Send the ping heartbeat
    match locked_tx.send(ping_request).await {
        Ok(_) => {
            Ok(())
        },
        Err(e) => {
            Err(format!("Failed to send heartbeat ping: {:?}", e))
        }
    }
}

pub async fn new_token_trader_pumpfun(
    yellowstone_grpc_http: String,
    yellowstone_grpc_token: String,
    app_state: AppState,
    swap_config: SwapConfig,
    time_exceed: u64,
    counter_limit: u64,
    min_dev_buy: u64,
    max_dev_buy: u64,
) -> Result<(), String> {
    // Log the copy trading configuration
    let logger = Logger::new("[PUMPFUN-MONITOR] => ".blue().bold().to_string());

    // INITIAL SETTING FOR SUBSCIBE
    // -----------------------------------------------------------------------------------------------------------------------------
    let mut client = GeyserGrpcClient::build_from_shared(yellowstone_grpc_http.clone())
        .map_err(|e| format!("Failed to build client: {}", e))?
        .x_token::<String>(Some(yellowstone_grpc_token.clone()))
        .map_err(|e| format!("Failed to set x_token: {}", e))?
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| format!("Failed to set tls config: {}", e))?
        .connect()
        .await
        .map_err(|e| format!("Failed to connect: {}", e))?;

    // Create additional clones for later use in tasks
    let yellowstone_grpc_http = Arc::new(yellowstone_grpc_http);
    let yellowstone_grpc_token = Arc::new(yellowstone_grpc_token);
    let app_state = Arc::new(app_state);
    let swap_config = Arc::new(swap_config);

    // Log the copy trading configuration
    let logger = Logger::new("[PUMPFUN-MONITOR] => ".blue().bold().to_string());

    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 3;
    let (subscribe_tx, mut stream) = loop {
        match client.subscribe().await {
            Ok(pair) => break pair,
            Err(e) => {
                retry_count += 1;
                if retry_count >= MAX_RETRIES {
                    return Err(format!("Failed to subscribe after {} attempts: {}", MAX_RETRIES, e));
                }
                logger.log(format!(
                    "[CONNECTION ERROR] => Failed to subscribe (attempt {}/{}): {}. Retrying in 5 seconds...",
                    retry_count, MAX_RETRIES, e
                ).red().to_string());
                time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    // Convert to Arc to allow cloning across tasks
    let subscribe_tx = Arc::new(tokio::sync::Mutex::new(subscribe_tx));

    // Get copy trading configuration from environment
    let copy_trading_target_address = std::env::var("COPY_TRADING_TARGET_ADDRESS").ok();
    let is_multi_copy_trading = std::env::var("IS_MULTI_COPY_TRADING")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false);
    
    // Prepare program IDs for monitoring
    let mut program_ids = vec![PUMP_PROGRAM.to_string()];
    let mut copy_trading_target_addresses = Vec::new();
    
    // Handle multiple copy trading targets if enabled
    if is_multi_copy_trading {
        if let Some(address_str) = copy_trading_target_address {
            // Parse comma-separated addresses
            for addr in address_str.split(',') {
                let trimmed_addr = addr.trim();
                if !trimmed_addr.is_empty() {
                    program_ids.push(trimmed_addr.to_string());
                    copy_trading_target_addresses.push(trimmed_addr.to_string());
                }
            }
        }
    } else if let Some(address) = copy_trading_target_address {
        // Single address mode
        if !address.is_empty() {
            program_ids.push(address.clone());
            copy_trading_target_addresses.push(address);
        }
    }

    let filter_config = FilterConfig {
        program_ids: program_ids.clone(),
        _instruction_discriminators: &[PUMP_FUN_CREATE_IX_DISCRIMINATOR],
        copy_trading_target_addresses,
        is_multi_copy_trading,
    };

    // Log the copy trading configuration
    if !filter_config.copy_trading_target_addresses.is_empty() {
        logger.log(format!(
            "[COPY TRADING] => Monitoring {} address(es)",
            filter_config.copy_trading_target_addresses.len(),
        ).green().to_string());
        
        for (i, addr) in filter_config.copy_trading_target_addresses.iter().enumerate() {
            logger.log(format!(
                "\t * [TARGET {}] => {}",
                i + 1, addr
            ).green().to_string());
        }
    }

    subscribe_tx
        .lock()
        .await
        .send(SubscribeRequest {
            slots: HashMap::new(),
            accounts: HashMap::new(),
            transactions: hashmap! {
                "All".to_owned() => SubscribeRequestFilterTransactions {
                    vote: None,
                    failed: Some(false),
                    signature: None,
                    account_include: program_ids,
                    account_exclude: vec![JUPITER_PROGRAM.to_string(), OKX_DEX_PROGRAM.to_string()],
                    account_required: Vec::<String>::new()
                }
            },
            transactions_status: HashMap::new(),
            entry: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: Some(CommitmentLevel::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        })
        .await
        .map_err(|e| format!("Failed to send subscribe request: {}", e))?;

    let existing_liquidity_pools = Arc::new(Mutex::new(HashSet::<LiquidityPool>::new()));

    let rpc_nonblocking_client = app_state.clone().rpc_nonblocking_client.clone();
    let rpc_client = app_state.clone().rpc_client.clone();
    let wallet = app_state.clone().wallet.clone();
    let swapx = Pump::new(
        rpc_nonblocking_client.clone(),
        rpc_client.clone(),
        wallet.clone(),
    );

    logger.log("[STARTED. MONITORING]...".blue().bold().to_string());
    
    // Set buying enabled to true at start
    {
        let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
        *buying_enabled = true;
    }

    // After all setup and before the main loop, add a heartbeat ping task
    let subscribe_tx_clone = subscribe_tx.clone();
    let logger_clone = logger.clone();
    
    tokio::spawn(async move {
        let ping_logger = logger_clone.clone();
        let mut interval = time::interval(Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = send_heartbeat_ping(&subscribe_tx_clone, &ping_logger).await {
                ping_logger.log(format!("[CONNECTION ERROR] => {}", e).red().to_string());
                break;
            }
        }
    });

    // Start a background task to check the status of tokens periodically
    let existing_liquidity_pools_clone = Arc::clone(&existing_liquidity_pools);
    let logger_clone = logger.clone();
    let app_state_for_background = Arc::clone(&app_state);
    let swap_config_for_background = Arc::clone(&swap_config);
    
    tokio::spawn(async move {
        let pools_clone = Arc::clone(&existing_liquidity_pools_clone);
        let check_logger = logger_clone.clone();
        let app_state_clone = Arc::clone(&app_state_for_background);
        let swap_config_clone = Arc::clone(&swap_config_for_background);
        
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            // Check if there are any bought tokens and if any have exceeded MAX_WAIT_TIME
            let now = Instant::now();
            let max_wait_time_millis = *MAX_WAIT_TIME.lock().unwrap();
            let max_wait_duration = Duration::from_millis(max_wait_time_millis);
            
            let (has_bought_tokens, tokens_to_sell) = {
                let pools = pools_clone.lock().unwrap();
                let bought_tokens: Vec<String> = pools.iter()
                    .filter(|pool| pool.status == Status::Bought)
                    .map(|pool| pool.mint.clone())
                    .collect();
                
                let timed_out_tokens: Vec<(String, Instant)> = pools.iter()
                    .filter(|pool| pool.status == Status::Bought && 
                           pool.timestamp.map_or(false, |ts| now.duration_since(ts) > max_wait_duration))
                    .map(|pool| (pool.mint.clone(), pool.timestamp.unwrap()))
                    .collect();
                
                // Log bought tokens that are waiting to be sold
                if !bought_tokens.is_empty() {
                    check_logger.log(format!(
                        "\n\t * [BUYING PAUSED] => Waiting for tokens to be sold: {:?}",
                        bought_tokens
                    ).yellow().to_string());
                }
                
                // Log tokens that have timed out and will be force-sold
                if !timed_out_tokens.is_empty() {
                    check_logger.log(format!(
                        "\n\t * [TIMEOUT DETECTED] => Will force-sell tokens that exceeded {} ms wait time: {:?}",
                        max_wait_time_millis,
                        timed_out_tokens.iter().map(|(mint, _)| mint).collect::<Vec<_>>()
                    ).red().bold().to_string());
                }
                
                (bought_tokens.len() > 0, timed_out_tokens)
            };
            
            // Update buying status
            {
                let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                *buying_enabled = !has_bought_tokens;
            }
            
            // Force-sell tokens that have exceeded MAX_WAIT_TIME
            for (mint, timestamp) in tokens_to_sell {
                // Clone the necessary state for this token
                let logger_for_selling = check_logger.clone();
                let pools_clone_for_selling = Arc::clone(&pools_clone);
                let app_state_for_selling = app_state_clone.clone();
                let swap_config_for_selling = swap_config_clone.clone();
                
                check_logger.log(format!(
                    "\n\t * [FORCE SELLING] => Token {} exceeded wait time (elapsed: {:?})",
                    mint, now.duration_since(timestamp)
                ).red().to_string());
                
                tokio::spawn(async move {
                    // Get the existing pool for this mint
                    let existing_pool = {
                        let pools = pools_clone_for_selling.lock().unwrap();
                        pools.iter()
                            .find(|pool| pool.mint == mint)
                            .cloned()
                            .unwrap_or(LiquidityPool {
                                mint: mint.clone(),
                                buy_price: 0_f64,
                                sell_price: 0_f64,
                                status: Status::Bought,
                                timestamp: Some(timestamp),
                            })
                    };
                    
                    // Set up sell config
                    let sell_config = SwapConfig {
                        swap_direction: SwapDirection::Sell,
                        in_type: SwapInType::Pct,
                        amount_in: 1_f64,  // Sell 100%
                        slippage: 100_u64, // Use full slippage
                        use_jito: swap_config_for_selling.clone().use_jito,
                    };
                    
                    // Create Pump instance for selling
                    let app_state_for_task = app_state_for_selling.clone();
                    let rpc_nonblocking_client = app_state_for_task.rpc_nonblocking_client.clone();
                    let rpc_client = app_state_for_task.rpc_client.clone();
                    let wallet = app_state_for_task.wallet.clone();
                    let swapx = Pump::new(rpc_nonblocking_client.clone(), rpc_client.clone(), wallet.clone());
                    
                    // Execute the sell operation
                    let start_time = Instant::now();
                    match swapx.build_swap_ixn_by_mint(&mint, None, sell_config, start_time).await {
                        Ok(result) => {
                            // Send instructions and confirm
                            let (keypair, instructions, token_price) = (result.0, result.1, result.2);
                            let recent_blockhash = match rpc_nonblocking_client.get_latest_blockhash().await {
                                Ok(hash) => hash,
                                Err(e) => {
                                    logger_for_selling.log(format!(
                                        "Error getting blockhash for force-selling {}: {}", mint, e
                                    ).red().to_string());
                                    return;
                                }
                            };
                            
                            match tx::new_signed_and_send_zeroslot(
                                recent_blockhash,
                                &keypair,
                                instructions,
                                &logger_for_selling,
                            ).await {
                                Ok(res) => {
                                    let sold_pool = LiquidityPool {
                                        mint: mint.clone(),
                                        buy_price: existing_pool.buy_price,
                                        sell_price: token_price,
                                        status: Status::Sold,
                                        timestamp: Some(Instant::now()),
                                    };
                                    
                                    // Update pool status to sold
                                    {
                                        let mut pools = pools_clone_for_selling.lock().unwrap();
                                        pools.retain(|pool| pool.mint != mint);
                                        pools.insert(sold_pool.clone());
                                    }
                                    
                                    logger_for_selling.log(format!(
                                        "\n\t * [SUCCESSFUL FORCE-SELL] => TX_HASH: (https://solscan.io/tx/{}) \n\t * [POOL] => ({}) \n\t * [SOLD] => {} :: ({:?}).",
                                        &res[0], mint, Utc::now(), start_time.elapsed()
                                    ).green().to_string());
                                    
                                    // Check if all tokens are sold
                                    let all_sold = {
                                        let pools = pools_clone_for_selling.lock().unwrap();
                                        !pools.iter().any(|pool| pool.status == Status::Bought)
                                    };
                                    
                                    if all_sold {
                                        // If all tokens are sold, enable buying
                                        let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                        *buying_enabled = true;
                                        
                                        logger_for_selling.log(
                                            "\n\t * [BUYING ENABLED] => All tokens sold, can buy new tokens now"
                                            .green()
                                            .to_string(),
                                        );
                                    }
                                },
                                Err(e) => {
                                    logger_for_selling.log(format!(
                                        "Force-sell failed for {}: {}", mint, e
                                    ).red().to_string());
                                }
                            }
                        },
                        Err(e) => {
                            logger_for_selling.log(format!(
                                "Error building swap instruction for force-selling {}: {}", mint, e
                            ).red().to_string());
                        }
                    }
                });
            }
        }
    });

    // In new_token_trader_pumpfun after the heartbeat task
    // Add a connection health check task
    let logger_health = logger.clone(); 
    tokio::spawn(async move {
        let health_logger = logger_health.clone();
        let mut interval = time::interval(Duration::from_secs(300)); // 5 minutes
        
        loop {
            interval.tick().await;
            health_logger.log("[CONNECTION HEALTH] => gRPC subscription still active".green().to_string());
        }
    });

    // In new_token_trader_pumpfun after the health check task:
    // Add a connection watchdog task
    let logger_watchdog = logger.clone();
    tokio::spawn(async move {
        let watchdog_logger = logger_watchdog;
        let mut interval = time::interval(Duration::from_secs(120)); // Check every 2 minutes
        
        loop {
            interval.tick().await;
            check_connection_health(&watchdog_logger).await;
        }
    });

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                // Process ping/pong messages
                if let Err(e) = process_stream_message(&msg, &subscribe_tx, &logger).await {
                    logger.log(format!("Error handling stream message: {}", e).red().to_string());
                    continue;
                }
                
                // Process transaction messages
                if let Some(UpdateOneof::Transaction(txn)) = msg.update_oneof {
                    let start_time = Instant::now();
                    if let Some(log_messages) = txn
                        .clone()
                        .transaction
                        .and_then(|txn1| txn1.meta)
                        .map(|meta| meta.log_messages)
                    {
                        let mut mint_flag = false;
                        let trade_info = match TradeInfoFromToken::from_json(txn.clone(), log_messages.clone()) {
                            Ok(info) => info,
                            Err(e) => {
                                logger.log(
                                    format!("Error in parsing txn: {}", e)
                                        .red()
                                        .italic()
                                        .to_string(),
                                );
                                continue;
                            }
                        };

                        // Check if this is a buy transaction (PumpBuy or PumpSwapBuy)
                        let is_buy_transaction = matches!(trade_info.instruction_type, 
                            InstructionType::PumpBuy | InstructionType::PumpSwapBuy);
                        
                        // Process copy trading for buy transactions
                        if is_buy_transaction {
                            // Check if this transaction is from one of our copy trading addresses
                            let is_copy_trading_tx = filter_config.copy_trading_target_addresses.iter()
                                .any(|addr| trade_info.target == *addr);
                            
                            if is_copy_trading_tx {
                                logger.log(format!(
                                    "\n\t * [COPY TRADING BUY DETECTED] => (https://solscan.io/tx/{}) - SLOT:({}) \n\t * [TARGET] => ({}) \n\t * [TOKEN] => ({}) \n\t * [BUY AMOUNT] => ({}) SOL \n\t * [TIMESTAMP] => {} :: ({:?}).",
                                    trade_info.signature,
                                    trade_info.slot,
                                    trade_info.target,
                                    trade_info.mint,
                                    lamports_to_sol(trade_info.volume_change.abs() as u64),
                                    Utc::now(),
                                    start_time.elapsed(),
                                ).blue().to_string());

                                // Check if buying is enabled
                                let buying_enabled = {
                                    let enabled = BUYING_ENABLED.lock().unwrap();
                                    *enabled
                                };
                                
                                if !buying_enabled {
                                    logger.log(format!(
                                        "\n\t * [SKIPPING BUY] => Waiting for all tokens to be sold first"
                                    ).yellow().to_string());
                                    continue;
                                }

                                // Check dev buy amount
                                let dev_buy_amount = lamports_to_sol(trade_info.volume_change.abs() as u64);
                                if dev_buy_amount > max_dev_buy as f64 {
                                    logger.log(format!(
                                        "\n\t * [BUY AMOUNT EXCEEDS MAX] => {} > {}",
                                        dev_buy_amount, max_dev_buy
                                    ).yellow().to_string());
                                    continue;
                                }
                                if dev_buy_amount < min_dev_buy as f64 {
                                    logger.log(format!(
                                        "\n\t * [BUY AMOUNT BELOW MIN] => {} < {}",
                                        dev_buy_amount, min_dev_buy
                                    ).yellow().to_string());
                                    continue;
                                }

                                // Check if this token is already in our pools
                                let is_duplicate = {
                                    let pools = existing_liquidity_pools.lock().unwrap();
                                    pools.iter().any(|pool| pool.mint == trade_info.mint)
                                };
                                
                                if is_duplicate {
                                    logger.log(format!(
                                        "\n\t * [DUPLICATE TOKEN] => Token already in our pools: {}",
                                        trade_info.mint
                                    ).yellow().to_string());
                                    continue;
                                }

                                // Temporarily disable buying while we're processing this buy
                                {
                                    let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                    *buying_enabled = false;
                                }

                                // Clone the shared variables for this task
                                let swapx_clone = swapx.clone();
                                let logger_clone = logger.clone();
                                let mut swap_config_clone = (*Arc::clone(&swap_config)).clone();
                                let app_state_clone = Arc::clone(&app_state).clone();
                                let yellowstone_grpc_http_clone = yellowstone_grpc_http.clone();
                                let yellowstone_grpc_token_clone = yellowstone_grpc_token.clone();
                                
                                let mint_str = trade_info.mint.clone();
                                
                                // Get bonding curve information for PumpBuy transactions
                                let bonding_curve_info = if matches!(trade_info.instruction_type, InstructionType::PumpBuy) {
                                    // For PumpBuy, we need to get the bonding curve account
                                    let rpc_client = app_state_clone.rpc_client.clone();
                                    let mint_pubkey = Pubkey::from_str(&mint_str).unwrap_or_default();
                                    let pump_program = Pubkey::from_str(PUMP_PROGRAM).unwrap_or_default();
                                    
                                    match tokio::task::block_in_place(|| {
                                        futures::executor::block_on(async {
                                            get_bonding_curve_account(rpc_client, mint_pubkey, pump_program).await
                                        })
                                    }) {
                                        Ok((bonding_curve, _, reserves)) => {
                                            Some(BondingCurveInfo {
                                                bonding_curve,
                                                new_virtual_sol_reserve: reserves.virtual_sol_reserves,
                                                new_virtual_token_reserve: reserves.virtual_token_reserves,
                                            })
                                        },
                                        Err(e) => {
                                            logger.log(format!(
                                                "\n\t * [BONDING CURVE ERROR] => Failed to get bonding curve for {}: {}",
                                                mint_str, e
                                            ).red().to_string());
                                            trade_info.bonding_curve_info.clone()
                                        }
                                    }
                                } else {
                                    trade_info.bonding_curve_info.clone()
                                };
                                
                                let existing_liquidity_pools_clone = Arc::clone(&existing_liquidity_pools);
                                let recent_blockhash = trade_info.clone().recent_blockhash;
                                
                                // Determine trading amount based on comparing SOL amount and TOKEN_AMOUNT
                                let sol_amount = lamports_to_sol(trade_info.volume_change.abs() as u64);
                                let token_amount = trade_info.token_amount;
                                
                                // If token amount is smaller than SOL amount, use token amount for trading
                                if token_amount > 0.0 && token_amount < sol_amount {
                                    // Modify swap_config to use the detected token amount
                                    swap_config_clone.amount_in = token_amount;
                                    logger.log(format!(
                                        "\n\t * [USING TOKEN AMOUNT] => {}, SOL Amount: {}",
                                        token_amount, sol_amount
                                    ).green().to_string());
                                }
    
                                let task = tokio::spawn(async move {
                                    match swapx_clone
                                        .build_swap_ixn_by_mint(
                                            &mint_str,
                                            bonding_curve_info,
                                            swap_config_clone.clone(),
                                            start_time,
                                        )
                                        .await
                                    {
                                        Ok(result) => {
                                            let (keypair, instructions, token_price) =
                                                (result.0, result.1, result.2);
                                            
                                            match tx::new_signed_and_send_zeroslot(
                                                recent_blockhash,
                                                &keypair,
                                                instructions,
                                                &logger_clone,
                                            ).await {
                                                Ok(res) => {
                                                    let bought_pool = LiquidityPool {
                                                        mint: mint_str.clone(),
                                                        buy_price: token_price,
                                                        sell_price: 0_f64,
                                                        status: Status::Bought,
                                                        timestamp: Some(Instant::now()),
                                                    };
                                                    
                                                    // Create a local copy before modifying
                                                    {
                                                        let mut existing_pools =
                                                            existing_liquidity_pools_clone.lock().unwrap();
                                                        existing_pools.retain(|pool| pool.mint != mint_str);
                                                        existing_pools.insert(bought_pool.clone());
                                                        
                                                        // Log after modification within the lock scope
                                                        logger_clone.log(format!(
                                                            "\n\t * [SUCCESSFUL-COPY-BUY] => TX_HASH: (https://solscan.io/tx/{}) \n\t * [TOKEN] => ({}) \n\t * [DONE] => {} :: ({:?}) \n\t * [TOTAL TOKENS] => {}",
                                                            &res[0], mint_str, Utc::now(), start_time.elapsed(), existing_pools.len()
                                                        ).green().to_string());
                                                    }
                                                },
                                                Err(e) => {
                                                    logger_clone.log(
                                                        format!("Failed to copy buy for {}: {}", mint_str.clone(), e)
                                                            .red()
                                                            .italic()
                                                            .to_string(),
                                                    );
                                                    
                                                    // Re-enable buying since this one failed
                                                    let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                                    *buying_enabled = true;
                                                    
                                                    let failed_pool = LiquidityPool {
                                                        mint: mint_str.clone(),
                                                        buy_price: 0_f64,
                                                        sell_price: 0_f64,
                                                        status: Status::Failure,
                                                        timestamp: None,
                                                    };
                                                    
                                                    // Use a local scope for the mutex lock
                                                    {
                                                        let mut update_pools =
                                                            existing_liquidity_pools_clone.lock().unwrap();
                                                        update_pools.retain(|pool| pool.mint != mint_str);
                                                        update_pools.insert(failed_pool.clone());
                                                    }
                                                }
                                            }
                                        },
                                        Err(error) => {
                                            logger_clone.log(
                                                format!("Error building swap instruction: {}", error)
                                                    .red()
                                                    .italic()
                                                    .to_string(),
                                            );
                                            
                                            // Re-enable buying since this one failed
                                            let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                            *buying_enabled = true;
                                            
                                            let failed_pool = LiquidityPool {
                                                mint: mint_str.clone(),
                                                buy_price: 0_f64,
                                                sell_price: 0_f64,
                                                status: Status::Failure,
                                                timestamp: None,
                                            };
                                            
                                            // Use a local scope for the mutex lock
                                            {
                                                let mut update_pools =
                                                    existing_liquidity_pools_clone.lock().unwrap();
                                                update_pools.retain(|pool| pool.mint != mint_str);
                                                update_pools.insert(failed_pool.clone());
                                            }
                                        }
                                    }
                                });
                                drop(task);
                            }
                        }
                    }
                }
            }
            Err(error) => {
                logger.log(
                    format!("Yellowstone gRpc Error: {:?}", error)
                        .red()
                        .to_string(),
                );
                break;
            }
        }
    }
    Ok(())
}// Function to get the appropriate retracement levels based on PNL and time elapsed
fn get_retracement_levels(current_pnl: f64, time_elapsed_seconds: u64) -> Vec<RetracementLevel> {
    // For high PNL tokens with shorter holding time, use more aggressive retracement triggers
    if time_elapsed_seconds <= 30 {
        vec![
            RetracementLevel { percentage: 3, threshold: 2000, sell_amount: 100 },  // 2000%+ PNL
            RetracementLevel { percentage: 4, threshold: 1500, sell_amount: 50 },   // 1500%+ PNL
            RetracementLevel { percentage: 5, threshold: 1000, sell_amount: 40 },   // 1000%+ PNL
            RetracementLevel { percentage: 6, threshold: 800, sell_amount: 35 },    // 800%+ PNL
            RetracementLevel { percentage: 6, threshold: 700, sell_amount: 35 },    // 700%+ PNL
            RetracementLevel { percentage: 6, threshold: 600, sell_amount: 30 },    // 600%+ PNL
            RetracementLevel { percentage: 7, threshold: 500, sell_amount: 30 },    // 500%+ PNL
            RetracementLevel { percentage: 7, threshold: 400, sell_amount: 30 },    // 400%+ PNL
            RetracementLevel { percentage: 8, threshold: 300, sell_amount: 20 },    // 300%+ PNL
            RetracementLevel { percentage: 10, threshold: 200, sell_amount: 15 },   // 200%+ PNL
            RetracementLevel { percentage: 12, threshold: 100, sell_amount: 15 },   // 100%+ PNL
            // Lower PNL thresholds with larger retracement triggers
            RetracementLevel { percentage: 20, threshold: 50, sell_amount: 10 },    // 50%+ PNL
            RetracementLevel { percentage: 30, threshold: 30, sell_amount: 10 },    // 30%+ PNL
            RetracementLevel { percentage: 42, threshold: 20, sell_amount: 100 },   // 20%+ PNL
        ]
    } else if current_pnl > 500.0 {
        // For high PNL tokens with longer holding time, use more aggressive retracement triggers
        vec![
            RetracementLevel { percentage: 3, threshold: 2000, sell_amount: 100 },  // 2000%+ PNL
            RetracementLevel { percentage: 4, threshold: 1500, sell_amount: 50 },   // 1500%+ PNL
            RetracementLevel { percentage: 5, threshold: 1000, sell_amount: 40 },   // 1000%+ PNL
            RetracementLevel { percentage: 6, threshold: 800, sell_amount: 35 },    // 800%+ PNL
            RetracementLevel { percentage: 6, threshold: 700, sell_amount: 35 },    // 700%+ PNL
            RetracementLevel { percentage: 6, threshold: 600, sell_amount: 30 },    // 600%+ PNL
            RetracementLevel { percentage: 7, threshold: 500, sell_amount: 30 },    // 500%+ PNL
            RetracementLevel { percentage: 7, threshold: 400, sell_amount: 30 },    // 400%+ PNL
            RetracementLevel { percentage: 8, threshold: 300, sell_amount: 20 },    // 300%+ PNL
            RetracementLevel { percentage: 15, threshold: 20, sell_amount: 100 },   // Just 20%+ PNL but significant time passed
        ]
    } else if current_pnl > 200.0 {
        // Medium PNL tokens with longer holding time
        vec![
            RetracementLevel { percentage: 3, threshold: 2000, sell_amount: 100 },  // 2000%+ PNL
            RetracementLevel { percentage: 4, threshold: 1500, sell_amount: 50 },   // 1500%+ PNL
            RetracementLevel { percentage: 5, threshold: 1000, sell_amount: 40 },   // 1000%+ PNL
            RetracementLevel { percentage: 6, threshold: 800, sell_amount: 35 },    // 800%+ PNL
            RetracementLevel { percentage: 6, threshold: 700, sell_amount: 35 },    // 700%+ PNL
            RetracementLevel { percentage: 8, threshold: 600, sell_amount: 30 },    // 600%+ PNL
            RetracementLevel { percentage: 10, threshold: 500, sell_amount: 30 },   // 500%+ PNL
            RetracementLevel { percentage: 10, threshold: 400, sell_amount: 30 },   // 400%+ PNL
            RetracementLevel { percentage: 10, threshold: 300, sell_amount: 20 },   // 300%+ PNL
            RetracementLevel { percentage: 10, threshold: 200, sell_amount: 20 },   // 200%+ PNL
            RetracementLevel { percentage: 20, threshold: 20, sell_amount: 100 },   // Just 20%+ PNL but significant time passed
        ]
    } else {
        // Standard retracement levels for default scenario
        RETRACEMENT_LEVELS.to_vec()
    }
}

// Function to check if we should execute trailing stop
fn check_trailing_stop_loss(current_pnl: f64, top_pnl: f64, logger: &Logger) -> bool {
    if top_pnl > 10.0 && current_pnl < top_pnl * 0.4 {
        logger.log(format!(
            "\n\t * [EXECUTING TRAILING STOP] => Current PNL: {}%, Peak PNL: {}%, Drop: {}%",
            current_pnl, top_pnl, (top_pnl - current_pnl) / top_pnl * 100.0
        ).yellow().to_string());
        return true;
    }
    false
}

pub async fn copy_trader_pumpfun(
    yellowstone_grpc_http: String,
    yellowstone_grpc_token: String,
    app_state: AppState,
    swap_config: SwapConfig,
    time_exceed: u64,
    counter_limit: u64,
    min_dev_buy: u64,
    max_dev_buy: u64,
) -> Result<(), String> {
    // Log the copy trading configuration
    let logger = Logger::new("[COPY-TRADER] => ".blue().bold().to_string());
    
    // INITIAL SETTING FOR SUBSCRIBE
    // -----------------------------------------------------------------------------------------------------------------------------
    let mut client = GeyserGrpcClient::build_from_shared(yellowstone_grpc_http.clone())
        .map_err(|e| format!("Failed to build client: {}", e))?
        .x_token::<String>(Some(yellowstone_grpc_token.clone()))
        .map_err(|e| format!("Failed to set x_token: {}", e))?
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| format!("Failed to set tls config: {}", e))?
        .connect()
        .await
        .map_err(|e| format!("Failed to connect: {}", e))?;

    // Create additional clones for later use in tasks
    let yellowstone_grpc_http = Arc::new(yellowstone_grpc_http);
    let yellowstone_grpc_token = Arc::new(yellowstone_grpc_token);
    let app_state = Arc::new(app_state);
    let swap_config = Arc::new(swap_config);

    // Log the copy trading configuration
    let logger = Logger::new("[COPY-TRADER] => ".blue().bold().to_string());

    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 3;
    let (subscribe_tx, mut stream) = loop {
        match client.subscribe().await {
            Ok(pair) => break pair,
            Err(e) => {
                retry_count += 1;
                if retry_count >= MAX_RETRIES {
                    return Err(format!("Failed to subscribe after {} attempts: {}", MAX_RETRIES, e));
                }
                logger.log(format!(
                    "[CONNECTION ERROR] => Failed to subscribe (attempt {}/{}): {}. Retrying in 5 seconds...",
                    retry_count, MAX_RETRIES, e
                ).red().to_string());
                time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    // Convert to Arc to allow cloning across tasks
    let subscribe_tx = Arc::new(tokio::sync::Mutex::new(subscribe_tx));

    // Get copy trading configuration from environment
    let copy_trading_target_address = std::env::var("COPY_TRADING_TARGET_ADDRESS").ok();
    let is_multi_copy_trading = std::env::var("IS_MULTI_COPY_TRADING")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false);
    
    // Prepare target addresses for monitoring
    let mut program_ids = vec![];
    let mut copy_trading_target_addresses = Vec::new();
    
    // Handle multiple copy trading targets if enabled
    if is_multi_copy_trading {
        if let Some(address_str) = copy_trading_target_address {
            // Parse comma-separated addresses
            for addr in address_str.split(',') {
                let trimmed_addr = addr.trim();
                if !trimmed_addr.is_empty() {
                    program_ids.push(trimmed_addr.to_string());
                    copy_trading_target_addresses.push(trimmed_addr.to_string());
                }
            }
        }
    } else if let Some(address) = copy_trading_target_address {
        // Single address mode
        if !address.is_empty() {
            program_ids.push(address.clone());
            copy_trading_target_addresses.push(address);
        }
    }

    // Ensure we have at least one target address
    if copy_trading_target_addresses.is_empty() {
        return Err("No COPY_TRADING_TARGET_ADDRESS specified. Please set this environment variable.".to_string());
    }

    let filter_config = FilterConfig {
        program_ids: program_ids.clone(),
        _instruction_discriminators: &[],
        copy_trading_target_addresses,
        is_multi_copy_trading,
    };

    // Log the copy trading configuration starts here
    logger.log(format!(
        "[COPY TRADING] => Monitoring {} address(es)",
        filter_config.copy_trading_target_addresses.len()
    ).green().to_string());
    
    for (i, addr) in filter_config.copy_trading_target_addresses.iter().enumerate() {
        logger.log(format!(
            "\t * [TARGET {}] => {}",
            i + 1, addr
        ).green().to_string());
    }

    subscribe_tx
        .lock()
        .await
        .send(SubscribeRequest {
            slots: HashMap::new(),
            accounts: HashMap::new(),
            transactions: hashmap! {
                "All".to_owned() => SubscribeRequestFilterTransactions {
                    vote: None,
                    failed: Some(false),
                    signature: None,
                    account_include: program_ids,
                    account_exclude: vec![JUPITER_PROGRAM.to_string(), OKX_DEX_PROGRAM.to_string()],
                    account_required: Vec::<String>::new()
                }
            },
            transactions_status: HashMap::new(),
            entry: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            commitment: Some(CommitmentLevel::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        })
        .await
        .map_err(|e| format!("Failed to send subscribe request: {}", e))?;

    let existing_liquidity_pools = Arc::new(Mutex::new(HashSet::<LiquidityPool>::new()));

    let rpc_nonblocking_client = app_state.clone().rpc_nonblocking_client.clone();
    let rpc_client = app_state.clone().rpc_client.clone();
    let wallet = app_state.clone().wallet.clone();
    let swapx = Pump::new(
        rpc_nonblocking_client.clone(),
        rpc_client.clone(),
        wallet.clone(),
    );

    logger.log("[STARTED. MONITORING COPY TARGETS]...".blue().bold().to_string());
    
    // Set buying enabled to true at start
    {
        let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
        *buying_enabled = true;
    }

    // After all setup and before the main loop, add a heartbeat ping task
    let subscribe_tx_clone = subscribe_tx.clone();
    let logger_clone = logger.clone();
    
    tokio::spawn(async move {
        let ping_logger = logger_clone.clone();
        let mut interval = time::interval(Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = send_heartbeat_ping(&subscribe_tx_clone, &ping_logger).await {
                ping_logger.log(format!("[CONNECTION ERROR] => {}", e).red().to_string());
                break;
            }
        }
    });

    // Start a background task to check the status of tokens periodically
    let existing_liquidity_pools_clone = Arc::clone(&existing_liquidity_pools);
    let logger_clone = logger.clone();
    let app_state_for_background = Arc::clone(&app_state);
    let swap_config_for_background = Arc::clone(&swap_config);
    
    tokio::spawn(async move {
        let pools_clone = Arc::clone(&existing_liquidity_pools_clone);
        let check_logger = logger_clone.clone();
        let app_state_clone = Arc::clone(&app_state_for_background);
        let swap_config_clone = Arc::clone(&swap_config_for_background);
        
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            // Check if there are any bought tokens and if any have exceeded MAX_WAIT_TIME
            let now = Instant::now();
            let max_wait_time_millis = *MAX_WAIT_TIME.lock().unwrap();
            let max_wait_duration = Duration::from_millis(max_wait_time_millis);
            
            let (has_bought_tokens, tokens_to_sell) = {
                let pools = pools_clone.lock().unwrap();
                let bought_tokens: Vec<String> = pools.iter()
                    .filter(|pool| pool.status == Status::Bought)
                    .map(|pool| pool.mint.clone())
                    .collect();
                
                let timed_out_tokens: Vec<(String, Instant)> = pools.iter()
                    .filter(|pool| pool.status == Status::Bought && 
                           pool.timestamp.map_or(false, |ts| now.duration_since(ts) > max_wait_duration))
                    .map(|pool| (pool.mint.clone(), pool.timestamp.unwrap()))
                    .collect();
                
                // Log bought tokens that are waiting to be sold
                if !bought_tokens.is_empty() {
                    check_logger.log(format!(
                        "\n\t * [BUYING PAUSED] => Waiting for tokens to be sold: {:?}",
                        bought_tokens
                    ).yellow().to_string());
                }
                
                // Log tokens that have timed out and will be force-sold
                if !timed_out_tokens.is_empty() {
                    check_logger.log(format!(
                        "\n\t * [TIMEOUT DETECTED] => Will force-sell tokens that exceeded {} ms wait time: {:?}",
                        max_wait_time_millis,
                        timed_out_tokens.iter().map(|(mint, _)| mint).collect::<Vec<_>>()
                    ).red().bold().to_string());
                }
                
                (bought_tokens.len() > 0, timed_out_tokens)
            };
            
            // Update buying status
            {
                let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                *buying_enabled = !has_bought_tokens;
                
            }
            
            // Force-sell tokens that have exceeded MAX_WAIT_TIME
            for (mint, timestamp) in tokens_to_sell {
                // Clone the necessary state for this token
                let logger_for_selling = check_logger.clone();
                let pools_clone_for_selling = Arc::clone(&pools_clone);
                let app_state_for_selling = app_state_clone.clone();
                let swap_config_for_selling = swap_config_clone.clone();
                
                check_logger.log(format!(
                    "\n\t * [FORCE SELLING] => Token {} exceeded wait time (elapsed: {:?})",
                    mint, now.duration_since(timestamp)
                ).red().to_string());
                
                tokio::spawn(async move {
                    // Get the existing pool for this mint
                    let existing_pool = {
                        let pools = pools_clone_for_selling.lock().unwrap();
                        pools.iter()
                            .find(|pool| pool.mint == mint)
                            .cloned()
                            .unwrap_or(LiquidityPool {
                                mint: mint.clone(),
                                buy_price: 0_f64,
                                sell_price: 0_f64,
                                status: Status::Bought,
                                timestamp: Some(timestamp),
                            })
                    };
                    
                    // Set up sell config
                    let sell_config = SwapConfig {
                        swap_direction: SwapDirection::Sell,
                        in_type: SwapInType::Pct,
                        amount_in: 1_f64,  // Sell 100%
                        slippage: 100_u64, // Use full slippage
                        use_jito: swap_config_for_selling.clone().use_jito,
                    };
                    
                    // Create Pump instance for selling
                    let app_state_for_task = app_state_for_selling.clone();
                    let rpc_nonblocking_client = app_state_for_task.rpc_nonblocking_client.clone();
                    let rpc_client = app_state_for_task.rpc_client.clone();
                    let wallet = app_state_for_task.wallet.clone();
                    let swapx = Pump::new(rpc_nonblocking_client.clone(), rpc_client.clone(), wallet.clone());
                    
                    // Execute the sell operation
                    let start_time = Instant::now();
                    match swapx.build_swap_ixn_by_mint(&mint, None, sell_config, start_time).await {
                        Ok(result) => {
                            // Send instructions and confirm
                            let (keypair, instructions, token_price) = (result.0, result.1, result.2);
                            let recent_blockhash = match rpc_nonblocking_client.get_latest_blockhash().await {
                                Ok(hash) => hash,
                                Err(e) => {
                                    logger_for_selling.log(format!(
                                        "Error getting blockhash for force-selling {}: {}", mint, e
                                    ).red().to_string());
                                    return;
                                }
                            };
                            
                            match tx::new_signed_and_send_zeroslot(
                                recent_blockhash,
                                &keypair,
                                instructions,
                                &logger_for_selling,
                            ).await {
                                Ok(res) => {
                                    let sold_pool = LiquidityPool {
                                        mint: mint.clone(),
                                        buy_price: existing_pool.buy_price,
                                        sell_price: token_price,
                                        status: Status::Sold,
                                        timestamp: Some(Instant::now()),
                                    };
                                    
                                    // Update pool status to sold
                                    {
                                        let mut pools = pools_clone_for_selling.lock().unwrap();
                                        pools.retain(|pool| pool.mint != mint);
                                        pools.insert(sold_pool.clone());
                                    }
                                    
                                    logger_for_selling.log(format!(
                                        "\n\t * [SUCCESSFUL FORCE-SELL] => TX_HASH: (https://solscan.io/tx/{}) \n\t * [POOL] => ({}) \n\t * [SOLD] => {} :: ({:?}).",
                                        &res[0], mint, Utc::now(), start_time.elapsed()
                                    ).green().to_string());
                                    
                                    // Check if all tokens are sold
                                    let all_sold = {
                                        let pools = pools_clone_for_selling.lock().unwrap();
                                        !pools.iter().any(|pool| pool.status == Status::Bought)
                                    };
                                    
                                    if all_sold {
                                        // If all tokens are sold, enable buying
                                        let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                        *buying_enabled = true;
                                        
                                        logger_for_selling.log(
                                            "\n\t * [BUYING ENABLED] => All tokens sold, can buy new tokens now"
                                            .green()
                                            .to_string(),
                                        );
                                    }
                                },
                                Err(e) => {
                                    logger_for_selling.log(format!(
                                        "Force-sell failed for {}: {}", mint, e
                                    ).red().to_string());
                                }
                            }
                        },
                        Err(e) => {
                            logger_for_selling.log(format!(
                                "Error building swap instruction for force-selling {}: {}", mint, e
                            ).red().to_string());
                        }
                    }
                });
            }
        }
    });

    // In copy_trader_pumpfun after the heartbeat task
    // Add a connection health check task
    let logger_health = logger.clone();
    tokio::spawn(async move {
        let health_logger = logger_health.clone();
        let mut interval = time::interval(Duration::from_secs(300)); // 5 minutes
        loop {
            interval.tick().await;
        }
    });

    // In copy_trader_pumpfun after the health check task:
    // Add a connection watchdog task
    let logger_watchdog = logger.clone();
    tokio::spawn(async move {
        let watchdog_logger = logger_watchdog;
        let mut interval = time::interval(Duration::from_secs(120)); // Check every 2 minutes
        
        loop {
            interval.tick().await;
            check_connection_health(&watchdog_logger).await;
        }
    });

    // In copy_trader_pumpfun after setting up the initial subscription and before the main event loop
    // Add a PNL monitoring and auto-sell task
    let pnl_liquidity_pools_clone = Arc::clone(&existing_liquidity_pools);
    let pnl_logger_clone = logger.clone();
    let pnl_app_state_clone = Arc::clone(&app_state);
    let pnl_swap_config_clone = Arc::clone(&swap_config);
    let pnl_token_tracking = Arc::clone(&TOKEN_TRACKING);

    tokio::spawn(async move {
        let pools_clone = Arc::clone(&pnl_liquidity_pools_clone);
        let pnl_check_logger = pnl_logger_clone.clone();
        let app_state_clone = Arc::clone(&pnl_app_state_clone);
        let swap_config_clone = Arc::clone(&pnl_swap_config_clone);
        let token_tracking = Arc::clone(&pnl_token_tracking);
        
        // Create PNL check interval - check every 5 seconds
        let mut interval = time::interval(Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            // Get current pools to check
            let tokens_to_check = {
                let pools = pools_clone.lock().unwrap();
                pools.iter()
                    .filter(|pool| pool.status == Status::Bought)
                    .map(|pool| pool.clone())
                    .collect::<Vec<LiquidityPool>>()
            };
            
            if tokens_to_check.is_empty() {
                continue;
            }
            
            pnl_check_logger.log(format!(
                "\n[PNL MONITOR] => Checking PNL for {} tokens",
                tokens_to_check.len()
            ).blue().to_string());
            
            // Check each token's current price and PNL
            for pool in tokens_to_check {
                let mint = pool.mint.clone();
                let buy_price = pool.buy_price;
                let bought_time = pool.timestamp.unwrap_or(Instant::now());
                let time_elapsed = Instant::now().duration_since(bought_time);
                
                // Clone necessary variables
                let logger_for_pnl = pnl_check_logger.clone();
                let pools_clone_for_pnl = Arc::clone(&pools_clone);
                let token_tracking_clone = Arc::clone(&token_tracking);
                let app_state_for_pnl = app_state_clone.clone();
                let swap_config_for_pnl = Arc::clone(&swap_config_clone);
                
                // Create Pump instance for price checking
                let rpc_nonblocking_client = app_state_for_pnl.rpc_nonblocking_client.clone();
                let rpc_client = app_state_for_pnl.rpc_client.clone();
                let wallet = app_state_for_pnl.wallet.clone();
                let swapx = Pump::new(rpc_nonblocking_client.clone(), rpc_client.clone(), wallet.clone());
                
                // Execute as a separate task to avoid blocking PNL check loop
                tokio::spawn(async move {
                    // Get current price estimate
                    let current_price = match swapx.get_token_price(&mint).await {
                        Ok(price) => price,
                        Err(e) => {
                            logger_for_pnl.log(format!(
                                "[PNL ERROR] => Failed to get current price for {}: {}",
                                mint, e
                            ).red().to_string());
                            return;
                        }
                    };
                    
                    // Calculate PNL
                    let pnl = if buy_price > 0.0 {
                        ((current_price - buy_price) / buy_price) * 100.0
                    } else {
                        0.0
                    };
                    
                    // Get or create token tracking info
                    let mut tracking_info = {
                        let mut tracking = token_tracking_clone.lock().unwrap();
                        tracking.entry(mint.clone()).or_insert_with(|| TokenTrackingInfo {
                            top_pnl: pnl,
                            last_sell_time: Instant::now(),
                            completed_intervals: HashSet::new(),
                        }).clone()
                    };
                    
                    // Update top PNL if current PNL is higher
                    if pnl > tracking_info.top_pnl {
                        let mut tracking = token_tracking_clone.lock().unwrap();
                        if let Some(info) = tracking.get_mut(&mint) {
                            info.top_pnl = pnl;
                        }
                        tracking_info.top_pnl = pnl;
                        
                        logger_for_pnl.log(format!(
                            "\n[PNL PEAK] => Token {} reached new peak PNL: {:.2}%",
                            mint, pnl
                        ).green().bold().to_string());
                    }
                    
                    // Log current PNL status
                    logger_for_pnl.log(format!(
                        "[PNL STATUS] => Token: {} | Buy: ${:.6} | Current: ${:.6} | PNL: {:.2}% | Peak PNL: {:.2}% | Time: {:?}",
                        mint, buy_price, current_price, pnl, tracking_info.top_pnl, time_elapsed
                    ).cyan().to_string());
                    
                    // Get appropriate retracement levels based on current PNL and time
                    let retracement_levels = get_retracement_levels(pnl, time_elapsed.as_secs());
                    
                    // Decision variables
                    let mut should_sell = false;
                    let mut sell_percentage = 0;
                    let mut sell_reason = String::new();
                    
                    // Check if we should sell based on time-based thresholds
                    // First check - Time-based PNL thresholds (more aggressive selling with time)
                    if time_elapsed.as_secs() > 300 { // If held for more than 5 minutes
                        for level in &retracement_levels {
                            if pnl >= level.threshold as f64 {
                                // Calculate how much the price has retraced from peak
                                let retracement = ((tracking_info.top_pnl - pnl) / tracking_info.top_pnl) * 100.0;
                                
                                if retracement >= level.percentage as f64 {
                                    should_sell = true;
                                    sell_percentage = level.sell_amount;
                                    sell_reason = format!(
                                        "Retracement of {:.2}% from peak PNL of {:.2}%", 
                                        retracement, tracking_info.top_pnl
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    
                    // Second check - Trailing stop loss for significant drops from peak
                    if !should_sell && check_trailing_stop_loss(pnl, tracking_info.top_pnl, &logger_for_pnl) {
                        should_sell = true;
                        sell_percentage = 100; // Sell all when trailing stop triggers
                        sell_reason = format!(
                            "Trailing stop loss triggered. Current PNL: {:.2}%, Peak PNL: {:.2}%", 
                            pnl, tracking_info.top_pnl
                        );
                    }
                    
                    // Third check - Fixed take-profit levels
                    if !should_sell {
                        for (threshold, percentage) in TAKE_PROFIT_LEVELS.iter() {
                            let threshold_key = format!("take_profit_{}", threshold);
                            
                            // Only trigger if we haven't sold at this level before and PNL exceeds threshold
                            if pnl >= *threshold as f64 && !tracking_info.completed_intervals.contains(&threshold_key) {
                                should_sell = true;
                                sell_percentage = *percentage;
                                sell_reason = format!(
                                    "Take profit at {:.2}% PNL threshold ({:.2}% of holding)", 
                                    pnl, percentage
                                );
                                
                                // Mark this threshold as completed
                                {
                                    let mut tracking = token_tracking_clone.lock().unwrap();
                                    if let Some(info) = tracking.get_mut(&mint) {
                                        info.completed_intervals.insert(threshold_key);
                                    }
                                }
                                break;
                            }
                        }
                    }
                    
                    // Fourth check - Force sell if PNL drops below threshold after significant profit
                    if !should_sell && tracking_info.top_pnl > 100.0 && pnl < 20.0 {
                        should_sell = true;
                        sell_percentage = 100;
                        sell_reason = format!(
                            "Emergency exit: PNL dropped to {:.2}% after peaking at {:.2}%", 
                            pnl, tracking_info.top_pnl
                        );
                    }
                    
                    // Execute sell if conditions are met
                    if should_sell {
                        logger_for_pnl.log(format!(
                            "\n[SELL DECISION] => Token: {} | Selling {}% | Reason: {}",
                            mint, sell_percentage, sell_reason
                        ).yellow().bold().to_string());
                        
                        // Create sell configuration
                        let sell_config = SwapConfig {
                            swap_direction: SwapDirection::Sell,
                            in_type: SwapInType::Pct,
                            amount_in: sell_percentage as f64 / 100.0,  // Convert percentage to decimal
                            slippage: 100_u64, // Use full slippage for sell
                            use_jito: swap_config_for_pnl.use_jito,
                        };
                        
                        // Execute the sell
                        let start_time = Instant::now();
                        match swapx.build_swap_ixn_by_mint(&mint, None, sell_config, start_time).await {
                            Ok(result) => {
                                // Send instructions and confirm
                                let (keypair, instructions, token_price) = (result.0, result.1, result.2);
                                let recent_blockhash = match rpc_nonblocking_client.get_latest_blockhash().await {
                                    Ok(hash) => hash,
                                    Err(e) => {
                                        logger_for_pnl.log(format!(
                                            "Error getting blockhash for selling {}: {}", mint, e
                                        ).red().to_string());
                                        return;
                                    }
                                };
                                
                                match tx::new_signed_and_send_zeroslot(
                                    recent_blockhash,
                                    &keypair,
                                    instructions,
                                    &logger_for_pnl,
                                ).await {
                                    Ok(res) => {
                                        // Update pool status
                                        if sell_percentage >= 100 {
                                            // If selling all, mark as sold
                                            let sold_pool = LiquidityPool {
                                                mint: mint.clone(),
                                                buy_price: buy_price,
                                                sell_price: token_price,
                                                status: Status::Sold,
                                                timestamp: Some(Instant::now()),
                                            };
                                            
                                            {
                                                let mut pools = pools_clone_for_pnl.lock().unwrap();
                                                pools.retain(|pool| pool.mint != mint);
                                                pools.insert(sold_pool.clone());
                                            }
                                            
                                            // Clean up tracking info
                                            {
                                                let mut tracking = token_tracking_clone.lock().unwrap();
                                                tracking.remove(&mint);
                                            }
                                            
                                            logger_for_pnl.log(format!(
                                                "\n[COMPLETE SELL] => TX_HASH: (https://solscan.io/tx/{}) \n\t * [POOL] => ({}) \n\t * [PNL] => {:.2}% \n\t * [SOLD] => {} :: ({:?}).",
                                                &res[0], mint, pnl, Local::now(), start_time.elapsed()
                                            ).green().bold().to_string());
                                            
                                            // Check if all tokens are sold to re-enable buying
                                            let all_sold = {
                                                let pools = pools_clone_for_pnl.lock().unwrap();
                                                !pools.iter().any(|pool| pool.status == Status::Bought)
                                            };
                                            
                                            if all_sold {
                                                let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                                *buying_enabled = true;
                                                
                                                logger_for_pnl.log(
                                                    "\n[BUYING ENABLED] => All tokens sold, can buy new tokens now"
                                                    .green()
                                                    .to_string(),
                                                );
                                            }
                                        } else {
                                            // If partial sell, just update timestamp
                                            {
                                                let mut pools = pools_clone_for_pnl.lock().unwrap();
                                                if let Some(pool) = pools.iter().find(|p| p.mint == mint) {
                                                    let mut updated_pool = pool.clone();
                                                    updated_pool.timestamp = Some(Instant::now());
                                                    pools.retain(|p| p.mint != mint);
                                                    pools.insert(updated_pool);
                                                }
                                            }
                                            
                                            // Update last sell time
                                            {
                                                let mut tracking = token_tracking_clone.lock().unwrap();
                                                if let Some(info) = tracking.get_mut(&mint) {
                                                    info.last_sell_time = Instant::now();
                                                }
                                            }
                                            
                                            logger_for_pnl.log(format!(
                                                "\n[PARTIAL SELL] => TX_HASH: (https://solscan.io/tx/{}) \n\t * [POOL] => ({}) \n\t * [PNL] => {:.2}% \n\t * [SOLD {}%] => {} :: ({:?}).",
                                                &res[0], mint, pnl, sell_percentage, Local::now(), start_time.elapsed()
                                            ).yellow().bold().to_string());
                                        }
                                    },
                                    Err(e) => {
                                        logger_for_pnl.log(format!(
                                            "Sell failed for {}: {}", mint, e
                                        ).red().to_string());
                                    }
                                }
                            },
                            Err(e) => {
                                logger_for_pnl.log(format!(
                                    "Error building swap instruction for selling {}: {}", mint, e
                                ).red().to_string());
                            }
                        }
                    }
                });
            }
        }
    });

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                // Process ping/pong messages
                if let Err(e) = process_stream_message(&msg, &subscribe_tx, &logger).await {
                    logger.log(format!("Error handling stream message: {}", e).red().to_string());
                    continue;
                }
                
                // Process transaction messages
                if let Some(UpdateOneof::Transaction(txn)) = msg.update_oneof {
                    let start_time = Instant::now();
                    if let Some(log_messages) = txn
                        .clone()
                        .transaction
                        .and_then(|txn1| txn1.meta)
                        .map(|meta| meta.log_messages)
                    {
                        let mut mint_flag = false;
                        let trade_info = match TradeInfoFromToken::from_json(txn.clone(), log_messages.clone()) {
                            Ok(info) => info,
                            Err(e) => {
                                logger.log(
                                    format!("Error in parsing txn: {}", e)
                                        .red()
                                        .italic()
                                        .to_string(),
                                );
                                continue;
                            }
                        };

                        // Check if this is a buy transaction (PumpBuy or PumpSwapBuy)
                        let is_buy_transaction = matches!(trade_info.instruction_type, 
                            InstructionType::PumpBuy | InstructionType::PumpSwapBuy);
                        
                        // Process copy trading for buy transactions
                        if is_buy_transaction {
                            // Check if this transaction is from one of our copy trading addresses
                            let is_copy_trading_tx = filter_config.copy_trading_target_addresses.iter()
                                .any(|addr| trade_info.target == *addr);
                            
                            if is_copy_trading_tx {
                                logger.log(format!(
                                    "\n\t * [COPY TRADING BUY DETECTED] => (https://solscan.io/tx/{}) - SLOT:({}) \n\t * [TARGET] => ({}) \n\t * [TOKEN] => ({}) \n\t * [BUY AMOUNT] => ({}) SOL \n\t * [TIMESTAMP] => {} :: ({:?}).",
                                    trade_info.signature,
                                    trade_info.slot,
                                    trade_info.target,
                                    trade_info.mint,
                                    lamports_to_sol(trade_info.volume_change.abs() as u64),
                                    Utc::now(),
                                    start_time.elapsed(),
                                ).blue().to_string());

                                // Check if buying is enabled
                                let buying_enabled = {
                                    let enabled = BUYING_ENABLED.lock().unwrap();
                                    *enabled
                                };
                                
                                if !buying_enabled {
                                    logger.log(format!(
                                        "\n\t * [SKIPPING BUY] => Waiting for all tokens to be sold first"
                                    ).yellow().to_string());
                                    continue;
                                }

                                // Check dev buy amount
                                let dev_buy_amount = lamports_to_sol(trade_info.volume_change.abs() as u64);
                                if dev_buy_amount > max_dev_buy as f64 {
                                    logger.log(format!(
                                        "\n\t * [BUY AMOUNT EXCEEDS MAX] => {} > {}",
                                        dev_buy_amount, max_dev_buy
                                    ).yellow().to_string());
                                    continue;
                                }
                                if dev_buy_amount < min_dev_buy as f64 {
                                    logger.log(format!(
                                        "\n\t * [BUY AMOUNT BELOW MIN] => {} < {}",
                                        dev_buy_amount, min_dev_buy
                                    ).yellow().to_string());
                                    continue;
                                }

                                // Check if this token is already in our pools
                                let is_duplicate = {
                                    let pools = existing_liquidity_pools.lock().unwrap();
                                    pools.iter().any(|pool| pool.mint == trade_info.mint)
                                };
                                
                                if is_duplicate {
                                    logger.log(format!(
                                        "\n\t * [DUPLICATE TOKEN] => Token already in our pools: {}",
                                        trade_info.mint
                                    ).yellow().to_string());
                                    continue;
                                }

                                // Temporarily disable buying while we're processing this buy
                                {
                                    let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                    *buying_enabled = false;
                                }

                                // Clone the shared variables for this task
                                let swapx_clone = swapx.clone();
                                let logger_clone = logger.clone();
                                let mut swap_config_clone = (*Arc::clone(&swap_config)).clone();
                                let app_state_clone = Arc::clone(&app_state).clone();
                                let yellowstone_grpc_http_clone = yellowstone_grpc_http.clone();
                                let yellowstone_grpc_token_clone = yellowstone_grpc_token.clone();
                                
                                let mint_str = trade_info.mint.clone();
                                
                                // Get bonding curve information for PumpBuy transactions
                                let bonding_curve_info = if matches!(trade_info.instruction_type, InstructionType::PumpBuy) {
                                    // For PumpBuy, we need to get the bonding curve account
                                    let rpc_client = app_state_clone.rpc_client.clone();
                                    let mint_pubkey = Pubkey::from_str(&mint_str).unwrap_or_default();
                                    let pump_program = Pubkey::from_str(PUMP_PROGRAM).unwrap_or_default();
                                    
                                    match tokio::task::block_in_place(|| {
                                        futures::executor::block_on(async {
                                            get_bonding_curve_account(rpc_client, mint_pubkey, pump_program).await
                                        })
                                    }) {
                                        Ok((bonding_curve, _, reserves)) => {
                                            Some(BondingCurveInfo {
                                                bonding_curve,
                                                new_virtual_sol_reserve: reserves.virtual_sol_reserves,
                                                new_virtual_token_reserve: reserves.virtual_token_reserves,
                                            })
                                        },
                                        Err(e) => {
                                            logger.log(format!(
                                                "\n\t * [BONDING CURVE ERROR] => Failed to get bonding curve for {}: {}",
                                                mint_str, e
                                            ).red().to_string());
                                            trade_info.bonding_curve_info.clone()
                                        }
                                    }
                                } else {
                                    trade_info.bonding_curve_info.clone()
                                };
                                
                                let existing_liquidity_pools_clone = Arc::clone(&existing_liquidity_pools);
                                let recent_blockhash = trade_info.clone().recent_blockhash;
                                
                                // Determine trading amount based on comparing SOL amount and TOKEN_AMOUNT
                                let sol_amount = lamports_to_sol(trade_info.volume_change.abs() as u64);
                                let token_amount = trade_info.token_amount;
                                
                                // If token amount is smaller than SOL amount, use token amount for trading
                                if token_amount > 0.0 && token_amount < sol_amount {
                                    // Modify swap_config to use the detected token amount
                                    swap_config_clone.amount_in = token_amount;
                                    logger.log(format!(
                                        "\n\t * [USING TOKEN AMOUNT] => {}, SOL Amount: {}",
                                        token_amount, sol_amount
                                    ).green().to_string());
                                }
    
                                let task = tokio::spawn(async move {
                                    match swapx_clone
                                        .build_swap_ixn_by_mint(
                                            &mint_str,
                                            bonding_curve_info,
                                            swap_config_clone.clone(),
                                            start_time,
                                        )
                                        .await
                                    {
                                        Ok(result) => {
                                            let (keypair, instructions, token_price) =
                                                (result.0, result.1, result.2);
                                            
                                            match tx::new_signed_and_send_zeroslot(
                                                recent_blockhash,
                                                &keypair,
                                                instructions,
                                                &logger_clone,
                                            ).await {
                                                Ok(res) => {
                                                    let bought_pool = LiquidityPool {
                                                        mint: mint_str.clone(),
                                                        buy_price: token_price,
                                                        sell_price: 0_f64,
                                                        status: Status::Bought,
                                                        timestamp: Some(Instant::now()),
                                                    };
                                                    
                                                    // Create a local copy before modifying
                                                    {
                                                        let mut existing_pools =
                                                            existing_liquidity_pools_clone.lock().unwrap();
                                                        existing_pools.retain(|pool| pool.mint != mint_str);
                                                        existing_pools.insert(bought_pool.clone());
                                                        
                                                        // Log after modification within the lock scope
                                                        logger_clone.log(format!(
                                                            "\n\t * [SUCCESSFUL-COPY-BUY] => TX_HASH: (https://solscan.io/tx/{}) \n\t * [TOKEN] => ({}) \n\t * [DONE] => {} :: ({:?}) \n\t * [TOTAL TOKENS] => {}",
                                                            &res[0], mint_str, Utc::now(), start_time.elapsed(), existing_pools.len()
                                                        ).green().to_string());
                                                    }
                                                },
                                                Err(e) => {
                                                    logger_clone.log(
                                                        format!("Failed to copy buy for {}: {}", mint_str.clone(), e)
                                                            .red()
                                                            .italic()
                                                            .to_string(),
                                                    );
                                                    
                                                    // Re-enable buying since this one failed
                                                    let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                                    *buying_enabled = true;
                                                    
                                                    let failed_pool = LiquidityPool {
                                                        mint: mint_str.clone(),
                                                        buy_price: 0_f64,
                                                        sell_price: 0_f64,
                                                        status: Status::Failure,
                                                        timestamp: None,
                                                    };
                                                    
                                                    // Use a local scope for the mutex lock
                                                    {
                                                        let mut update_pools =
                                                            existing_liquidity_pools_clone.lock().unwrap();
                                                        update_pools.retain(|pool| pool.mint != mint_str);
                                                        update_pools.insert(failed_pool.clone());
                                                    }
                                                }
                                            }
                                        },
                                        Err(error) => {
                                            logger_clone.log(
                                                format!("Error building swap instruction: {}", error)
                                                    .red()
                                                    .italic()
                                                    .to_string(),
                                            );
                                            
                                            // Re-enable buying since this one failed
                                            let mut buying_enabled = BUYING_ENABLED.lock().unwrap();
                                            *buying_enabled = true;
                                            
                                            let failed_pool = LiquidityPool {
                                                mint: mint_str.clone(),
                                                buy_price: 0_f64,
                                                sell_price: 0_f64,
                                                status: Status::Failure,
                                                timestamp: None,
                                            };
                                            
                                            // Use a local scope for the mutex lock
                                            {
                                                let mut update_pools =
                                                    existing_liquidity_pools_clone.lock().unwrap();
                                                update_pools.retain(|pool| pool.mint != mint_str);
                                                update_pools.insert(failed_pool.clone());
                                            }
                                        }
                                    }
                                });
                                drop(task);
                            }
                        }
                    }
                }
            }
            Err(error) => {
                logger.log(
                    format!("Yellowstone gRpc Error: {:?}", error)
                        .red()
                        .to_string(),
                );
                break;
            }
        }
    }
    Ok(())
}



