use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::process;
use std::string::String;

use csv_async::{AsyncReaderBuilder, Trim};
use futures::StreamExt;
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use strum::{EnumString, IntoStaticStr}; // most are already in 0.28
use tokio::fs::File;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumString, IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[strum(ascii_case_insensitive)]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
struct Input {
    #[serde(rename = "type")]
    action: TxType,
    client: u16,
    tx: u32,
    #[serde(deserialize_with = "etl_amount")]
    amount: Option<i128>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TxState {
    Healthy,
    Disputed,
    Resolved,
    Changedback,
    Invalid,
}

#[derive(Debug)]
struct Account {
    available: i128,
    held: i128,
    total: i128,
    locked: bool,
    actions: HashMap<u32, (Input, TxState)>,
}

fn etl_amount<'de, D>(deserializer: D) -> Result<Option<i128>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut s: String = Deserialize::deserialize(deserializer)?;
    s = s.trim().to_string();

    if s.is_empty() {
        return Ok(None);
    }

    let is_negative = s.chars().next().unwrap().eq(&'-');

    if is_negative {
        s = s[1..].to_string();
    }

    // Two-way algorith is used; effecient for small, simple, data sets.
    if let Some(decimal) = s.find('.') {
        let integer = &s[..decimal];
        let digits = &s[decimal + 1..];

        let decimal = match digits.len() {
            0 => Ok(String::from("0000")),
            1 => Ok(String::from(digits) + "000"),
            2 => Ok(String::from(digits) + "00"),
            3 => Ok(String::from(digits) + "0"),
            4 => Ok(String::from(digits)),
            _ => Err(D::Error::custom("Decimal exceeds four places")),
        }?;

        s = integer.to_string() + &decimal;
    }

    match s.parse::<i128>() {
        Ok(res) => {
            if is_negative {
                return Ok(Some(-res));
            }
            Ok(Some(res))
        }
        Err(err) => Err(D::Error::custom(format!(
            "Failed to parse string to i64: {}",
            err
        ))),
    }
}

fn amount_to_string(amount: i128) -> String {
    let sign = if amount.is_negative() { "-" } else { "" };
    let integers = (amount.abs() / 10000i128).to_string();
    let decimal = format!("{:04}", (amount.abs() % 10000i128));

    sign.to_string() + &integers + "." + &decimal
}

#[tokio::main]
async fn main() {
    if env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt::init();
    }

    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        tracing::error!("Error: Must specify at least one argument (input file path).");
        process::exit(1);
    }

    let path = Path::new(&args[1]);
    if !path.exists() {
        tracing::error!("File does not exist: {}", path.display());
        process::exit(1);
    }
    if !path.is_file() {
        tracing::error!("Error: Specified input is not a file: {}", path.display());
        process::exit(1);
    }

    let file = match File::open(path).await {
        Ok(file) => file,
        Err(err) => {
            tracing::error!("Error: Failed to open file: {}", err);
            process::exit(1);
        }
    };

    let mut reader = AsyncReaderBuilder::new()
        .trim(Trim::All)
        .create_deserializer(file);

    let mut stream = reader.deserialize::<Input>();
    let mut state: HashMap<u16, Account> = HashMap::new();

    while let Some(result) = stream.next().await {
        let result = match result {
            Ok(ok) => ok,
            Err(err) => {
                tracing::warn!("Retrieving `result` failed: {}", err);
                continue;
            }
        };

        let account = state.get_mut(&result.client);

        match result.action {
            TxType::Deposit => {
                let client = result.client;
                let amount = result.amount.unwrap_or_default();

                if amount <= 0 {
                    tracing::warn!(
                        "Skipping: `TxType::Deposit` amount leq zero - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                if account.is_none() {
                    let mut map = HashMap::new();
                    map.insert(result.tx, (result, TxState::Healthy));

                    state.insert(
                        client,
                        Account {
                            available: amount,
                            held: 0,
                            total: amount,
                            locked: false,
                            actions: map,
                        },
                    );

                    continue;
                }

                let account = account.expect("Expected `account` to be not `Some()`.");

                if account.locked {
                    tracing::warn!(
                        "Skipping: `TxType::Deposit` account locked - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                account.available += amount;
                account.total += amount;

                account
                    .actions
                    .insert(result.tx, (result, TxState::Healthy));
            }
            TxType::Withdrawal => {
                let client = result.client;
                let amount = result.amount.unwrap_or_default();

                if account.is_none() {
                    tracing::warn!(
                        "Skipping: `TxType::Withdraw` account not found; creating record - {}",
                        result.client
                    );

                    let mut map = HashMap::new();
                    map.insert(result.tx, (result, TxState::Healthy));

                    state.insert(
                        client,
                        Account {
                            available: 0,
                            held: 0,
                            total: 0,
                            locked: false,
                            actions: map,
                        },
                    );

                    continue;
                }

                let account = account.expect("Expected `account` to be `Some()`.");

                if account.locked {
                    tracing::warn!(
                        "Skipping: `TxType::Withdraw` account locked - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                if amount > account.available {
                    tracing::warn!(
                        "Skipping: `TxType::Withdraw` not enough funds - {} available: {}, total: {}",
                        result.client,
                        account.available,
                        account.total,
                    );

                    continue;
                }

                account.available -= amount;
                account.total -= amount;

                account
                    .actions
                    .insert(result.tx, (result, TxState::Healthy));
            }
            TxType::Dispute => {
                if account.is_none() {
                    tracing::warn!(
                        "Skipping: `TxType::Dispute` account not found; creating record - {}",
                        result.client
                    );

                    state.insert(
                        result.client,
                        Account {
                            available: 0,
                            held: 0,
                            total: 0,
                            locked: false,
                            actions: HashMap::new(),
                        },
                    );

                    continue;
                }

                let account = account.expect("Expected `account` to be not `Some()`.");

                if account.locked {
                    tracing::warn!(
                        "Skipping: `TxType::Dispute` account locked - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                assert!(result.tx > 0, "ERROR: Transaction index is 0");
                let (tx, state) = match account.actions.get_mut(&result.tx) {
                    Some(tx) => tx,
                    None => {
                        tracing::warn!(
                            "Skipping: `TxType::Dispute` no transaction found - {} transaction: {}",
                            result.client,
                            result.tx
                        );
                        continue;
                    }
                };

                if (*state).eq(&TxState::Disputed) {
                    tracing::warn!(
                        "Skipping: `TxType::Dispute` transaction is disputed - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                if !tx.action.eq(&TxType::Deposit) {
                    tracing::warn!(
                        "Skipping: `TxType::Dispute` transaction found is not deposit - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                let tx_amount = tx.amount.unwrap_or_default();

                account.available -= tx_amount;
                account.held += tx_amount;

                *state = TxState::Disputed;
            }
            TxType::Resolve => {
                if account.is_none() {
                    tracing::warn!(
                        "Skipping: `TxType::Resolve` account not found; creating record - {}",
                        result.client
                    );

                    state.insert(
                        result.client,
                        Account {
                            available: 0,
                            held: 0,
                            total: 0,
                            locked: false,
                            actions: HashMap::new(),
                        },
                    );

                    continue;
                }

                let account = account.expect("Expected `account` to be `Some()`.");

                if account.locked {
                    tracing::warn!(
                        "Skipping: `TxType::Resolve` account locked - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                assert!(result.tx > 0, "ERROR: Transaction index is 0");
                let (tx, state) = match account.actions.get_mut(&result.tx) {
                    Some(tx) => tx,
                    None => {
                        tracing::warn!(
                            "Skipping: `TxType::Resolve` no transaction found - {} transaction: {}",
                            result.client,
                            result.tx
                        );
                        continue;
                    }
                };

                if !(*state).eq(&TxState::Disputed) {
                    tracing::warn!(
                        "Skipping: `TxType::Dispute` transaction is not disputed - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }
                assert!(tx.action.eq(&TxType::Deposit));

                let tx_amount = tx.amount.unwrap_or_default();

                *state = TxState::Resolved;
                account.held -= tx_amount;
                account.available += tx_amount;
            }
            TxType::Chargeback => {
                if account.is_none() {
                    tracing::warn!(
                        "Skipping: `TxType::Chargeback` account not found; creating record - {}",
                        result.client
                    );

                    state.insert(
                        result.client,
                        Account {
                            available: 0,
                            held: 0,
                            total: 0,
                            locked: false,
                            actions: HashMap::new(),
                        },
                    );

                    continue;
                }

                let account = account.expect("Expected `account` to be `Some()`.");

                if account.locked {
                    tracing::warn!(
                        "Skipping: `TxType::Chargeback` account locked - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }

                assert!(result.tx > 0, "ERROR: Transaction index is 0");
                let (tx, state) = match account.actions.get_mut(&result.tx) {
                    Some(tx) => tx,
                    None => {
                        tracing::warn!(
                            "Skipping: `TxType::Chargeback` no transaction found - {} transaction: {}",
                            result.client,
                            result.tx
                        );
                        continue;
                    }
                };

                if !(*state).eq(&TxState::Disputed) {
                    tracing::warn!(
                        "Skipping: `TxType::Chargeback` transaction not disputed - {} transaction: {}",
                        result.client,
                        result.tx
                    );
                    continue;
                }
                assert!(tx.action.eq(&TxType::Deposit));

                let tx_amount = tx.amount.unwrap_or_default();

                *state = TxState::Changedback;
                account.held -= tx_amount;
                account.total -= tx_amount;
                account.locked = true;
            }
        }
    }

    println!("client,available,held,total,locked");

    for (id, account) in state.iter() {
        println!(
            "{},{},{},{},{}",
            id,
            amount_to_string(account.available),
            amount_to_string(account.held),
            amount_to_string(account.total),
            account.locked
        );
    }
}
