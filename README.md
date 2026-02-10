# Temporal-DB

**Event-sourced temporal database in Rust**

Temporal-DB records every change as an immutable event and lets you query data *as it was* at any point in time.

It is useful for audit trails, debugging production behaviour, historical analytics, and experimenting with temporal data models.

## Installation

```bash
git clone https://github.com/Julzz10110/temporal-db.git 
cd temporal-db
cargo build
```

## Quick example

```rust
use temporal_db::prelude::*;

#[tokio::main]
async fn main() -> temporal_db::error::Result<()> {
    let db = TemporalDB::in_memory()?;

    let ts = Timestamp::now();
    db.insert("user:1", "active", ts).await?;

    let value: Option<String> = db.query_as_of("user:1", ts).await?;
    println!("Value at {:?}: {:?}", ts, value);

    Ok(())
}
```