# Scallion

Distributed task queue

## Minimal example

```rust
#[derive(Debug, Serialize, Deserialize)]
struct Add {
    x: f64,
    y: f64,
}

#[async_trait]
impl Task for Add {
    type Output = f64;
    async fn task(self: Box<Self>, _: ConsumerState) -> Result<Self::Output, Box<dyn Error + Send + Sync + 'static>> {
        Ok(self.x + self.y)
    }
}
```

## Run example

Run consumer:

```bash
cargo run --release --features rkyv,protobuf --example app -- consume
```

Produce task:

```bash
cargo run --release --features rkyv,protobuf --example app -- produce ping
```
