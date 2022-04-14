pub mod command;
pub mod service;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    command::execute().await
}
