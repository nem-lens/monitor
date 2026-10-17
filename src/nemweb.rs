//! NEMWeb access: listings and downloads. File name parsing and unzipping
//! live in the shared `nem-lens-aemo` crate.

use anyhow::Result;
pub use nem_lens_aemo::files::{daily_files, dispatch_file_interval, dispatch_files, unzip_csvs};

pub fn client(user_agent: &str) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(user_agent)
        .timeout(std::time::Duration::from_secs(60))
        .build()?)
}

pub async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client.get(url).send().await?.error_for_status()?;
    Ok(resp.text().await?)
}

pub async fn fetch_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let resp = client.get(url).send().await?.error_for_status()?;
    Ok(resp.bytes().await?.to_vec())
}
