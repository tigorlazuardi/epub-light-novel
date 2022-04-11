use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use reqwest::{Client, Response, Url};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct Scraper {
    config: ScraperConfig,
    client: Client,
    semaphore: Semaphore,
}

impl Scraper {
    pub fn new(config: ScraperConfig, client: Client) -> Self {
        let semaphore = Semaphore::new(config.download_threads);
        log::debug!(
            "initialized scraper with {} download threads",
            config.download_threads
        );
        Self {
            config,
            client,
            semaphore,
        }
    }

    pub async fn scrape(&self, url: &str) -> Result<()> {
        let uri = Url::try_from(url).with_context(|| format!("invalid url: {}", url))?;

        let host = uri
            .host_str()
            .with_context(|| format!("host / domain not found in url: {}", uri))?;

        let cfg = self
            .config
            .host
            .get(host)
            .with_context(|| format!("host configuration not found in config: {}", host))?
            .clone();

        cfg.validate()?;

        Ok(())
    }

    async fn download_page(&self, url: &str) -> Result<Html> {
        let _x = self
            .semaphore
            .acquire()
            .await
            .context("failed to acquire download queue")?;

        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to fetch webpage from: {}", url))?;

        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            bail!(
                "unexpected status code {} when fetching page from {}",
                status,
                url
            );
        }

        let body = resp
            .text()
            .await
            .with_context(|| format!("failed to read body from: {}", url))?;

        Ok(Html::parse_document(&body))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScraperConfig {
    pub download_threads: usize,
    pub host: HashMap<String, HostConfig>,
}

impl Default for ScraperConfig {
    fn default() -> Self {
        let cpus = num_cpus::get();
        log::debug!("creating default config with {} download threads", cpus);
        Self {
            download_threads: cpus,
            host: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HostConfig {
    pub selector: String,
    pub next_page_selector: String,
    pub start_index: usize,
    pub end_index: usize,
}

impl HostConfig {
    /// Sanity check the config.
    /// selector should be valid css selector.
    /// next_page_selector should be valid css selector.
    /// start_index should not be the same as end_index and end_index should not be higher than start_index.
    pub fn validate(&self) -> Result<()> {
        self.get_selector()?;
        self.get_next_page_selector()?;
        if self.end_index <= self.start_index {
            bail!("end_index should not be lower than start_index");
        }
        Ok(())
    }

    pub fn get_selector(&self) -> Result<scraper::Selector> {
        match Selector::parse(&self.selector) {
            Ok(s) => Ok(s),
            Err(_) => bail!("failed to parse {} as selector", self.selector),
        }
    }

    pub fn get_next_page_selector(&self) -> Result<scraper::Selector> {
        match Selector::parse(&self.next_page_selector) {
            Ok(s) => Ok(s),
            Err(_) => {
                bail!(
                    "failed to parse {} as next page selector",
                    self.next_page_selector,
                )
            }
        }
    }
}
