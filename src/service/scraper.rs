use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Context, Error, Result};
use reqwest::{Client, Url};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct Scraper {
    config: Arc<ScraperConfig>,
    client: Client,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug)]
pub struct ScrapeData {
    pub data: Vec<String>,
    pub index: usize,
}

impl Scraper {
    pub fn new(config: ScraperConfig, client: Client) -> Self {
        let semaphore = Semaphore::new(config.download_threads);
        log::debug!(
            "initialized scraper with {} download threads",
            config.download_threads
        );
        Self {
            config: Arc::new(config),
            client,
            semaphore: Arc::new(semaphore),
        }
    }

    pub async fn scrape(&self, url: Url) -> Result<UnboundedReceiver<(usize, Result<ScrapeData>)>> {
        let host = url
            .host_str()
            .with_context(|| format!("host / domain not found in url: {}", url))?;

        let cfg = self
            .config
            .host
            .get(host)
            .with_context(|| format!("host configuration not found in config: {}", host))?
            .clone();

        cfg.validate()?;

        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn({
            let cfg = Arc::new(cfg);
            let this = self.clone();
            let chan = tx;
            let url = url.clone();
            async move {
                let index = 1;
                let uri = url.to_string();
                let act = async {
                    let chan = chan.clone();
                    let html = this.download_page(url).await?;
                    this.scrape_data(html, cfg, chan, index)?;
                    Ok::<(), Error>(())
                };
                if let Err(err) = act.await {
                    if chan.send((index, Err(err))).is_err() {
                        log::debug!(
                            "download channel is already closed. not sending more data from 1. {}",
                            uri
                        );
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn download_page(&self, url: Url) -> Result<Html> {
        let _x = self
            .semaphore
            .acquire()
            .await
            .context("failed to acquire download queue")?;

        log::debug!("downloading page: {url}");

        let uri = url.to_string();
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("failed to fetch webpage from: {uri}"))?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let status_name = resp.status().as_str().to_string();
            bail!("unexpected status code [{status} {status_name}] when fetching page from {uri}");
        }

        let body = resp
            .text()
            .await
            .with_context(|| format!("failed to read response body from: {uri}"))?;

        Ok(Html::parse_document(&body))
    }

    fn scrape_data(
        &self,
        webpage: Html,
        cfg: Arc<HostConfig>,
        chan: UnboundedSender<(usize, Result<ScrapeData>)>,
        index: usize,
    ) -> Result<()> {
        let next_page_selector = cfg.get_next_page_selector()?;
        if let Some(child) = webpage.select(&next_page_selector).next() {
            log::debug!(
                "next page selector {}:  found a child",
                cfg.next_page_selector
            );
            if let Some(url) = child.value().attr("href") {
                let index = index + 1;
                log::debug!("found next page url: {url}");
                match Url::try_from(url) {
                    Ok(url) => {
                        tokio::spawn({
                            let cfg = cfg.clone();
                            let this = self.clone();
                            let chan = chan.clone();
                            async move {
                                let act = async {
                                    let chan = chan.clone();
                                    let html = this.download_page(url).await?;
                                    this.scrape_data(html, cfg, chan, index)?;
                                    Ok::<(), Error>(())
                                };
                                if let Err(err) = act.await {
                                    chan.send((index, Err(err))).ok();
                                }
                            }
                        });
                    }
                    Err(err) => {
                        chan.send((index, Err(err.into()))).ok();
                    }
                }
            }
        }
        let selector = cfg.get_selector()?;
        let data = webpage.select(&selector).next().with_context(|| {
            format!("no html data found using given selector: {}", cfg.selector)
        })?;
        let v: Vec<String> = data
            .children()
            .map(|el| {
                el.value()
                    .as_text()
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "".to_string())
            })
            .collect();

        // Ensures the starting index is not out of bounds
        if v.len() < cfg.start_index {
            log::debug!(
                "no html data found from starting index of {}",
                cfg.start_index
            );
            chan.send((
                index,
                Ok(ScrapeData {
                    data: Vec::new(),
                    index,
                }),
            ))
            .ok();
            return Ok(());
        }

        // Ensures the ending index is not out of bounds
        let end_index = if cfg.end_index > v.len() {
            v.len()
        } else {
            cfg.end_index
        };

        let v = (&v[cfg.start_index..end_index]).to_vec();

        chan.send((index, Ok(ScrapeData { data: v, index })))?;

        Ok(())
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
