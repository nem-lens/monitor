//! NEMWeb access: directory listings, downloads and unzipping.

use std::io::{Cursor, Read};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use regex::Regex;

fn dispatch_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"PUBLIC_DISPATCHIS_(\d{12})_(\d{16})\.zip").expect("valid regex"))
}

fn daily_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(PUBLIC_[A-Z_]+?)_(\d{8})_(\d{16})\.zip").expect("valid regex"))
}

/// DispatchIS filenames found anywhere in a listing (HTML or plain text), sorted and deduplicated.
pub fn dispatch_files(listing: &str) -> Vec<String> {
    let mut v: Vec<String> = dispatch_re()
        .find_iter(listing)
        .map(|m| m.as_str().to_string())
        .collect();
    v.sort();
    v.dedup();
    v
}

/// The interval-end time encoded in a DispatchIS filename.
pub fn dispatch_file_interval(name: &str) -> Option<NaiveDateTime> {
    let caps = dispatch_re().captures(name)?;
    NaiveDateTime::parse_from_str(&caps[1], "%Y%m%d%H%M").ok()
}

/// Daily files (Next_Day_*), returned as (filename, trading day).
pub fn daily_files(listing: &str) -> Vec<(String, NaiveDate)> {
    let mut v: Vec<(String, NaiveDate)> = daily_re()
        .captures_iter(listing)
        .filter_map(|c| {
            let d = NaiveDate::parse_from_str(&c[2], "%Y%m%d").ok()?;
            Some((c[0].to_string(), d))
        })
        .collect();
    v.sort();
    v.dedup();
    v
}

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

/// All CSV members of a zip, recursing one level into nested zips.
pub fn unzip_csvs(bytes: &[u8]) -> Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    unzip_into(bytes, &mut out, 0)?;
    Ok(out)
}

fn unzip_into(bytes: &[u8], out: &mut Vec<(String, Vec<u8>)>, depth: u8) -> Result<()> {
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).context("not a zip archive")?;
    for i in 0..zip.len() {
        let mut f = zip.by_index(i)?;
        let name = f.name().to_string();
        // Guard against decompression bombs: DispatchIS members are well under 10 MB.
        anyhow::ensure!(f.size() < 200 * 1024 * 1024, "zip member {name} too large");
        let mut buf = Vec::with_capacity(f.size() as usize);
        f.read_to_end(&mut buf)?;
        let lower = name.to_ascii_lowercase();
        if lower.ends_with(".csv") {
            out.push((name, buf));
        } else if lower.ends_with(".zip") && depth == 0 {
            unzip_into(&buf, out, depth + 1)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_files_in_html_and_text_listings() {
        let html = r#"<A HREF="/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_202610170955_0000000543328677.zip">PUBLIC_DISPATCHIS_202610170955_0000000543328677.zip</A><br>
        Saturday, October 17, 2026 09:46 AM 21391 PUBLIC_DISPATCHIS_202610170950_0000000543327927.zip"#;
        let files = dispatch_files(html);
        assert_eq!(files.len(), 2);
        assert_eq!(
            files[0],
            "PUBLIC_DISPATCHIS_202610170950_0000000543327927.zip"
        );
        assert_eq!(
            dispatch_file_interval(&files[1]).unwrap().to_string(),
            "2026-10-17 09:55:00"
        );
    }

    #[test]
    fn finds_daily_files() {
        let text = "PUBLIC_NEXT_DAY_DISPATCH_20261012_0000000542594175.zip PUBLIC_NEXT_DAY_OFFER_ENERGY_20261012_0000000542594100.zip";
        let files = daily_files(text);
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|(_, d)| d.to_string() == "2026-10-12"));
    }
}
