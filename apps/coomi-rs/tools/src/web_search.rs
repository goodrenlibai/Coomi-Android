//! Keyless multi-engine web search with automatic failover.
//!
//! Design follows the best keyless implementations used by agent runtimes in
//! 2025/2026 (duckduckgo-search/`ddgs`, Hermes Agent, OpenClaw): chain several
//! engines behind one uniform result shape and stop at the first engine that
//! yields usable hits. No API keys are required anywhere:
//!
//! 1. `searxng`     – user-hosted SearXNG instance (`COOMI_SEARXNG_URL`), the
//!    recommended power-user setup: no key, no quota, aggregates 70+ engines.
//! 2. `bing_rss`    – Bing's RSS endpoint (`format=rss`): stable structured XML
//!    with snippets, the most reliable keyless structured endpoint.
//! 3. `bing_html`   – classic Bing SERP parser (title + snippet) for when RSS
//!    is throttled or returns an empty channel.
//! 4. `baidu`       – mainland-China reachable; result links are resolved from
//!    `baidu.com/link?url=` redirect stubs to their real targets.
//! 5. `sogou` – second mainland-China engine.
//! 6. `sogou_weixin` – Sogou's WeChat official-account vertical; surfaces
//!    school/merchant/official announcements generic SERPs miss.
//! 7. `duckduckgo`  – keyless western engine (html + lite endpoints); blocked
//!    inside mainland China but valuable everywhere else.
//! 8. `mojeek`      – lightweight independent index, tolerant markup.
//! 9. `wikipedia`   – near-zero block rate final fallback so the agent almost
//!    always comes back with *something* citable.
//!
//! The default chain is query-language aware: Chinese-capable engines lead
//! for CJK queries, western keyless engines lead otherwise.
//!
//! `COOMI_SEARCH_ENGINE` (comma-separated) overrides the chain, e.g.
//! `COOMI_SEARCH_ENGINE=baidu,bing_rss`.

use coomi_engine::ToolResult;
use regex::Regex;
use serde_json::Value;
use std::time::Duration;

use crate::collapse_whitespace;
use crate::decode_html;
use crate::string_arg;
use crate::usize_arg;

/// Desktop-Chrome UA: engines serve their classic, parser-friendly SERP markup
/// to it far more reliably than to mobile or bot-looking agents.
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

/// Android-phone UA for mobile-first engines (Sogou Weixin): they are built
/// for phones and their anti-spider is noticeably more lenient toward mobile
/// browsers.
const MOBILE_USER_AGENT: &str = "Mozilla/5.0 (Linux; Android 14; 2407FPN8EG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36";

/// Maximum bytes read from any remote body (web_search / fetch) to avoid OOM on Android.
const MAX_BODY_BYTES: usize = 512 * 1024;

/// Read a response body capped at [`MAX_BODY_BYTES`], lossy-decoded to UTF-8.
pub(crate) async fn read_body_capped(mut response: reqwest::Response) -> Result<String, String> {
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = MAX_BODY_BYTES - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if body.len() >= MAX_BODY_BYTES {
                    break;
                }
            }
            Ok(None) => break,
            Err(error) => return Err(format!("response read failed: {error}")),
        }
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// One ranked search hit, normalized across engines.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchHit {
    title: String,
    url: String,
    snippet: String,
}

impl SearchHit {
    fn new(title: impl AsRef<str>, url: impl AsRef<str>, snippet: impl AsRef<str>) -> Option<Self> {
        let title = collapse_whitespace(title.as_ref());
        let url = url.as_ref().trim().to_string();
        let snippet = collapse_whitespace(snippet.as_ref());
        let title = title.chars().take(200).collect::<String>();
        let snippet = snippet.chars().take(300).collect::<String>();
        if title.is_empty() || !(url.starts_with("http://") || url.starts_with("https://")) {
            return None;
        }
        Some(Self {
            title,
            url,
            snippet,
        })
    }
}

/// Entry point for the built-in `web_search` tool.
pub(crate) async fn search(arguments: &Value) -> ToolResult {
    let Some(query) = string_arg(arguments, "query") else {
        return ToolResult::error("missing string argument: query");
    };
    let query = query.trim();
    if query.is_empty() {
        return ToolResult::error("missing string argument: query");
    }
    let limit = usize_arg(arguments, "limit").unwrap_or(5).clamp(1, 10);

    let client = match reqwest::Client::builder()
        .cookie_store(true)
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent(USER_AGENT)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return web_search_unavailable(format!("HTTP client initialization failed: {error}"));
        }
    };

    let engines = engine_chain(query);
    let mut failures = Vec::new();
    for engine in engines {
        let outcome = match engine {
            Engine::Searxng => search_searxng(&client, query, limit).await,
            Engine::BingRss => search_bing_rss(&client, query, limit).await,
            Engine::BingHtml => search_bing_html(&client, query, limit).await,
            Engine::Baidu => search_baidu(&client, query, limit).await,
            Engine::Sogou => search_sogou(&client, query, limit).await,
            Engine::SogouWeixin => search_sogou_weixin(&client, query, limit).await,
            Engine::DuckDuckGo => search_duckduckgo(&client, query, limit).await,
            Engine::Mojeek => search_mojeek(&client, query, limit).await,
            Engine::Wikipedia => search_wikipedia(&client, query, limit).await,
        };
        match outcome {
            Ok(hits) => {
                if hits.is_empty() {
                    failures.push(format!("{}: no usable results", engine.label()));
                    continue;
                }
                // Quality gate: engines occasionally answer with generic popular
                // pages on cache misses (Bing RSS does this for CJK queries).
                // Treat zero lexical overlap with the query as a false success
                // and keep failing over to the next engine.
                if !results_are_relevant(query, &hits) {
                    failures.push(format!(
                        "{}: results not relevant to the query (possible engine cache miss)",
                        engine.label()
                    ));
                    continue;
                }
                let hits = finalize_hits(hits, limit);
                if !hits.is_empty() {
                    return ToolResult::success(render_hits(&hits, engine.label()));
                }
                failures.push(format!("{}: no usable results", engine.label()));
            }
            Err(reason) => failures.push(format!("{}: {reason}", engine.label())),
        }
    }
    web_search_unavailable(failures.join("; "))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Searxng,
    BingRss,
    BingHtml,
    Baidu,
    Sogou,
    SogouWeixin,
    DuckDuckGo,
    Mojeek,
    Wikipedia,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Engine::Searxng => "searxng",
            Engine::BingRss => "bing_rss",
            Engine::BingHtml => "bing_html",
            Engine::Baidu => "baidu",
            Engine::Sogou => "sogou",
            Engine::SogouWeixin => "sogou_weixin",
            Engine::DuckDuckGo => "duckduckgo",
            Engine::Mojeek => "mojeek",
            Engine::Wikipedia => "wikipedia",
        }
    }
}

/// Resolve the engine chain from the process environment for `query`.
fn engine_chain(query: &str) -> Vec<Engine> {
    engine_chain_from(
        std::env::var("COOMI_SEARCH_ENGINE").ok().as_deref(),
        std::env::var("COOMI_SEARXNG_URL").ok().as_deref(),
        has_cjk(query),
    )
}

/// Pure engine-chain resolution: `engine_override` wins when it parses to at
/// least one known engine; otherwise the query-language-aware default order is
/// used, with SearXNG leading whenever a `searxng_url` is configured.
fn engine_chain_from(
    engine_override: Option<&str>,
    searxng_url: Option<&str>,
    cjk_query: bool,
) -> Vec<Engine> {
    if let Some(override_list) = engine_override {
        let chain = override_list
            .split(',')
            .filter_map(|name| match name.trim().to_ascii_lowercase().as_str() {
                "searxng" => Some(Engine::Searxng),
                "bing_rss" | "bing-rss" | "rss" => Some(Engine::BingRss),
                "bing" | "bing_html" | "bing-html" => Some(Engine::BingHtml),
                "baidu" => Some(Engine::Baidu),
                "sogou" => Some(Engine::Sogou),
                "sogou_weixin" | "weixin" | "wechat" => Some(Engine::SogouWeixin),
                "duckduckgo" | "ddg" => Some(Engine::DuckDuckGo),
                "mojeek" => Some(Engine::Mojeek),
                "wikipedia" | "wiki" => Some(Engine::Wikipedia),
                _ => None,
            })
            .collect::<Vec<_>>();
        if !chain.is_empty() {
            return chain;
        }
    }
    // Chinese-first engines lead for CJK queries: Bing's keyless endpoints
    // degrade to generic popular pages on CJK cache misses, while Sogou answers
    // Chinese long-tail queries well even from datacenter addresses. The Sogou
    // WeChat vertical surfaces school / merchant / official-account
    // announcements that generic web SERPs often miss. (Shenma/m.sm.cn is
    // deliberately excluded: its Alibaba WAF rejects non-JS TLS fingerprints.)
    let mut chain = if cjk_query {
        vec![
            Engine::BingRss,
            Engine::Sogou,
            Engine::Baidu,
            Engine::SogouWeixin,
            Engine::BingHtml,
            Engine::DuckDuckGo,
            Engine::Mojeek,
            Engine::Wikipedia,
        ]
    } else {
        vec![
            Engine::BingRss,
            Engine::BingHtml,
            Engine::DuckDuckGo,
            Engine::Mojeek,
            Engine::Sogou,
            Engine::Baidu,
            Engine::Wikipedia,
        ]
    };
    if searxng_url
        .map(|url| !url.trim().is_empty())
        .unwrap_or(false)
    {
        chain.insert(0, Engine::Searxng);
    }
    chain
}

/// True when the query contains CJK characters — used to pick the Bing market.
fn has_cjk(text: &str) -> bool {
    text.chars().any(|c| {
        ('\u{4e00}'..='\u{9fff}').contains(&c)
            || ('\u{3400}'..='\u{4dbf}').contains(&c)
            || ('\u{f900}'..='\u{faff}').contains(&c)
    })
}

fn accept_headers(builder: reqwest::RequestBuilder, accept: &str) -> reqwest::RequestBuilder {
    builder
        .header("Accept", accept)
        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.7")
}

// ---------------------------------------------------------------------------
// SearXNG (self-hosted, JSON API)
// ---------------------------------------------------------------------------

async fn search_searxng(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let base = std::env::var("COOMI_SEARXNG_URL")
        .map_err(|_| "COOMI_SEARXNG_URL is not set".to_string())?;
    let base = base.trim().trim_end_matches('/');
    if base.is_empty() {
        return Err("COOMI_SEARXNG_URL is empty".to_string());
    }
    let url = format!("{base}/search");
    let response = accept_headers(client.get(&url), "application/json")
        .query(&[("q", query), ("format", "json"), ("safesearch", "0")])
        .send()
        .await
        .map_err(|error| format!("{error}"))?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = read_body_capped(response).await?;
    parse_searxng_json(&body, limit)
}

fn parse_searxng_json(body: &str, limit: usize) -> Result<Vec<SearchHit>, String> {
    let value: Value =
        serde_json::from_str(body).map_err(|error| format!("invalid JSON: {error}"))?;
    let mut hits = Vec::new();
    if let Some(results) = value.get("results").and_then(Value::as_array) {
        for result in results.iter().take(limit) {
            let title = result.get("title").and_then(Value::as_str).unwrap_or("");
            let url = result.get("url").and_then(Value::as_str).unwrap_or("");
            let snippet = result.get("content").and_then(Value::as_str).unwrap_or("");
            if let Some(hit) = SearchHit::new(title, url, snippet) {
                hits.push(hit);
            }
        }
    }
    Ok(hits)
}

// ---------------------------------------------------------------------------
// Bing RSS (`format=rss`)
// ---------------------------------------------------------------------------

async fn search_bing_rss(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let mkt = if has_cjk(query) { "zh-CN" } else { "en-US" };
    let response = accept_headers(
        client.get("https://www.bing.com/search"),
        "application/rss+xml, application/xml, text/xml",
    )
    .query(&[("format", "rss"), ("q", query), ("mkt", mkt)])
    .send()
    .await
    .map_err(|error| format!("{error}"))?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = read_body_capped(response).await?;
    Ok(parse_bing_rss(&body, limit))
}

/// Parse Bing's RSS search results (`format=rss`): title, link and a short snippet per item.
fn parse_bing_rss(body: &str, limit: usize) -> Vec<SearchHit> {
    let item_re = Regex::new(r"(?is)<item>(.*?)</item>").expect("valid RSS item regex");
    let title_re = Regex::new(r"(?is)<title>(.*?)</title>").expect("valid RSS title regex");
    let link_re = Regex::new(r"(?is)<link>(.*?)</link>").expect("valid RSS link regex");
    let desc_re =
        Regex::new(r"(?is)<description>(.*?)</description>").expect("valid RSS description regex");
    let cdata_re = Regex::new(r"(?is)<!\[CDATA\[(.*?)\]\]>").expect("valid CDATA regex");
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let mut hits = Vec::new();
    for item in item_re.captures_iter(body).take(limit) {
        let block = &item[1];
        let pick = |re: &Regex| {
            re.captures(block)
                .map_or("", |m| m.get(1).map_or("", |v| v.as_str()))
        };
        let title = strip_cdata_and_tags(&tag_re, &cdata_re, pick(&title_re));
        let url = normalize_search_url(pick(&link_re));
        let snippet = strip_cdata_and_tags(&tag_re, &cdata_re, pick(&desc_re));
        if let Some(hit) = SearchHit::new(title, url, snippet) {
            hits.push(hit);
        }
    }
    hits
}

fn strip_cdata_and_tags(tag_re: &Regex, cdata_re: &Regex, value: &str) -> String {
    let value = value.trim();
    let value = if let Some(captures) = cdata_re.captures(value) {
        captures.get(1).map_or("", |v| v.as_str())
    } else {
        value
    };
    let value = tag_re.replace_all(value, "");
    decode_html(&value).trim().to_string()
}

// ---------------------------------------------------------------------------
// Bing HTML SERP
// ---------------------------------------------------------------------------

async fn search_bing_html(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let mkt = if has_cjk(query) { "zh-CN" } else { "en-US" };
    let response = accept_headers(
        client.get("https://www.bing.com/search"),
        "text/html,application/xhtml+xml",
    )
    .query(&[
        ("q", query),
        ("mkt", mkt),
        ("count", "20"),
        ("setlang", "zh-hans"),
    ])
    .send()
    .await
    .map_err(|error| format!("{error}"))?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = read_body_capped(response).await?;
    Ok(parse_bing_html(&body, limit))
}

/// Parse the Bing SERP. Bing A/B-tests two organic layouts (both observed in
/// August 2026 depending on request parameters), so both title forms are
/// supported and merged by document position:
///   A) `<a href=…><h2>title</h2></a>` (anchor wraps the heading)
///   B) `<h2><a href=…>title</a></h2>` (heading wraps the anchor)
/// Abstracts live in `<div class="b_caption"><p …>`; ad blocks (`b_ad`) carry
/// different titles and never match these patterns.
fn parse_bing_html(body: &str, limit: usize) -> Vec<SearchHit> {
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let wrap_re =
        Regex::new(r#"(?is)<a[^>]+href=['"](https?://[^'"]+)['"][^>]*>\s*<h2[^>]*>(.*?)</h2>"#)
            .expect("valid Bing wrap regex");
    let inner_re =
        Regex::new(r#"(?is)<h2[^>]*>\s*<a[^>]+href=['"](https?://[^'"]+)['"][^>]*>(.*?)</a>"#)
            .expect("valid Bing inner regex");
    let caption_re = Regex::new(
        r#"(?is)<div[^>]+class=['"][^'"]*b_caption[^'"]*['"][^>]*>.*?<p[^>]*>(.*?)</p>"#,
    )
    .expect("valid Bing caption regex");
    // Collect (start, end, url, title) from both layouts, then sort by position.
    let mut found: Vec<(usize, usize, String, String)> = Vec::new();
    for captures in wrap_re.captures_iter(body) {
        let whole = captures.get(0).map_or((0, 0), |m| (m.start(), m.end()));
        let url = normalize_search_url(captures.get(1).map_or("", |v| v.as_str()));
        let title =
            decode_html(&tag_re.replace_all(captures.get(2).map_or("", |v| v.as_str()), ""));
        found.push((whole.0, whole.1, url, title));
    }
    for captures in inner_re.captures_iter(body) {
        let whole = captures.get(0).map_or((0, 0), |m| (m.start(), m.end()));
        // Skip anchors already claimed by the wrap layout (same h2).
        if found
            .iter()
            .any(|&(s, e, _, _)| whole.0 >= s && whole.0 < e)
        {
            continue;
        }
        let url = normalize_search_url(captures.get(1).map_or("", |v| v.as_str()));
        let title =
            decode_html(&tag_re.replace_all(captures.get(2).map_or("", |v| v.as_str()), ""));
        found.push((whole.0, whole.1, url, title));
    }
    found.sort_by_key(|&(start, _, _, _)| start);
    let mut hits = Vec::new();
    for (_, end, url, title) in found {
        if hits.len() >= limit {
            break;
        }
        // Snippet: the b_caption <p> following this title (bounded lookahead).
        let window_end = (end + 3000).min(body.len());
        let window = &body[end..window_end];
        let snippet = caption_re
            .captures(window)
            .map(|m| decode_html(&tag_re.replace_all(&m[1], "")))
            .unwrap_or_default();
        if let Some(hit) = SearchHit::new(title, url, snippet) {
            hits.push(hit);
        }
    }
    hits
}

// ---------------------------------------------------------------------------
// Baidu
// ---------------------------------------------------------------------------

async fn search_baidu(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    // Baidu's anti-spider expects a BAIDUID session cookie; prime the jar first.
    let _ = accept_headers(
        client.get("https://www.baidu.com/"),
        "text/html,application/xhtml+xml",
    )
    .timeout(Duration::from_secs(6))
    .send()
    .await;
    let response = accept_headers(
        client.get("https://www.baidu.com/s"),
        "text/html,application/xhtml+xml",
    )
    .query(&[("wd", query), ("rn", "20"), ("ie", "utf-8")])
    .header("Referer", "https://www.baidu.com/")
    .send()
    .await
    .map_err(|error| format!("{error}"))?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = read_body_capped(response).await?;
    if body.contains("wappass.baidu.com") || body.contains("百度安全验证") {
        return Err("blocked by Baidu anti-spider verification".to_string());
    }
    let mut hits = parse_baidu_html(&body, limit);
    resolve_baidu_redirects(client, &mut hits).await;
    Ok(hits)
}

/// Parse Baidu's SERP: organic results are `<h3 class="t|c-title"><a href=…>`;
/// the abstract lives in a following `c-abstract`/similar container. Blocks
/// flagged as advertisements (广告 badge) are skipped.
fn parse_baidu_html(body: &str, limit: usize) -> Vec<SearchHit> {
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let title_re = Regex::new(
        r#"(?is)<h3[^>]*>\s*<a[^>]+href=['"](https?://[^'"]+)['"][^>]*>(.*?)</a>\s*</h3>"#,
    )
    .expect("valid Baidu title regex");
    let snippet_re = Regex::new(
        r#"(?is)<div[^>]+class=['"][^'"]*(?:c-abstract|c-span-last|cosc-source-text)[^'"]*['"][^>]*>(.*?)</div>"#,
    )
    .expect("valid Baidu snippet regex");
    let mut hits = Vec::new();
    let mut positions = Vec::new();
    for captures in title_re.captures_iter(body) {
        if let Some(m) = captures.get(0) {
            positions.push(m.start());
        }
    }
    for (index, captures) in title_re.captures_iter(body).enumerate() {
        if hits.len() >= limit {
            break;
        }
        let whole = captures.get(0).map_or((0, 0), |m| (m.start(), m.end()));
        let window_end = positions
            .get(index + 1)
            .copied()
            .unwrap_or_else(|| (whole.1 + 6000).min(body.len()))
            .min(whole.1 + 6000)
            .min(body.len());
        if window_end <= whole.1 {
            continue;
        }
        let window = &body[whole.1..window_end];
        // Skip ad blocks (百度广告徽标 / 广告 marker).
        if window.contains(">广告</span>") || window.contains("data-adburst") {
            continue;
        }
        let url = normalize_search_url(captures.get(1).map_or("", |v| v.as_str()));
        let title =
            decode_html(&tag_re.replace_all(captures.get(2).map_or("", |v| v.as_str()), ""));
        let snippet = snippet_re
            .captures(window)
            .map(|m| decode_html(&tag_re.replace_all(&m[1], "")))
            .unwrap_or_default();
        if let Some(hit) = SearchHit::new(title, url, snippet) {
            hits.push(hit);
        }
    }
    hits
}

/// Replace `baidu.com/link?url=…` stubs with their real targets by reading the
/// `Location` header of a redirect-handshake request (no body download).
async fn resolve_baidu_redirects(_client: &reqwest::Client, hits: &mut [SearchHit]) {
    // Redirect policy is client-scoped in reqwest, so resolution needs its own
    // no-redirect client to observe the 302 Location header.
    let resolver = match reqwest::Client::builder()
        .cookie_store(true)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(6))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(USER_AGENT)
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    for hit in hits.iter_mut() {
        if !hit.url.contains("baidu.com/link?") {
            continue;
        }
        let resolved = resolver
            .get(&hit.url)
            .send()
            .await
            .ok()
            .and_then(|response| {
                response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
            });
        if let Some(target) = resolved
            .filter(|target| target.starts_with("http://") || target.starts_with("https://"))
        {
            hit.url = target;
        }
    }
}

// ---------------------------------------------------------------------------
// Sogou
// ---------------------------------------------------------------------------

async fn search_sogou(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let response = accept_headers(
        client.get("https://www.sogou.com/web"),
        "text/html,application/xhtml+xml",
    )
    .query(&[("query", query), ("ie", "utf8")])
    .header("Referer", "https://www.sogou.com/")
    .send()
    .await
    .map_err(|error| format!("{error}"))?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = read_body_capped(response).await?;
    if body.contains("antispider") {
        return Err("blocked by Sogou anti-spider verification".to_string());
    }
    Ok(parse_sogou_html(&body, limit))
}

/// Parse Sogou's SERP: organic results are `<h3 class="vr-title|vrTitle"><a>`;
/// snippets live in `str-text` / `fz-mid` / `space-txt` containers.
fn parse_sogou_html(body: &str, limit: usize) -> Vec<SearchHit> {
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let title_re =
        Regex::new(r#"(?is)<h3[^>]*>\s*<a[^>]+href=['"]([^'"]+)['"][^>]*>(.*?)</a>\s*</h3>"#)
            .expect("valid Sogou title regex");
    let snippet_re = Regex::new(
        r#"(?is)<(?:div|p)[^>]+class=['"][^'"]*(?:str-text-info|space-txt|fz-mid|text-layout)[^'"]*['"][^>]*>(.*?)</(?:div|p)>"#,
    )
    .expect("valid Sogou snippet regex");
    let mut hits = Vec::new();
    let mut positions: Vec<usize> = title_re
        .captures_iter(body)
        .filter_map(|c| c.get(0).map(|m| m.start()))
        .collect();
    positions.push(body.len());
    for (index, captures) in title_re.captures_iter(body).enumerate() {
        if hits.len() >= limit {
            break;
        }
        let whole = captures.get(0).map_or((0, 0), |m| (m.start(), m.end()));
        let window_end = positions
            .get(index + 1)
            .copied()
            .unwrap_or(body.len())
            .min(whole.1 + 5000)
            .min(body.len());
        if window_end <= whole.1 {
            continue;
        }
        let window = &body[whole.1..window_end];
        if window.contains(">广告<") {
            continue;
        }
        let raw_url = captures.get(1).map_or("", |v| v.as_str());
        let url = if raw_url.starts_with('/') {
            format!("https://www.sogou.com{raw_url}")
        } else {
            normalize_search_url(raw_url)
        };
        let title =
            decode_html(&tag_re.replace_all(captures.get(2).map_or("", |v| v.as_str()), ""));
        let snippet = snippet_re
            .captures(window)
            .map(|m| decode_html(&tag_re.replace_all(&m[1], "")))
            .unwrap_or_default();
        if let Some(hit) = SearchHit::new(title, url, snippet) {
            hits.push(hit);
        }
    }
    hits
}

// ---------------------------------------------------------------------------
// Sogou Weixin (WeChat official-account article vertical)
// ---------------------------------------------------------------------------

async fn search_sogou_weixin(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let response = accept_headers(
        client.get("https://weixin.sogou.com/weixin"),
        "text/html,application/xhtml+xml",
    )
    .query(&[("type", "2"), ("query", query), ("ie", "utf8")])
    .header("Referer", "https://weixin.sogou.com/")
    .header("User-Agent", MOBILE_USER_AGENT)
    .send()
    .await
    .map_err(|error| format!("{error}"))?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = read_body_capped(response).await?;
    if body.contains("antispider") {
        return Err("blocked by Sogou anti-spider verification".to_string());
    }
    Ok(parse_sogou_weixin_html(&body, limit))
}

/// Parse the Sogou Weixin SERP: `<li id="sogou_vr…box_N">` items holding an
/// `<h3><a href"` title plus a `<p class="txt-info">` abstract. Links are
/// Sogou redirect stubs (`/link?url=…`), absolutized for the fetch tool.
fn parse_sogou_weixin_html(body: &str, limit: usize) -> Vec<SearchHit> {
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let item_re = Regex::new(r#"(?is)<li[^>]+id=['"]sogou_vr[^'"]*['"][^>]*>(.*?)</li>"#)
        .expect("valid wx item regex");
    let title_re = Regex::new(r#"(?is)<h3[^>]*>\s*<a[^>]+href=['"]([^'"]+)['"][^>]*>(.*?)</a>"#)
        .expect("valid wx title regex");
    let snippet_re = Regex::new(r#"(?is)<p[^>]+class=['"][^'"]*txt-info[^'"]*['"][^>]*>(.*?)</p>"#)
        .expect("valid wx snippet regex");
    let mut hits = Vec::new();
    for item in item_re.captures_iter(body) {
        if hits.len() >= limit {
            break;
        }
        let block = &item[1];
        let Some(captures) = title_re.captures(block) else {
            continue;
        };
        let raw_url = decode_html(captures.get(1).map_or("", |v| v.as_str()));
        let url = if raw_url.starts_with('/') {
            format!("https://weixin.sogou.com{raw_url}")
        } else if raw_url.starts_with("http") {
            raw_url
        } else {
            continue;
        };
        let title =
            decode_html(&tag_re.replace_all(captures.get(2).map_or("", |v| v.as_str()), ""));
        let snippet = snippet_re
            .captures(block)
            .map(|m| decode_html(&tag_re.replace_all(&m[1], "")))
            .unwrap_or_default();
        if let Some(hit) = SearchHit::new(title, url, snippet) {
            hits.push(hit);
        }
    }
    hits
}

// ---------------------------------------------------------------------------
// DuckDuckGo (html + lite endpoints)
// ---------------------------------------------------------------------------

async fn search_duckduckgo(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let mut last_error = String::new();
    for endpoint in [
        "https://html.duckduckgo.com/html/",
        "https://lite.duckduckgo.com/lite/",
    ] {
        let response = accept_headers(
            client
                .post(endpoint)
                .header("Referer", "https://duckduckgo.com/")
                .form(&[("q", query), ("kl", "cn-zh")]),
            "text/html,application/xhtml+xml",
        )
        .send()
        .await;
        match response {
            Ok(response) if response.status().is_success() => {
                let body = read_body_capped(response).await?;
                let hits = parse_duckduckgo_html(&body, limit);
                if !hits.is_empty() {
                    return Ok(hits);
                }
                last_error = format!("{endpoint}: no parseable results");
            }
            Ok(response) => {
                last_error = format!("{endpoint}: HTTP {}", response.status());
            }
            Err(error) => {
                last_error = format!("{endpoint}: {error}");
            }
        }
    }
    Err(if last_error.is_empty() {
        "no endpoint reached".to_string()
    } else {
        last_error
    })
}

/// Parse both DuckDuckGo classic (`result__a`) and lite (`result-link`) markup.
fn parse_duckduckgo_html(body: &str, limit: usize) -> Vec<SearchHit> {
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let anchor_re = Regex::new(
        r#"(?is)<a[^>]+class=['"][^'"]*(?:result__a|result-link)[^'"]*['"][^>]+href=['"]([^'"]+)['"][^>]*>(.*?)</a>"#,
    )
    .expect("valid DDG anchor regex");
    let anchor_alt_re = Regex::new(
        r#"(?is)<a[^>]+href=['"]([^'"]+)['"][^>]+class=['"][^'"]*(?:result__a|result-link)[^'"]*['"][^>]*>(.*?)</a>"#,
    )
    .expect("valid DDG anchor alt regex");
    let snippet_re = Regex::new(
        r#"(?is)<(?:a|td|div)[^>]+class=['"][^'"]*(?:result__snippet|result-snippet)[^'"]*['"][^>]*>(.*?)</(?:a|td|div)>"#,
    )
    .expect("valid DDG snippet regex");
    let mut hits = Vec::new();
    let mut seen_starts = Vec::new();
    for re in [&anchor_re, &anchor_alt_re] {
        for captures in re.captures_iter(body) {
            if hits.len() >= limit {
                break;
            }
            let start = captures.get(0).map_or(0, |m| m.start());
            if seen_starts.contains(&start) {
                continue;
            }
            seen_starts.push(start);
            let raw_url = captures.get(1).map_or("", |v| v.as_str());
            let url = normalize_search_url(raw_url);
            let title =
                decode_html(&tag_re.replace_all(captures.get(2).map_or("", |v| v.as_str()), ""));
            let end = captures.get(0).map_or(body.len(), |m| m.end());
            let window = &body[end..(end + 2500).min(body.len())];
            let snippet = snippet_re
                .captures(window)
                .map(|m| decode_html(&tag_re.replace_all(&m[1], "")))
                .unwrap_or_default();
            if let Some(hit) = SearchHit::new(title, url, snippet) {
                hits.push(hit);
            }
        }
        if !hits.is_empty() {
            break;
        }
    }
    hits
}

// ---------------------------------------------------------------------------
// Mojeek
// ---------------------------------------------------------------------------

async fn search_mojeek(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let response = accept_headers(
        client.get("https://www.mojeek.com/search"),
        "text/html,application/xhtml+xml",
    )
    .query(&[("q", query)])
    .send()
    .await
    .map_err(|error| format!("{error}"))?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = read_body_capped(response).await?;
    Ok(parse_mojeek_html(&body, limit))
}

/// Parse Mojeek's SERP: `<li><h2><a class="title" href>…</a></h2><p class="s">…`.
fn parse_mojeek_html(body: &str, limit: usize) -> Vec<SearchHit> {
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let title_re = Regex::new(
        r#"(?is)<h2[^>]*>\s*<a[^>]+href=['"](https?://[^'"]+)['"][^>]*>(.*?)</a>\s*</h2>"#,
    )
    .expect("valid Mojeek title regex");
    let snippet_re = Regex::new(r#"(?is)<p[^>]+class=['"][^'"]*\bs\b[^'"]*['"][^>]*>(.*?)</p>"#)
        .expect("valid Mojeek snippet regex");
    let mut hits = Vec::new();
    for captures in title_re.captures_iter(body).take(limit) {
        let url = normalize_search_url(captures.get(1).map_or("", |v| v.as_str()));
        let title =
            decode_html(&tag_re.replace_all(captures.get(2).map_or("", |v| v.as_str()), ""));
        let end = captures.get(0).map_or(body.len(), |m| m.end());
        let window = &body[end..(end + 2500).min(body.len())];
        let snippet = snippet_re
            .captures(window)
            .map(|m| decode_html(&tag_re.replace_all(&m[1], "")))
            .unwrap_or_default();
        if let Some(hit) = SearchHit::new(title, url, snippet) {
            hits.push(hit);
        }
    }
    hits
}

// ---------------------------------------------------------------------------
// Wikipedia (opensearch JSON — final fallback)
// ---------------------------------------------------------------------------

async fn search_wikipedia(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, String> {
    let hosts: &[&str] = if has_cjk(query) {
        &["https://zh.wikipedia.org", "https://en.wikipedia.org"]
    } else {
        &["https://en.wikipedia.org", "https://zh.wikipedia.org"]
    };
    let mut last_error = String::new();
    for host in hosts {
        let url = format!("{host}/w/api.php");
        // Full-text `list=search` handles arbitrary multi-term queries better
        // than prefix-based `opensearch`; try it first, opensearch second.
        let full_text: [(&str, &str); 5] = [
            ("action", "query"),
            ("list", "search"),
            ("srsearch", query),
            ("srlimit", "10"),
            ("format", "json"),
        ];
        let open_search: [(&str, &str); 5] = [
            ("action", "opensearch"),
            ("search", query),
            ("limit", "10"),
            ("namespace", "0"),
            ("format", "json"),
        ];
        for (index, params) in [&full_text, &open_search].into_iter().enumerate() {
            let response = client
                .get(&url)
                .query(params)
                .header("Accept", "application/json")
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    let body = read_body_capped(response).await?;
                    let hits = if index == 0 {
                        parse_wikipedia_search(&body, host, limit)
                    } else {
                        parse_wikipedia_opensearch(&body, limit)
                    };
                    if !hits.is_empty() {
                        return Ok(hits);
                    }
                    last_error = format!("{host}: no matching articles");
                }
                Ok(response) => {
                    last_error = format!("{host}: HTTP {}", response.status());
                    break;
                }
                Err(error) => {
                    last_error = format!("{host}: {error}");
                    break;
                }
            }
        }
    }
    Err(last_error)
}

/// Parse `action=query&list=search` JSON; article URLs are synthesized from
/// the title because search entries carry no URL field.
fn parse_wikipedia_search(body: &str, host: &str, limit: usize) -> Vec<SearchHit> {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    let tag_re = Regex::new(r"<[^>]+>").expect("valid HTML tag regex");
    let mut hits = Vec::new();
    if let Some(entries) = value
        .get("query")
        .and_then(|query| query.get("search"))
        .and_then(Value::as_array)
    {
        for entry in entries.iter().take(limit) {
            let title = entry.get("title").and_then(Value::as_str).unwrap_or("");
            let snippet = entry
                .get("snippet")
                .and_then(Value::as_str)
                .map(|snippet| collapse_whitespace(&decode_html(&tag_re.replace_all(snippet, ""))))
                .unwrap_or_default();
            let url = format!("{host}/wiki/{}", title.replace(' ', "_"));
            if let Some(hit) = SearchHit::new(title, url, snippet) {
                hits.push(hit);
            }
        }
    }
    hits
}

fn parse_wikipedia_opensearch(body: &str, limit: usize) -> Vec<SearchHit> {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    let array = value.as_array();
    let titles = array
        .and_then(|a| a.get(1))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let descriptions = array
        .and_then(|a| a.get(2))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let urls = array
        .and_then(|a| a.get(3))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut hits = Vec::new();
    for (index, title) in titles.iter().enumerate().take(limit) {
        let title = title.as_str().unwrap_or("");
        let url = urls.get(index).and_then(Value::as_str).unwrap_or("");
        let snippet = descriptions
            .get(index)
            .and_then(Value::as_str)
            .unwrap_or("");
        if let Some(hit) = SearchHit::new(title, url, snippet) {
            hits.push(hit);
        }
    }
    hits
}

// ---------------------------------------------------------------------------
// Shared post-processing
// ---------------------------------------------------------------------------

/// Decode engine redirect wrappers (DuckDuckGo `uddg=`, Bing `ck/a?u=a1<base64>`)
/// into real target URLs, and absolutize scheme-relative URLs.
fn normalize_search_url(value: &str) -> String {
    let decoded = decode_html(value.trim());
    let absolute = if decoded.starts_with("//") {
        format!("https:{decoded}")
    } else if decoded.starts_with('/') {
        format!("https://duckduckgo.com{decoded}")
    } else {
        decoded
    };
    let Some(url) = reqwest::Url::parse(&absolute).ok() else {
        return absolute;
    };
    let host = url.host_str().unwrap_or("");
    if host.ends_with("duckduckgo.com") {
        if let Some((_, target)) = url.query_pairs().find(|(key, _)| key == "uddg") {
            return target.into_owned();
        }
        return absolute;
    }
    if host.ends_with("bing.com") && url.path().starts_with("/ck/") {
        if let Some(target) = url
            .query_pairs()
            .find(|(key, _)| key == "u")
            .and_then(|(_, value)| decode_bing_ck_target(&value))
        {
            return target;
        }
        return absolute;
    }
    absolute
}

/// Bing wraps result links as `https://www.bing.com/ck/a?…&u=a1<base64url(target)>`.
/// The `u=` value carries the target URL base64url-encoded behind a two-byte
/// marker prefix (typically `a1`/`a2`); decode it back to the real URL.
fn decode_bing_ck_target(value: &str) -> Option<String> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    // Try with and without the two-byte marker prefix; accept whichever
    // decodes to a plausible http(s) URL.
    let stripped = value.get(2..).filter(|_| value.len() > 2);
    for candidate in [stripped, Some(value)].into_iter().flatten() {
        let text = URL_SAFE_NO_PAD
            .decode(candidate)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok());
        if let Some(text) =
            text.filter(|text| text.starts_with("http://") || text.starts_with("https://"))
        {
            return Some(text);
        }
    }
    None
}

/// Dedupe by canonical URL (lowercased host, stripped fragment/trailing slash),
/// preserving engine ranking, then truncate to `limit`.
fn finalize_hits(hits: Vec<SearchHit>, limit: usize) -> Vec<SearchHit> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for hit in hits {
        let mut key = hit.url.trim().to_string();
        if let Ok(mut parsed) = reqwest::Url::parse(&key) {
            parsed.set_fragment(None);
            if let Some(host) = parsed.host_str() {
                let _ = parsed.set_host(Some(&host.to_ascii_lowercase()));
            }
            key = parsed.to_string();
        }
        let key = key.trim_end_matches('/').to_ascii_lowercase();
        if seen.insert(key) {
            out.push(hit);
        }
        if out.len() >= limit {
            break;
        }
    }
    out
}

/// Lexical relevance gate: at least one hit must share a query signal token.
/// Signal tokens are ASCII words of 2+ chars plus CJK bigrams (single CJK
/// characters are far too common to mean anything). Query operators/quotes
/// are stripped. Queries without any usable token pass unconditionally.
fn results_are_relevant(query: &str, hits: &[SearchHit]) -> bool {
    let mut ascii_tokens = Vec::new();
    let mut cjk_bigrams = Vec::new();
    let mut cjk_run = String::new();
    let flush_cjk = |cjk_run: &mut String, bigrams: &mut Vec<String>| {
        let chars: Vec<char> = cjk_run.chars().collect();
        if chars.len() >= 2 {
            for pair in chars.windows(2) {
                bigrams.push(pair.iter().collect());
            }
        }
        cjk_run.clear();
    };
    let mut ascii_run = String::new();
    let flush_ascii = |ascii_run: &mut String, tokens: &mut Vec<String>| {
        if ascii_run.chars().count() >= 2 {
            tokens.push(ascii_run.to_ascii_lowercase());
        }
        ascii_run.clear();
    };
    for ch in query.chars() {
        if ('\u{4e00}'..='\u{9fff}').contains(&ch) || ('\u{3400}'..='\u{4dbf}').contains(&ch) {
            flush_ascii(&mut ascii_run, &mut ascii_tokens);
            cjk_run.push(ch);
        } else if ch.is_ascii_alphanumeric() {
            flush_cjk(&mut cjk_run, &mut cjk_bigrams);
            ascii_run.push(ch);
        } else {
            flush_ascii(&mut ascii_run, &mut ascii_tokens);
            flush_cjk(&mut cjk_run, &mut cjk_bigrams);
        }
    }
    flush_ascii(&mut ascii_run, &mut ascii_tokens);
    flush_cjk(&mut cjk_run, &mut cjk_bigrams);
    if ascii_tokens.is_empty() && cjk_bigrams.is_empty() {
        return true;
    }
    hits.iter().any(|hit| {
        let haystack = format!("{} {} {}", hit.title, hit.url, hit.snippet).to_ascii_lowercase();
        cjk_bigrams
            .iter()
            .any(|bigram| haystack.contains(bigram.as_str()))
            || ascii_tokens
                .iter()
                .any(|token| haystack.contains(token.as_str()))
    })
}

fn render_hits(hits: &[SearchHit], engine: &str) -> String {
    let mut out = String::new();
    for (index, hit) in hits.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        out.push_str(&format!("{}. {}\n   {}", index + 1, hit.title, hit.url));
        if !hit.snippet.is_empty() {
            out.push_str(&format!("\n   {}", hit.snippet));
        }
        out.push('\n');
    }
    out.push_str(&format!("(source: {engine})"));
    out
}

fn web_search_unavailable(reason: impl AsRef<str>) -> ToolResult {
    ToolResult::error(format!(
        "web_search unavailable: {}. Do not retry this search with shell, curl, wget, or command-line browsing; report the cause once to the user.",
        reason.as_ref()
    ))
}

// ---------------------------------------------------------------------------
// Tests (offline fixtures; live chain behind `--ignored`)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_bing_rss_extracts_items() {
        let rss = r#"<?xml version="1.0"?><rss><channel><item><title><![CDATA[Example &amp; Result]]></title><link>https://example.com/a?q=1</link><description><![CDATA[<p>First snippet</p>]]></description></item><item><title><![CDATA[Second]]></title><link>https://example.com/b</link><description><![CDATA[Second snippet]]></description></item></channel></rss>"#;
        let hits = parse_bing_rss(rss, 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].title, "Example & Result");
        assert_eq!(hits[0].url, "https://example.com/a?q=1");
        assert_eq!(hits[0].snippet, "First snippet");
        assert_eq!(hits[1].title, "Second");
    }

    #[test]
    fn parse_bing_html_extracts_wrapped_h2_and_snippet() {
        // Mirrors live Bing markup captured 2026-08, both A/B layouts:
        // A) b_algoheader: <a href><h2>…  B) classic: <h2><a href>…
        let html = r#"<html><body><ol id="b_results">
        <li class="b_algo" data-id iid=SERP.5233><link rel="stylesheet" href="/rs/x.css" type="text/css"/>
        <a target="_blank" href="https://github.com/features/copilot" h="ID=SERP,1.1"><div class="b_tpcn"></div><div class="b_attribution" tabindex="-1"><cite>https://<strong>github.com</strong> › features › <strong>copilot</strong></cite></div></a>
        <div class="b_algoheader"><a href="https://github.com/features/copilot" h="ID=SERP,5110.2"><h2 class=""><strong>GitHub Copilot</strong> · Your AI pair programmer</h2></a></div>
        <div class="b_caption"><p class="b_lineclamp3">GitHub Copilot transforms the developer experience.</p></div></li>
        <li class="b_algo"><div class="tptxt"><div class="tpmeta"><div class="b_attribution" tabindex="-1"><cite>https://docs.github.com</cite></div></div></div>
        <h2 class=""><a target="_blank" href="https://docs.github.com/en/copilot/get-started/what-is-github-copilot" h="ID=SERP,5123.2">What is <strong>GitHub Copilot</strong>? - <strong>GitHub</strong> Docs</a></h2>
        <div class="b_caption"><p class="b_lineclamp2">GitHub Copilot is an AI coding assistant.</p></div></li>
        </ol></body></html>"#;
        let hits = parse_bing_html(html, 10);
        assert_eq!(hits.len(), 2, "{hits:?}");
        assert_eq!(hits[0].url, "https://github.com/features/copilot");
        assert_eq!(hits[0].title, "GitHub Copilot · Your AI pair programmer");
        assert_eq!(
            hits[0].snippet,
            "GitHub Copilot transforms the developer experience."
        );
        assert_eq!(hits[1].title, "What is GitHub Copilot? - GitHub Docs");
    }

    #[test]
    fn parse_baidu_html_extracts_results_and_skips_ads() {
        let html = r#"<div class="result c-container xpath-log new-pmd" id="1"><h3 class="t"><a href="http://www.baidu.com/link?url=abc123" target="_blank">Rust <em>官网</em></a></h3><div class="c-abstract">Rust 是一门系统编程语言，注重安全与并发。</div></div>
        <div class="result c-container" id="2"><h3 class="t"><a href="http://www.baidu.com/link?url=ad9" target="_blank">推广页</a></h3><div class="c-abstract"><span class="f13">广告</span>买课</div></div>
        <div class="result-op c-container" id="3"><h3 class="t c-title"><a href="https://doc.rust-lang.org/book/" target="_blank">The Rust Book</a></h3><div class="c-abstract c-span-last">The official book.</div></div>"#;
        let hits = parse_baidu_html(html, 10);
        assert_eq!(hits.len(), 2, "{hits:?}");
        assert_eq!(hits[0].title, "Rust 官网");
        assert_eq!(hits[0].url, "http://www.baidu.com/link?url=abc123");
        assert!(hits[0].snippet.contains("系统编程语言"));
        assert_eq!(hits[1].title, "The Rust Book");
        assert_eq!(hits[1].url, "https://doc.rust-lang.org/book/");
        assert_eq!(hits[1].snippet, "The official book.");
    }

    #[test]
    fn parse_sogou_html_extracts_results() {
        let html = r#"<div class="results"><div class="rb" id="sogou_vr_1"><h3 class="vr-title"><a href="https://www.rust-lang.org/zh-CN/" id="sogou_vr_title_" target="_blank">Rust 中文官网</a></h3><div class="ft"><div class="str-text-info">Rust 语言中文官方网站。</div></div></div>
        <div class="vrwrap"><h3 class="vrTitle"><a href="/link?url=xyz" target="_blank">知乎讨论</a></h3><p class="fz-mid">Rust 值得学吗？</p></div></div>"#;
        let hits = parse_sogou_html(html, 10);
        assert_eq!(hits.len(), 2, "{hits:?}");
        assert_eq!(hits[0].title, "Rust 中文官网");
        assert_eq!(hits[0].url, "https://www.rust-lang.org/zh-CN/");
        assert!(hits[0].snippet.contains("官方网站"));
        assert_eq!(hits[1].url, "https://www.sogou.com/link?url=xyz");
    }

    #[test]
    fn parse_duckduckgo_html_handles_classic_and_lite() {
        let classic = r##"<div class="result"><a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fpage&rut=deadbeef">Example Page</a><a class="result__snippet" href="#">Snippet for example page</a></div>"##;
        let hits = parse_duckduckgo_html(classic, 10);
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].title, "Example Page");
        assert_eq!(hits[0].url, "https://example.com/page");
        assert_eq!(hits[0].snippet, "Snippet for example page");

        let lite = r#"<table><tr><td><a rel="nofollow" href="https://real.site/x" class='result-link'>Real Site X</a></td></tr><tr><td class="result-snippet">Lite snippet here</td></tr></table>"#;
        let lite_hits = parse_duckduckgo_html(lite, 10);
        assert_eq!(lite_hits.len(), 1, "{lite_hits:?}");
        assert_eq!(lite_hits[0].url, "https://real.site/x");
        assert_eq!(lite_hits[0].title, "Real Site X");
    }

    #[test]
    fn parse_mojeek_html_extracts_results() {
        let html = r#"<ul class="results-standard"><li class="ob"><h2><a class="title" href="https://example.org/a">Alpha Result</a></h2><p class="s">Alpha snippet text</p></li>
        <li class="ob"><h2><a class="title" href="https://example.org/b">Beta Result</a></h2><p class="s">Beta snippet text</p></li></ul>"#;
        let hits = parse_mojeek_html(html, 10);
        assert_eq!(hits.len(), 2, "{hits:?}");
        assert_eq!(hits[0].title, "Alpha Result");
        assert_eq!(hits[0].snippet, "Alpha snippet text");
    }

    #[test]
    fn parse_searxng_json_extracts_results() {
        let body = json!({
            "query": "rust",
            "results": [
                {"title": "Rust Lang", "url": "https://www.rust-lang.org/", "content": "A language empowering everyone."},
                {"title": "Bad", "url": "javascript:alert(1)", "content": "x"},
                {"title": "Rust Book", "url": "https://doc.rust-lang.org/book/", "content": "Read the book."}
            ]
        })
        .to_string();
        let hits = parse_searxng_json(&body, 10).expect("parse searxng");
        assert_eq!(hits.len(), 2, "{hits:?}");
        assert_eq!(hits[0].title, "Rust Lang");
        assert_eq!(hits[1].url, "https://doc.rust-lang.org/book/");
    }

    #[test]
    fn parse_wikipedia_search_extracts_results() {
        let body = r#"{"query":{"search":[{"title":"深度求索","snippet":"<span class=\"searchmatch\">深度求索</span>（DeepSeek）是一家中国人工智能公司。","pageid":1},{"title":"DeepSeek","snippet":"AI assistant","pageid":2}]}}"#;
        let hits = parse_wikipedia_search(body, "https://zh.wikipedia.org", 10);
        assert_eq!(hits.len(), 2, "{hits:?}");
        assert_eq!(hits[0].title, "深度求索");
        assert_eq!(hits[0].url, "https://zh.wikipedia.org/wiki/深度求索");
        assert!(hits[0].snippet.contains("人工智能公司"));
        assert!(!hits[0].snippet.contains('<'));
    }

    #[test]
    fn parse_wikipedia_opensearch_extracts_results() {
        let body = r#"["rust",["Rust (programming language)","Rust (game)"],["A systems language","A survival game"],["https://en.wikipedia.org/wiki/Rust_(programming_language)","https://en.wikipedia.org/wiki/Rust_(game)"]]"#;
        let hits = parse_wikipedia_opensearch(body, 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].title, "Rust (programming language)");
        assert_eq!(hits[0].snippet, "A systems language");
    }

    #[test]
    fn finalize_hits_dedupes_canonical_urls_and_keeps_ranking() {
        let hits = vec![
            SearchHit::new("A", "https://Example.com/page#frag", "s1").expect("hit A"),
            SearchHit::new("A2", "https://example.com/page/", "s2").expect("hit A2"),
            SearchHit::new("B", "https://other.com/", "s3").expect("hit B"),
        ];
        let out = finalize_hits(hits, 10);
        assert_eq!(out.len(), 2, "{out:?}");
        assert_eq!(out[0].title, "A");
        assert_eq!(out[1].title, "B");
        let limited = finalize_hits(out, 1);
        assert_eq!(limited.len(), 1);
    }

    #[test]
    fn search_hit_rejects_junk() {
        assert!(SearchHit::new("", "https://a.com/", "s").is_none());
        assert!(SearchHit::new("T", "ftp://a.com/", "s").is_none());
        assert!(SearchHit::new("T", "https://a.com/", "s").is_some());
    }

    #[test]
    fn normalize_url_decodes_ddg_redirects() {
        let url = normalize_search_url(
            "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fx%3Fa%3D1&rut=abc",
        );
        assert_eq!(url, "https://example.com/x?a=1");
    }

    #[test]
    fn normalize_url_decodes_bing_ck_redirects() {
        // u=a1<base64url("https://github.com/login")>
        let wrapped =
            "https://www.bing.com/ck/a?!&&p=abc&u=a1aHR0cHM6Ly9naXRodWIuY29tL2xvZ2lu&ntb=1";
        assert_eq!(normalize_search_url(wrapped), "https://github.com/login");
        // Non-Bing URLs pass through untouched.
        assert_eq!(
            normalize_search_url("https://example.com/ck/a?u=a1zzz"),
            "https://example.com/ck/a?u=a1zzz"
        );
        // Bing links whose payload is not a URL stay as-is.
        let undecodable = "https://www.bing.com/ck/a?u=%%%invalid%%%";
        assert_eq!(normalize_search_url(undecodable), undecodable);
    }

    #[test]
    fn render_hits_numbers_results() {
        let hits = vec![
            SearchHit::new("One", "https://a.com/", "first").expect("hit one"),
            SearchHit::new("Two", "https://b.com/", "").expect("hit two"),
        ];
        let rendered = render_hits(&hits, "bing_rss");
        assert!(rendered.contains("1. One\n   https://a.com/\n   first"));
        assert!(rendered.contains("2. Two\n   https://b.com/"));
        assert!(rendered.ends_with("(source: bing_rss)"));
    }

    #[test]
    fn engine_chain_defaults_and_override() {
        // CJK default: Chinese-capable engines right after Bing RSS.
        let chain = engine_chain_from(None, None, true);
        assert_eq!(chain.first(), Some(&Engine::BingRss));
        assert_eq!(chain.get(1), Some(&Engine::Sogou));
        assert!(chain.contains(&Engine::SogouWeixin));
        assert!(!chain.contains(&Engine::Searxng));

        // Non-CJK default: western engines lead the Chinese ones.
        let en = engine_chain_from(None, None, false);
        assert_eq!(en.first(), Some(&Engine::BingRss));
        assert_eq!(en.get(1), Some(&Engine::BingHtml));
        let pos_ddg = en.iter().position(|e| *e == Engine::DuckDuckGo);
        let pos_sogou = en.iter().position(|e| *e == Engine::Sogou);
        assert!(pos_ddg < pos_sogou);

        assert_eq!(
            engine_chain_from(Some("baidu,bing_rss"), None, true),
            vec![Engine::Baidu, Engine::BingRss]
        );
        assert_eq!(
            engine_chain_from(Some("weixin"), None, false),
            vec![Engine::SogouWeixin]
        );
        // Unknown names are dropped; if nothing parses, fall back to default.
        assert_eq!(
            engine_chain_from(Some("bogus,,nope"), None, false).first(),
            Some(&Engine::BingRss)
        );
        assert_eq!(
            engine_chain_from(None, Some("https://searx.example.com"), false).first(),
            Some(&Engine::Searxng)
        );
        assert_eq!(
            engine_chain_from(None, Some(""), false).first(),
            Some(&Engine::BingRss)
        );
    }

    #[test]
    fn relevance_gate_accepts_matching_and_rejects_generic_results() {
        let good = vec![
            SearchHit::new(
                "DeepSeek 深度求索官网",
                "https://www.deepseek.com/",
                "深度求索人工智能基础技术研究有限公司",
            )
            .expect("good hit"),
        ];
        assert!(results_are_relevant("深度求索 DeepSeek 官网", &good));
        assert!(results_are_relevant("deepseek 官网", &good));

        let junk = vec![
            SearchHit::new(
                "Anmelden bei Hotmail | Microsoft Support",
                "https://support.microsoft.com/de-de/accounts-billing/manage",
                "Microsoft hält immer ein Auge auf ungewöhnliche Anmeldeaktivitäten",
            )
            .expect("junk hit"),
        ];
        assert!(!results_are_relevant("深度求索 DeepSeek 官网", &junk));

        // English query, partial-token match is enough ("github" in title/url).
        let gh = vec![
            SearchHit::new("GitHub login", "https://github.com/login", "sign in").expect("gh hit"),
        ];
        assert!(results_are_relevant("github", &gh));
        assert!(!results_are_relevant("nonexistent-thing-xyz", &gh));

        // Queries without any usable token pass unconditionally.
        assert!(results_are_relevant("??? ,,,", &junk));
    }

    #[test]
    fn parse_sogou_weixin_html_extracts_articles() {
        let html = r##"<ul class="news-list2"><li id="sogou_vr_11002601_box_0"><div class="img-box"></div><div class="txt-box">
        <h3><a href="/link?url=dn9a_-gY295K0Rci_xozVXfd&amp;k=1" id="weixin_account_0" uigs="account_name_0" target="_blank">福建师范大学永泰附属中学关于2026级校服选用<em>征询</em>结果的公告</a></h3>
        <p class="txt-info" id="sogou_vr_11002601_summary_0">关于2026级校服选用征询结果的公告福建师范大学永泰附属中学各位家长:根据教育部及地方教育主管部门关于校服管理的相关规定...</p>
        <div class="s-p"><a class="account" href="#">师大永泰附中</a></div></div></li>
        <li id="sogou_vr_11002601_box_1"><div class="txt-box"><h3><a href="/link?url=abc2&amp;k=1" target="_blank">美术学院赴福建师范大学附属中学看望实习学生</a></h3><p class="txt-info">2020级辅导员一行先后赴福建师范大学附属中学...</p></div></li></ul>"##;
        let hits = parse_sogou_weixin_html(html, 10);
        assert_eq!(hits.len(), 2, "{hits:?}");
        assert!(hits[0].title.contains("校服选用"));
        assert!(!hits[0].title.contains("<em>"));
        assert!(
            hits[0]
                .url
                .starts_with("https://weixin.sogou.com/link?url=dn9a_")
        );
        assert!(hits[0].snippet.contains("校服管理"));
        assert!(hits[1].title.contains("实习学生"));
    }

    #[test]
    fn has_cjk_detects_chinese() {
        assert!(has_cjk("深度求索"));
        assert!(!has_cjk("github copilot"));
    }

    /// Live end-to-end check of the whole failover chain. Run explicitly with:
    /// `cargo test -p coomi-tools web_search_live_chain -- --ignored`
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn web_search_live_chain_end_to_end() {
        let result = search(&json!({"query": "github", "limit": 5})).await;
        println!(
            "---- live search output ----\n{}\n----------------------------",
            result.output
        );
        assert!(result.success, "{}", result.output);
        assert!(result.output.contains("1. "), "{}", result.output);
        assert!(result.output.contains("(source:"), "{}", result.output);
    }

    /// Live check with a Chinese query through the default chain. Run with:
    /// `cargo test -p coomi-tools web_search_live_chinese -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn web_search_live_chinese_query() {
        let result = search(&json!({"query": "深度求索 DeepSeek 官网", "limit": 5})).await;
        println!(
            "---- live zh search output ----\n{}\n-------------------------------",
            result.output
        );
        assert!(result.success, "{}", result.output);
        assert!(result.output.contains("1. "), "{}", result.output);
    }

    /// Live regression check for a Chinese long-tail query (a specific school
    /// name reported as unfindable). Run with:
    /// `cargo test -p coomi-tools web_search_live_school -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn web_search_live_school_query() {
        let result = search(&json!({"query": "福建师范大学永泰附属中学", "limit": 5})).await;
        println!(
            "---- live school search output ----\n{}\n-----------------------------------",
            result.output
        );
        assert!(result.success, "{}", result.output);
        assert!(result.output.contains("1. "), "{}", result.output);
        assert!(result.output.contains("永泰"), "{}", result.output);
    }
}
