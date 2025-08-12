use std::fs::File;
use std::io::Read;

use anyhow::Result;
use feed_rs::model::{Feed, Person};
use feed_rs::parser;
use futures::stream::{self, StreamExt};
use polars::prelude::*;
use reqwest::IntoUrl;
use url::Url;

pub fn feed_from_file(path: String) -> Result<Feed> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    let feed = parser::parse(&buffer[..])?;
    Ok(feed)
}

pub async fn feed_from_url<T>(url: T) -> Result<Feed>
where
    T: IntoUrl,
{
    let content = reqwest::get(url).await?.bytes().await?;
    let feed = parser::parse(&content[..])?;
    Ok(feed)
}

pub fn feed_to_lf(feed: Feed) -> Result<LazyFrame> {
    let n_entries = feed.entries.len();
    let feed_description = feed.description.map(|d| d.content).unwrap_or_default();
    let feed_published = feed.published.map(|d| d.to_rfc3339()).unwrap_or_default();
    let feed_title = feed.title.map(|t| t.content).unwrap_or_default();
    let feed_updated = feed.updated.map(|d| d.to_rfc3339()).unwrap_or_default();

    let mut ids = Vec::new();
    let mut links = Vec::new();
    let mut pub_dates = Vec::new();
    let mut pub_dates_ms = Vec::new();
    let mut titles = Vec::new();
    let mut summaries = Vec::new();
    let mut contents = Vec::new();
    let mut updated_dates = Vec::new();
    let mut updated_dates_ms = Vec::new();
    let mut authors = Vec::new();
    let mut media_urls = Vec::new();
    let mut media_descriptions = Vec::new();

    for entry in feed.entries {
        ids.push(entry.clone().id);
        titles.push(entry.title.clone().map(|t| t.content).unwrap_or_default());
        links.push(
            entry
                .links
                .first()
                .map(|l| l.href.clone())
                .unwrap_or_default(),
        );
        pub_dates.push(entry.published.map(|d| d.to_rfc2822()).unwrap_or_default());
        pub_dates_ms.push(
            entry
                .published
                .map(|d| d.timestamp_millis())
                .unwrap_or_default(),
        );
        summaries.push(entry.summary.clone().map(|s| s.content).unwrap_or_default());
        contents.push(
            entry
                .content
                .clone()
                .and_then(|c| c.body)
                .unwrap_or_default(),
        );
        updated_dates.push(entry.updated.map(|d| d.to_rfc2822()).unwrap_or_default());
        updated_dates_ms.push(
            entry
                .updated
                .map(|d| d.timestamp_millis())
                .unwrap_or_default(),
        );
        authors.push(format_authors(&entry.authors));
        media_urls.push(extract_media_url(&entry));
        media_descriptions.push(extract_media_description(&entry));
    }

    let df = DataFrame::new(vec![
        Series::new("feed_id", vec![feed.id; n_entries]),
        Series::new("feed_description", vec![feed_description; n_entries]),
        Series::new("feed_published", vec![feed_published; n_entries]),
        Series::new("feed_title", vec![feed_title; n_entries]),
        Series::new("feed_updated", vec![feed_updated; n_entries]),
        Series::new("id", ids),
        Series::new("title", titles),
        Series::new("link", links),
        Series::new("published", pub_dates),
        Series::new("published_ms", pub_dates_ms),
        Series::new("summary", summaries),
        Series::new("content", contents),
        Series::new("updated", updated_dates),
        Series::new("updated_ms", updated_dates_ms),
        Series::new("authors", authors),
        Series::new("media_url", media_urls),
        Series::new("media_descriptions", media_descriptions),
    ])?;

    Ok(df.lazy())
}

fn format_authors(authors: &[Person]) -> String {
    authors
        .iter()
        .map(|a| a.name.clone())
        .collect::<Vec<String>>()
        .join(", ")
}

fn extract_media_description(entry: &feed_rs::model::Entry) -> String {
    entry
        .media
        .first()
        .and_then(|m| m.description.clone())
        .map(|d| d.content)
        .unwrap_or_default()
}

fn extract_media_url(entry: &feed_rs::model::Entry) -> String {
    entry
        .media
        .first()
        .and_then(|m| m.content.first())
        .and_then(|c| c.url.as_ref())
        .map(|url| url.to_string())
        .unwrap_or_default()
}

pub async fn urls_to_lazyframes(urls: Vec<Url>) -> Result<Vec<LazyFrame>> {
    let lazyframes = stream::iter(urls)
        .map(|url| async move {
            match feed_from_url(url).await {
                Ok(feed) => feed_to_lf(feed).ok(),
                Err(_) => None,
            }
        })
        .buffer_unordered(4)
        .filter_map(|df_option| async move { df_option })
        .collect::<Vec<LazyFrame>>()
        .await;

    Ok(lazyframes)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_feed_from_url_blog() {
        let url = "https://www.scotswhayhae.com/blog-feed.xml";
        let feed = feed_from_url(url).await.unwrap();
        let feed_title = feed.title.map(|t| t.content).unwrap_or_default();
        assert_eq!(feed_title, "Scots Whay Hae!");
    }
    #[tokio::test]
    async fn test_feed_from_url_podcast() {
        let url = "https://anchor.fm/s/4fb99a98/podcast/rss".to_string();
        let feed = feed_from_url(url).await.unwrap();
        let feed_title = feed.title.map(|t| t.content).unwrap_or_default();
        assert_eq!(feed_title, "Scots Whay Hae!");
    }
    #[tokio::test]
    async fn test_feed_from_url_videos() {
        let url = "https://www.youtube.com/feeds/videos.xml?channel_id=UCw3KWjq0CcrJfSiXqZjeGiQ"
            .to_string();
        let feed = feed_from_url(url).await.unwrap();
        let feed_title = feed.title.map(|t| t.content).unwrap_or_default();
        assert_eq!(feed_title, "Scots Whay Hae!");
    }

    #[test]
    fn test_feed_from_file_blog() {
        let path = "feeds/blog-feed.xml".to_string();
        let feed = feed_from_file(path).unwrap();
        let feed_title = feed.title.map(|t| t.content).unwrap_or_default();
        assert_eq!(feed_title, "Scots Whay Hae!");
        let n_entries = feed.entries.len();
        assert_eq!(n_entries, 20);
    }
    #[test]
    fn test_feed_from_file_podcast() {
        let path = "feeds/podcast.rss".to_string();
        let feed = feed_from_file(path).unwrap();
        let feed_title = feed.title.map(|t| t.content).unwrap_or_default();
        assert_eq!(feed_title, "Scots Whay Hae!");
        let n_entries = feed.entries.len();
        assert_eq!(n_entries, 324);
    }
    #[test]
    fn test_feed_from_file_videos() {
        let path = "feeds/videos.xml".to_string();
        let feed = feed_from_file(path).unwrap();
        let feed_title = feed.title.map(|t| t.content).unwrap_or_default();
        assert_eq!(feed_title, "Scots Whay Hae!");
        let n_entries = feed.entries.len();
        assert_eq!(n_entries, 15);
    }

    #[test]
    fn test_feed_to_df() -> Result<()> {
        let path = "feeds/blog-feed.xml".to_string();
        let feed = feed_from_file(path).unwrap();
        let lf = feed_to_lf(feed).unwrap();
        let df = lf.collect()?;
        let n_rows = df.height();
        assert_eq!(n_rows, 20);
        let n_cols = df.width();
        assert_eq!(n_cols, 16);
        // define expected columns
        let expected_columns = vec![
            "feed_id",
            "feed_description",
            "feed_published",
            "feed_title",
            "feed_updated",
            "id",
            "title",
            "link",
            "published",
            "published_ms",
            "summary",
            "content",
            "updated",
            "updated_ms",
            "authors",
            "media_url",
        ];
        // check column names
        let columns = df.get_column_names();
        assert_eq!(columns, expected_columns);

        // check max value in published_ms
        let max_published_ms: i64 = df
            .column("published_ms")?
            .i64()?
            .max()
            .expect("could not get max value");

        assert_eq!(max_published_ms, 1718949619000);

        Ok(())
    }
}
