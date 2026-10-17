//! Atom and JSON Feed renderers for public notifications.

use crate::config::Config;
use crate::model::NotificationItem;

pub fn xml_escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&apos;"),
            c if (c as u32) < 0x20 && c != '\n' && c != '\t' => {}
            c => o.push(c),
        }
    }
    o
}

pub fn atom(items: &[NotificationItem], cfg: &Config) -> String {
    let base = cfg.server.public_url.trim_end_matches('/');
    let updated = items.first().map(|i| i.created_at.clone()).unwrap_or_else(|| "1970-01-01T00:00:00Z".into());
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<feed xmlns=\"http://www.w3.org/2005/Atom\">\n");
    s.push_str(&format!("<title>{}</title>\n", xml_escape(&cfg.public.site_title)));
    s.push_str("<subtitle>Automatically detected events in the Australian National Electricity Market, from AEMO public data.</subtitle>\n");
    s.push_str(&format!("<id>{}/feed.atom</id>\n", xml_escape(base)));
    s.push_str(&format!("<link rel=\"self\" href=\"{}/feed.atom\"/>\n", xml_escape(base)));
    s.push_str(&format!("<link rel=\"alternate\" href=\"{}/\"/>\n", xml_escape(base)));
    s.push_str(&format!("<updated>{}</updated>\n", xml_escape(&updated)));
    s.push_str(&format!("<author><name>{}</name></author>\n", xml_escape(&cfg.public.site_title)));
    for i in items {
        s.push_str("<entry>\n");
        s.push_str(&format!("<id>{}/n/{}</id>\n", xml_escape(base), i.seq));
        s.push_str(&format!("<title>{}</title>\n", xml_escape(&i.title)));
        s.push_str(&format!("<link href=\"{}\"/>\n", xml_escape(&i.url)));
        s.push_str(&format!("<updated>{}</updated>\n", xml_escape(&i.created_at)));
        s.push_str(&format!("<category term=\"{}\"/>\n", xml_escape(&i.transition)));
        s.push_str(&format!("<category term=\"{}\"/>\n", xml_escape(&i.event.region)));
        s.push_str(&format!("<summary>{}</summary>\n", xml_escape(&i.summary)));
        s.push_str("</entry>\n");
    }
    s.push_str("</feed>\n");
    s
}

pub fn json_feed(items: &[NotificationItem], cfg: &Config) -> serde_json::Value {
    let base = cfg.server.public_url.trim_end_matches('/');
    serde_json::json!({
        "version": "https://jsonfeed.org/version/1.1",
        "title": cfg.public.site_title,
        "home_page_url": format!("{base}/"),
        "feed_url": format!("{base}/feed.json"),
        "description": "Automatically detected events in the Australian National Electricity Market, from AEMO public data.",
        "items": items.iter().map(|i| serde_json::json!({
            "id": format!("{base}/n/{}", i.seq),
            "url": i.url,
            "title": i.title,
            "content_text": i.summary,
            "date_published": i.created_at,
            "tags": [i.transition, i.event.region, i.event.severity.as_str()],
            "_nem": { "event_id": i.event_id, "transition": i.transition, "event": i.event },
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn escapes() {
        assert_eq!(super::xml_escape("a<b & \"c\""), "a&lt;b &amp; &quot;c&quot;");
    }
}
