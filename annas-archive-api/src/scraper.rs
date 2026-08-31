// Search-results extraction: single-pass astral-tl tree walk over the search results page (formats, authors, languages, has_more paging).

use std::borrow::Cow;

use crate::error::{Error, ParseKind, parse_error};
use crate::types::SearchResult;

/// File formats recognized in metadata lines.
const FILE_FORMATS: &[&str] = &[
    "pdf", "epub", "mobi", "azw3", "djvu", "cbr", "cbz", "fb2", "txt", "doc", "docx", "rtf",
];

fn is_file_size(s: &str) -> bool {
    let s = s.trim();

    ["gb", "mb", "kb", "b"].iter().any(|u| {
        s.len() >= u.len()
            && s[s.len() - u.len()..].eq_ignore_ascii_case(u)
            && s[..s.len() - u.len()].chars().any(|c| c.is_ascii_digit())
    })
}

fn parse_metadata_line(text: &str) -> (Option<String>, Option<String>, Option<String>) {
    let mut format = None;
    let mut size = None;
    let mut language = None;

    for part in text.split('·') {
        let part = part.trim();

        if FILE_FORMATS.iter().any(|f| part.eq_ignore_ascii_case(f)) {
            format = Some(part.to_uppercase());
        } else if is_file_size(part) {
            size = Some(part.to_string());
        } else if part.contains('[') && part.contains(']') {
            language = Some(part.to_string()); // e.g. "English [en]"
        }
    }

    (format, size, language)
}

/// ASCII case-insensitive substring test.
fn contains_ci(h: &str, n: &str) -> bool {
    h.len() >= n.len()
        && h.as_bytes()
            .windows(n.len())
            .any(|w| w.eq_ignore_ascii_case(n.as_bytes()))
}

/// Trim `s` in place; avoids a fresh allocation + copy.
fn trim_in_place(mut s: String) -> String {
    let start = s.len() - s.trim_start().len();
    let keep = s.trim_end().len() - start;
    s.drain(..start);
    s.truncate(keep);
    s
}

/// Pagination: "(Z+ total)" or a "more" hint means more pages exist; "Results X-Y (Z total)" with no "+" is the last page.
fn looks_like_more(text: &str) -> bool {
    let plus = text.contains('+');
    let total = contains_ci(text, "total");
    (plus && (total || contains_ci(text, "results"))) || (total && contains_ci(text, "more"))
}

#[derive(Default)]
struct Card {
    md5: Option<String>,
    title_done: bool,
    title: String,
    author: Option<String>,
    format: Option<String>,
    size: Option<String>,
    language: Option<String>,
}

impl Card {
    fn into_result(self) -> Option<SearchResult> {
        let md5 = self.md5?;
        let title = trim_in_place(self.title);
        (!title.is_empty()).then_some(SearchResult {
            md5,
            title,
            author: self.author,
            format: self.format,
            size: self.size,
            language: self.language,
        })
    }
}

#[derive(Default)]
struct Anchor {
    has_icon: bool,
    text: String,
}

#[derive(Default)]
struct State {
    out: Vec<SearchResult>,
    card: Card,
    anchors: Vec<Anchor>,
    title_open: bool,
    meta_open: bool,
    meta_buf: String,
    script_open: bool,
    more_open: bool,
    more_buf: String,
    has_more: bool,
}

impl State {
    fn flush_card(&mut self) {
        self.title_open = false;
        if let Some(r) = std::mem::take(&mut self.card).into_result() {
            self.out.push(r);
        }
    }
}

pub fn parse_search_results(html: &str) -> Result<(Vec<SearchResult>, bool), Error> {
    let dom = astral_tl::parse(html, astral_tl::ParserOptions::default())
        .map_err(|e| parse_error(format!("HTML parse failed: {e}"), ParseKind::MalformedJson))?;
    let parser = dom.parser();
    let mut st = State::default();
    for node in dom.children().iter().filter_map(|h| h.get(parser)) {
        walk(&mut st, parser, node);
    }
    st.flush_card();
    Ok((std::mem::take(&mut st.out), st.has_more))
}

fn attr<'a>(tag: &'a astral_tl::HTMLTag<'a>, key: &'a str) -> Option<Cow<'a, str>> {
    match tag.attributes().get(key) {
        Some(Some(b)) => Some(b.as_utf8_str()),
        _ => None,
    }
}

fn children(st: &mut State, parser: &astral_tl::Parser, tag: &astral_tl::HTMLTag) {
    for child in tag.children().top().iter().filter_map(|h| h.get(parser)) {
        walk(st, parser, child);
    }
}

fn walk(st: &mut State, parser: &astral_tl::Parser, node: &astral_tl::Node) {
    match node {
        astral_tl::Node::Raw(bytes) => {
            let t = bytes.as_utf8_str();
            if st.title_open {
                st.card.title.push_str(&t);
            }
            if let Some(a) = st.anchors.last_mut() {
                a.text.push_str(&t);
            }
            if st.meta_open && !st.script_open {
                st.meta_buf.push_str(&t);
            }
            if st.more_open {
                st.more_buf.push_str(&t);
            }
        }
        astral_tl::Node::Tag(tag) => {
            let name = tag.name().as_utf8_str();
            let cow = attr(tag, "class");
            let class = cow.as_deref().unwrap_or("");
            let mut is_card = false;
            let mut is_meta = false;
            let mut is_more = false;
            if name.as_ref() == "div" {
                const SETS: [&[&str]; 3] = [
                    &["flex", "pt-3", "pb-3", "border-b"],
                    &["text-gray-800", "font-semibold", "text-sm"],
                    &["uppercase", "text-xs", "text-gray-500"],
                ];
                let mut hit = [0usize; 3];
                for tok in class.split_whitespace() {
                    for (i, set) in SETS.iter().enumerate() {
                        if hit[i] != set.len() && set.contains(&tok) {
                            hit[i] += 1;
                        }
                    }
                }
                is_card = hit[0] == SETS[0].len();
                is_meta = hit[1] == SETS[1].len();
                is_more = hit[2] == SETS[2].len();
            }
            if is_card {
                st.flush_card();
                children(st, parser, tag);
                return;
            }
            match name.as_ref() {
                "script" => {
                    st.script_open = true;
                    children(st, parser, tag);
                    st.script_open = false;
                }
                "a" => {
                    if st.card.md5.is_none()
                        && let Some(href) = attr(tag, "href")
                        && let Some(rest) = href.strip_prefix("/md5/")
                    {
                        st.card.md5 = Some(rest.to_string());
                    }
                    let is_title = !st.card.title_done
                        && class.split_whitespace().any(|t| t == "js-vim-focus");
                    if is_title {
                        st.card.title_done = true;
                        st.title_open = true;
                    }
                    st.anchors.push(Anchor::default());
                    children(st, parser, tag);
                    let anchor = st.anchors.pop().unwrap_or_default();
                    if anchor.has_icon && st.card.author.is_none() {
                        let t = trim_in_place(anchor.text);
                        if !t.is_empty() {
                            st.card.author = Some(t);
                        }
                    }
                    if is_title {
                        st.title_open = false;
                    }
                }
                "span" => {
                    if class.contains("mdi--user-edit")
                        && let Some(a) = st.anchors.last_mut()
                    {
                        a.has_icon = true;
                    }
                    children(st, parser, tag);
                }
                _ => {
                    if is_meta {
                        st.meta_open = true;
                        st.meta_buf.clear();
                    }
                    if is_more {
                        st.more_open = true;
                    }
                    children(st, parser, tag);
                    if is_meta {
                        st.meta_open = false;
                        (st.card.format, st.card.size, st.card.language) =
                            parse_metadata_line(st.meta_buf.trim());
                    }
                    if is_more {
                        st.more_open = false;
                        st.has_more |= looks_like_more(st.more_buf.trim());
                    }
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metadata_line() {
        let (format, size, language) = parse_metadata_line("PDF · 54.2MB · English [en] · 1987");
        assert_eq!(format, Some("PDF".to_string()));
        assert_eq!(size, Some("54.2MB".to_string()));
        assert_eq!(language, Some("English [en]".to_string()));
    }

    #[test]
    fn test_malformed_html_does_not_panic() {
        let _ = parse_search_results("<div class=\"flex pt-3 pb-3 border-b\"><a href=\"/md5/");
        let _ = parse_search_results(
            "<div class=\"flex pt-3 pb-3 border-b\"><div><a href=\"/md5/ab\" class=\"js-vim-focus\">T</a><div>",
        );
        let _ = parse_search_results(
            "<div class=flex pt-3 pb-3 border-b><a href=/md5/abc class=js-vim-focus>Title</a></div>",
        );
    }
}
