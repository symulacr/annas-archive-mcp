use scraper::{Html, Selector};

use crate::error::Error;
use crate::types::SearchResult;

pub fn parse_search_results(html: &str) -> Result<(Vec<SearchResult>, bool), Error> {
    let document = Html::parse_document(html);

    // Select result containers
    let result_selector =
        Selector::parse("div.flex.pt-3.pb-3.border-b").map_err(|e| Error::Parse {
            message: format!("Invalid selector: {e:?}"),
        })?;

    let link_selector = Selector::parse("a[href^=\"/md5/\"]").map_err(|e| Error::Parse {
        message: format!("Invalid selector: {e:?}"),
    })?;

    let title_selector = Selector::parse("a.js-vim-focus").map_err(|e| Error::Parse {
        message: format!("Invalid selector: {e:?}"),
    })?;

    let metadata_selector =
        Selector::parse("div.text-gray-800.font-semibold.text-sm").map_err(|e| Error::Parse {
            message: format!("Invalid selector: {e:?}"),
        })?;

    let author_icon_selector =
        Selector::parse("span.icon-\\[mdi--user-edit\\]").map_err(|e| Error::Parse {
            message: format!("Invalid selector: {e:?}"),
        })?;

    let mut results = Vec::new();

    for result_elem in document.select(&result_selector) {
        // Extract MD5 from first link with /md5/ href
        let md5 = result_elem
            .select(&link_selector)
            .next()
            .and_then(|a| a.value().attr("href"))
            .and_then(|href| href.strip_prefix("/md5/"))
            .map(|s| s.to_string());

        let Some(md5) = md5 else {
            continue;
        };

        // Extract title from a.js-vim-focus
        let title = result_elem
            .select(&title_selector)
            .next()
            .map(|a| a.text().collect::<String>().trim().to_string())
            .unwrap_or_default();

        if title.is_empty() {
            continue;
        }

        // Extract author - look for link containing the user-edit icon
        let author = result_elem
            .select(&Selector::parse("a").unwrap())
            .find(|a| a.select(&author_icon_selector).next().is_some())
            .map(|a| a.text().collect::<String>().trim().to_string())
            .filter(|s| !s.is_empty());

        // Parse metadata line (format · size · language · year)
        // Note: The metadata div contains inline <script> tags, so we must only
        // collect text from non-script elements to avoid capturing JS code
        let metadata_text = result_elem
            .select(&metadata_selector)
            .next()
            .map(|div| extract_text_without_scripts(div))
            .unwrap_or_default();

        let (format, size, language) = parse_metadata_line(&metadata_text);

        results.push(SearchResult {
            md5,
            title,
            author,
            format,
            size,
            language,
        });
    }

    // Detect pagination - look for "Results X-Y (Z+ total)" pattern
    let has_more = detect_has_more(&document);

    Ok((results, has_more))
}

/// Extracts text from an element while skipping <script> tags.
/// This is necessary because Anna's Archive embeds inline scripts in metadata divs.
fn extract_text_without_scripts(element: scraper::ElementRef) -> String {
    use scraper::Node;

    let mut text = String::new();

    for node in element.descendants() {
        if let Node::Text(t) = node.value() {
            // Check if any ancestor is a script tag
            let in_script = node.ancestors().any(|ancestor| {
                ancestor
                    .value()
                    .as_element()
                    .is_some_and(|el| el.name() == "script")
            });

            if !in_script {
                text.push_str(t);
            }
        }
    }

    text
}

/// Checks if a string looks like a file size (e.g., "6.4MB", "512KB", "1.2GB")
fn is_file_size(s: &str) -> bool {
    let s = s.trim().to_lowercase();

    // Must end with a size unit
    let units = ["gb", "mb", "kb", "b"];
    let Some(unit) = units.iter().find(|u| s.ends_with(*u)) else {
        return false;
    };

    // Get the part before the unit
    let number_part = &s[..s.len() - unit.len()];

    // Must have some digits before the unit
    number_part.chars().any(|c| c.is_ascii_digit())
}

fn parse_metadata_line(text: &str) -> (Option<String>, Option<String>, Option<String>) {
    let parts: Vec<&str> = text.split('·').map(|s| s.trim()).collect();

    let mut format = None;
    let mut size = None;
    let mut language = None;

    for part in parts {
        let part_lower = part.to_lowercase();

        // Check if it's a file format
        if matches!(
            part_lower.as_str(),
            "pdf"
                | "epub"
                | "mobi"
                | "azw3"
                | "djvu"
                | "cbr"
                | "cbz"
                | "fb2"
                | "txt"
                | "doc"
                | "docx"
                | "rtf"
        ) {
            format = Some(part.to_uppercase());
        }
        // Check if it's a size (number followed by B, KB, MB, GB)
        // Must have a digit before the unit to avoid matching things like "zlib"
        else if is_file_size(&part_lower) {
            size = Some(part.to_string());
        }
        // Check if it contains language code like "[en]"
        else if part.contains('[') && part.contains(']') {
            language = Some(part.to_string());
        }
    }

    (format, size, language)
}

fn detect_has_more(document: &Html) -> bool {
    // Look for pagination text like "Results 1-25 (1000+ total)"
    let text_selector = Selector::parse("div.uppercase.text-xs.text-gray-500").ok();

    if let Some(selector) = text_selector {
        for elem in document.select(&selector) {
            let text = elem.text().collect::<String>();
            if text.contains("total") && (text.contains('+') || text.contains("more")) {
                return true;
            }
            // Check if current range end < total
            if let Some(has_more) = parse_pagination_text(&text) {
                return has_more;
            }
        }
    }

    false
}

fn parse_pagination_text(text: &str) -> Option<bool> {
    // Try to parse "Results X-Y (Z total)" or "Results X-Y (Z+ total)"
    let text = text.to_lowercase();

    if !text.contains("results") {
        return None;
    }

    // If it says "+" it means there are more
    if text.contains('+') {
        return Some(true);
    }

    // Try to extract numbers: "results 1-25 (100 total)"
    // This is a simple heuristic - if we see the pattern, assume there might be more
    // unless the end number equals the total
    None
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
}
