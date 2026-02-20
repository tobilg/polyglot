//! Time format conversion utilities
//!
//! This module provides functionality for converting time format strings
//! between different SQL dialects. For example, converting Python strftime
//! formats to Snowflake or BigQuery formats.
//!
//! Based on the Python implementation in `sqlglot/time.py`.

use crate::trie::{new_trie_from_keys, Trie, TrieResult};
use std::collections::{HashMap, HashSet};

/// Convert a time format string using a dialect-specific mapping
///
/// This function uses a trie-based algorithm to handle overlapping format
/// specifiers correctly. For example, `%Y` and `%y` both start with `%`,
/// so the algorithm needs to find the longest matching specifier.
///
/// # Arguments
/// * `input` - The format string to convert
/// * `mapping` - A map from source format specifiers to target format specifiers
/// * `trie` - Optional pre-built trie for the mapping keys (for performance)
///
/// # Returns
/// The converted format string, or `None` if input is empty
///
/// # Example
///
/// ```
/// use polyglot_sql::time::format_time;
/// use std::collections::HashMap;
///
/// let mut mapping = HashMap::new();
/// mapping.insert("%Y", "YYYY");
/// mapping.insert("%m", "MM");
/// mapping.insert("%d", "DD");
///
/// let result = format_time("%Y-%m-%d", &mapping, None);
/// assert_eq!(result, Some("YYYY-MM-DD".to_string()));
/// ```
pub fn format_time(
    input: &str,
    mapping: &HashMap<&str, &str>,
    trie: Option<&Trie<()>>,
) -> Option<String> {
    if input.is_empty() {
        return None;
    }

    // Build trie if not provided
    let owned_trie;
    let trie = match trie {
        Some(t) => t,
        None => {
            owned_trie = build_format_trie(mapping);
            &owned_trie
        }
    };

    let chars: Vec<char> = input.chars().collect();
    let size = chars.len();
    let mut start = 0;
    let mut end = 1;
    let mut current = trie;
    let mut chunks = Vec::new();
    let mut sym: Option<String> = None;

    while end <= size {
        let ch = chars[end - 1];
        let (result, subtrie) = current.in_trie_char(ch);

        match result {
            TrieResult::Failed => {
                if let Some(ref matched) = sym {
                    // We had a previous match, use it
                    end -= 1;
                    chunks.push(matched.clone());
                    start += matched.chars().count();
                    sym = None;
                } else {
                    // No match, emit the first character
                    chunks.push(chars[start].to_string());
                    end = start + 1;
                    start += 1;
                }
                current = trie;
            }
            TrieResult::Exists => {
                // Found a complete match, remember it
                let matched: String = chars[start..end].iter().collect();
                sym = Some(matched);
                current = subtrie.unwrap_or(trie);
            }
            TrieResult::Prefix => {
                // Partial match, continue
                current = subtrie.unwrap_or(trie);
            }
        }

        end += 1;

        // At end of string, emit any remaining match
        if result != TrieResult::Failed && end > size {
            let matched: String = chars[start..end - 1].iter().collect();
            chunks.push(matched);
        }
    }

    // Apply mapping to chunks
    let result: String = chunks
        .iter()
        .map(|chunk| {
            mapping
                .get(chunk.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| chunk.clone())
        })
        .collect();

    Some(result)
}

/// Build a trie from format mapping keys
///
/// This is useful for performance when the same mapping will be used
/// multiple times.
pub fn build_format_trie(mapping: &HashMap<&str, &str>) -> Trie<()> {
    new_trie_from_keys(mapping.keys().copied())
}

/// Extract subsecond precision from an ISO timestamp literal
///
/// Given a timestamp like '2023-01-01 12:13:14.123456+00:00', returns
/// the precision (0, 3, or 6) based on the number of subsecond digits.
///
/// # Arguments
/// * `timestamp_literal` - An ISO-8601 timestamp string
///
/// # Returns
/// The precision: 0 (no subseconds), 3 (milliseconds), or 6 (microseconds)
///
/// # Example
///
/// ```
/// use polyglot_sql::time::subsecond_precision;
///
/// assert_eq!(subsecond_precision("2023-01-01 12:13:14"), 0);
/// assert_eq!(subsecond_precision("2023-01-01 12:13:14.123"), 3);
/// assert_eq!(subsecond_precision("2023-01-01 12:13:14.123456"), 6);
/// ```
pub fn subsecond_precision(timestamp_literal: &str) -> u8 {
    // Find the decimal point after seconds
    let dot_pos = match timestamp_literal.find('.') {
        Some(pos) => pos,
        None => return 0,
    };

    // Find where the fractional part ends (timezone or end of string)
    let frac_end = timestamp_literal[dot_pos + 1..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|pos| pos + dot_pos + 1)
        .unwrap_or(timestamp_literal.len());

    let frac_len = frac_end - dot_pos - 1;

    // Count significant digits (exclude trailing zeros)
    let frac_part = &timestamp_literal[dot_pos + 1..frac_end];
    let significant = frac_part.trim_end_matches('0').len();

    if significant > 3 {
        6
    } else if significant > 0 {
        3
    } else if frac_len > 0 {
        // Has fractional part but all zeros - return based on original length
        if frac_len > 3 {
            6
        } else {
            3
        }
    } else {
        0
    }
}

/// Set of valid timezone names (lowercase)
///
/// This includes:
/// - Olson timezone database names (e.g., "america/new_york")
/// - Timezone abbreviations (e.g., "utc", "est")
/// - Region-based names
pub static TIMEZONES: std::sync::LazyLock<HashSet<&'static str>> = std::sync::LazyLock::new(|| {
    let tzs = [
        // Africa
        "africa/abidjan",
        "africa/accra",
        "africa/addis_ababa",
        "africa/algiers",
        "africa/asmara",
        "africa/bamako",
        "africa/bangui",
        "africa/banjul",
        "africa/bissau",
        "africa/blantyre",
        "africa/brazzaville",
        "africa/bujumbura",
        "africa/cairo",
        "africa/casablanca",
        "africa/ceuta",
        "africa/conakry",
        "africa/dakar",
        "africa/dar_es_salaam",
        "africa/djibouti",
        "africa/douala",
        "africa/el_aaiun",
        "africa/freetown",
        "africa/gaborone",
        "africa/harare",
        "africa/johannesburg",
        "africa/juba",
        "africa/kampala",
        "africa/khartoum",
        "africa/kigali",
        "africa/kinshasa",
        "africa/lagos",
        "africa/libreville",
        "africa/lome",
        "africa/luanda",
        "africa/lubumbashi",
        "africa/lusaka",
        "africa/malabo",
        "africa/maputo",
        "africa/maseru",
        "africa/mbabane",
        "africa/mogadishu",
        "africa/monrovia",
        "africa/nairobi",
        "africa/ndjamena",
        "africa/niamey",
        "africa/nouakchott",
        "africa/ouagadougou",
        "africa/porto-novo",
        "africa/sao_tome",
        "africa/tripoli",
        "africa/tunis",
        "africa/windhoek",
        // America
        "america/adak",
        "america/anchorage",
        "america/anguilla",
        "america/antigua",
        "america/araguaina",
        "america/argentina/buenos_aires",
        "america/argentina/catamarca",
        "america/argentina/cordoba",
        "america/argentina/jujuy",
        "america/argentina/la_rioja",
        "america/argentina/mendoza",
        "america/argentina/rio_gallegos",
        "america/argentina/salta",
        "america/argentina/san_juan",
        "america/argentina/san_luis",
        "america/argentina/tucuman",
        "america/argentina/ushuaia",
        "america/aruba",
        "america/asuncion",
        "america/atikokan",
        "america/bahia",
        "america/bahia_banderas",
        "america/barbados",
        "america/belem",
        "america/belize",
        "america/blanc-sablon",
        "america/boa_vista",
        "america/bogota",
        "america/boise",
        "america/cambridge_bay",
        "america/campo_grande",
        "america/cancun",
        "america/caracas",
        "america/cayenne",
        "america/cayman",
        "america/chicago",
        "america/chihuahua",
        "america/ciudad_juarez",
        "america/costa_rica",
        "america/creston",
        "america/cuiaba",
        "america/curacao",
        "america/danmarkshavn",
        "america/dawson",
        "america/dawson_creek",
        "america/denver",
        "america/detroit",
        "america/dominica",
        "america/edmonton",
        "america/eirunepe",
        "america/el_salvador",
        "america/fort_nelson",
        "america/fortaleza",
        "america/glace_bay",
        "america/goose_bay",
        "america/grand_turk",
        "america/grenada",
        "america/guadeloupe",
        "america/guatemala",
        "america/guayaquil",
        "america/guyana",
        "america/halifax",
        "america/havana",
        "america/hermosillo",
        "america/indiana/indianapolis",
        "america/indiana/knox",
        "america/indiana/marengo",
        "america/indiana/petersburg",
        "america/indiana/tell_city",
        "america/indiana/vevay",
        "america/indiana/vincennes",
        "america/indiana/winamac",
        "america/inuvik",
        "america/iqaluit",
        "america/jamaica",
        "america/juneau",
        "america/kentucky/louisville",
        "america/kentucky/monticello",
        "america/kralendijk",
        "america/la_paz",
        "america/lima",
        "america/los_angeles",
        "america/lower_princes",
        "america/maceio",
        "america/managua",
        "america/manaus",
        "america/marigot",
        "america/martinique",
        "america/matamoros",
        "america/mazatlan",
        "america/menominee",
        "america/merida",
        "america/metlakatla",
        "america/mexico_city",
        "america/miquelon",
        "america/moncton",
        "america/monterrey",
        "america/montevideo",
        "america/montserrat",
        "america/nassau",
        "america/new_york",
        "america/nipigon",
        "america/nome",
        "america/noronha",
        "america/north_dakota/beulah",
        "america/north_dakota/center",
        "america/north_dakota/new_salem",
        "america/nuuk",
        "america/ojinaga",
        "america/panama",
        "america/pangnirtung",
        "america/paramaribo",
        "america/phoenix",
        "america/port-au-prince",
        "america/port_of_spain",
        "america/porto_velho",
        "america/puerto_rico",
        "america/punta_arenas",
        "america/rainy_river",
        "america/rankin_inlet",
        "america/recife",
        "america/regina",
        "america/resolute",
        "america/rio_branco",
        "america/santarem",
        "america/santiago",
        "america/santo_domingo",
        "america/sao_paulo",
        "america/scoresbysund",
        "america/sitka",
        "america/st_barthelemy",
        "america/st_johns",
        "america/st_kitts",
        "america/st_lucia",
        "america/st_thomas",
        "america/st_vincent",
        "america/swift_current",
        "america/tegucigalpa",
        "america/thule",
        "america/thunder_bay",
        "america/tijuana",
        "america/toronto",
        "america/tortola",
        "america/vancouver",
        "america/whitehorse",
        "america/winnipeg",
        "america/yakutat",
        "america/yellowknife",
        // Antarctica
        "antarctica/casey",
        "antarctica/davis",
        "antarctica/dumontdurville",
        "antarctica/macquarie",
        "antarctica/mawson",
        "antarctica/mcmurdo",
        "antarctica/palmer",
        "antarctica/rothera",
        "antarctica/syowa",
        "antarctica/troll",
        "antarctica/vostok",
        // Arctic
        "arctic/longyearbyen",
        // Asia
        "asia/aden",
        "asia/almaty",
        "asia/amman",
        "asia/anadyr",
        "asia/aqtau",
        "asia/aqtobe",
        "asia/ashgabat",
        "asia/atyrau",
        "asia/baghdad",
        "asia/bahrain",
        "asia/baku",
        "asia/bangkok",
        "asia/barnaul",
        "asia/beirut",
        "asia/bishkek",
        "asia/brunei",
        "asia/chita",
        "asia/choibalsan",
        "asia/colombo",
        "asia/damascus",
        "asia/dhaka",
        "asia/dili",
        "asia/dubai",
        "asia/dushanbe",
        "asia/famagusta",
        "asia/gaza",
        "asia/hebron",
        "asia/ho_chi_minh",
        "asia/hong_kong",
        "asia/hovd",
        "asia/irkutsk",
        "asia/jakarta",
        "asia/jayapura",
        "asia/jerusalem",
        "asia/kabul",
        "asia/kamchatka",
        "asia/karachi",
        "asia/kathmandu",
        "asia/khandyga",
        "asia/kolkata",
        "asia/krasnoyarsk",
        "asia/kuala_lumpur",
        "asia/kuching",
        "asia/kuwait",
        "asia/macau",
        "asia/magadan",
        "asia/makassar",
        "asia/manila",
        "asia/muscat",
        "asia/nicosia",
        "asia/novokuznetsk",
        "asia/novosibirsk",
        "asia/omsk",
        "asia/oral",
        "asia/phnom_penh",
        "asia/pontianak",
        "asia/pyongyang",
        "asia/qatar",
        "asia/qostanay",
        "asia/qyzylorda",
        "asia/riyadh",
        "asia/sakhalin",
        "asia/samarkand",
        "asia/seoul",
        "asia/shanghai",
        "asia/singapore",
        "asia/srednekolymsk",
        "asia/taipei",
        "asia/tashkent",
        "asia/tbilisi",
        "asia/tehran",
        "asia/thimphu",
        "asia/tokyo",
        "asia/tomsk",
        "asia/ulaanbaatar",
        "asia/urumqi",
        "asia/ust-nera",
        "asia/vientiane",
        "asia/vladivostok",
        "asia/yakutsk",
        "asia/yangon",
        "asia/yekaterinburg",
        "asia/yerevan",
        // Atlantic
        "atlantic/azores",
        "atlantic/bermuda",
        "atlantic/canary",
        "atlantic/cape_verde",
        "atlantic/faroe",
        "atlantic/madeira",
        "atlantic/reykjavik",
        "atlantic/south_georgia",
        "atlantic/st_helena",
        "atlantic/stanley",
        // Australia
        "australia/adelaide",
        "australia/brisbane",
        "australia/broken_hill",
        "australia/darwin",
        "australia/eucla",
        "australia/hobart",
        "australia/lindeman",
        "australia/lord_howe",
        "australia/melbourne",
        "australia/perth",
        "australia/sydney",
        // Europe
        "europe/amsterdam",
        "europe/andorra",
        "europe/astrakhan",
        "europe/athens",
        "europe/belgrade",
        "europe/berlin",
        "europe/bratislava",
        "europe/brussels",
        "europe/bucharest",
        "europe/budapest",
        "europe/busingen",
        "europe/chisinau",
        "europe/copenhagen",
        "europe/dublin",
        "europe/gibraltar",
        "europe/guernsey",
        "europe/helsinki",
        "europe/isle_of_man",
        "europe/istanbul",
        "europe/jersey",
        "europe/kaliningrad",
        "europe/kiev",
        "europe/kirov",
        "europe/kyiv",
        "europe/lisbon",
        "europe/ljubljana",
        "europe/london",
        "europe/luxembourg",
        "europe/madrid",
        "europe/malta",
        "europe/mariehamn",
        "europe/minsk",
        "europe/monaco",
        "europe/moscow",
        "europe/oslo",
        "europe/paris",
        "europe/podgorica",
        "europe/prague",
        "europe/riga",
        "europe/rome",
        "europe/samara",
        "europe/san_marino",
        "europe/sarajevo",
        "europe/saratov",
        "europe/simferopol",
        "europe/skopje",
        "europe/sofia",
        "europe/stockholm",
        "europe/tallinn",
        "europe/tirane",
        "europe/ulyanovsk",
        "europe/uzhgorod",
        "europe/vaduz",
        "europe/vatican",
        "europe/vienna",
        "europe/vilnius",
        "europe/volgograd",
        "europe/warsaw",
        "europe/zagreb",
        "europe/zaporozhye",
        "europe/zurich",
        // Indian
        "indian/antananarivo",
        "indian/chagos",
        "indian/christmas",
        "indian/cocos",
        "indian/comoro",
        "indian/kerguelen",
        "indian/mahe",
        "indian/maldives",
        "indian/mauritius",
        "indian/mayotte",
        "indian/reunion",
        // Pacific
        "pacific/apia",
        "pacific/auckland",
        "pacific/bougainville",
        "pacific/chatham",
        "pacific/chuuk",
        "pacific/easter",
        "pacific/efate",
        "pacific/fakaofo",
        "pacific/fiji",
        "pacific/funafuti",
        "pacific/galapagos",
        "pacific/gambier",
        "pacific/guadalcanal",
        "pacific/guam",
        "pacific/honolulu",
        "pacific/kanton",
        "pacific/kiritimati",
        "pacific/kosrae",
        "pacific/kwajalein",
        "pacific/majuro",
        "pacific/marquesas",
        "pacific/midway",
        "pacific/nauru",
        "pacific/niue",
        "pacific/norfolk",
        "pacific/noumea",
        "pacific/pago_pago",
        "pacific/palau",
        "pacific/pitcairn",
        "pacific/pohnpei",
        "pacific/port_moresby",
        "pacific/rarotonga",
        "pacific/saipan",
        "pacific/tahiti",
        "pacific/tarawa",
        "pacific/tongatapu",
        "pacific/wake",
        "pacific/wallis",
        // Common abbreviations
        "utc",
        "gmt",
        "est",
        "edt",
        "cst",
        "cdt",
        "mst",
        "mdt",
        "pst",
        "pdt",
        "cet",
        "cest",
        "wet",
        "west",
        "eet",
        "eest",
        "gmt+0",
        "gmt-0",
        "gmt0",
        "etc/gmt",
        "etc/utc",
        "etc/gmt+0",
        "etc/gmt-0",
        "etc/gmt+1",
        "etc/gmt+2",
        "etc/gmt+3",
        "etc/gmt+4",
        "etc/gmt+5",
        "etc/gmt+6",
        "etc/gmt+7",
        "etc/gmt+8",
        "etc/gmt+9",
        "etc/gmt+10",
        "etc/gmt+11",
        "etc/gmt+12",
        "etc/gmt-1",
        "etc/gmt-2",
        "etc/gmt-3",
        "etc/gmt-4",
        "etc/gmt-5",
        "etc/gmt-6",
        "etc/gmt-7",
        "etc/gmt-8",
        "etc/gmt-9",
        "etc/gmt-10",
        "etc/gmt-11",
        "etc/gmt-12",
        "etc/gmt-13",
        "etc/gmt-14",
    ];
    tzs.into_iter().collect()
});

/// Check if a string is a valid timezone name
///
/// # Example
///
/// ```
/// use polyglot_sql::time::is_valid_timezone;
///
/// assert!(is_valid_timezone("America/New_York"));
/// assert!(is_valid_timezone("UTC"));
/// assert!(!is_valid_timezone("Invalid/Timezone"));
/// ```
pub fn is_valid_timezone(tz: &str) -> bool {
    TIMEZONES.contains(tz.to_lowercase().as_str())
}

/// Common format mapping for Python strftime to various SQL dialects
pub mod format_mappings {
    use std::collections::HashMap;

    /// Create a Python strftime to Snowflake format mapping
    pub fn python_to_snowflake() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("%Y", "YYYY");
        m.insert("%y", "YY");
        m.insert("%m", "MM");
        m.insert("%d", "DD");
        m.insert("%H", "HH24");
        m.insert("%I", "HH12");
        m.insert("%M", "MI");
        m.insert("%S", "SS");
        m.insert("%f", "FF6");
        m.insert("%p", "AM");
        m.insert("%j", "DDD");
        m.insert("%W", "WW");
        m.insert("%w", "D");
        m.insert("%b", "MON");
        m.insert("%B", "MONTH");
        m.insert("%a", "DY");
        m.insert("%A", "DAY");
        m.insert("%z", "TZH:TZM");
        m.insert("%Z", "TZR");
        m
    }

    /// Create a Python strftime to BigQuery format mapping
    pub fn python_to_bigquery() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("%Y", "%Y");
        m.insert("%y", "%y");
        m.insert("%m", "%m");
        m.insert("%d", "%d");
        m.insert("%H", "%H");
        m.insert("%I", "%I");
        m.insert("%M", "%M");
        m.insert("%S", "%S");
        m.insert("%f", "%E6S");
        m.insert("%p", "%p");
        m.insert("%j", "%j");
        m.insert("%W", "%W");
        m.insert("%w", "%w");
        m.insert("%b", "%b");
        m.insert("%B", "%B");
        m.insert("%a", "%a");
        m.insert("%A", "%A");
        m.insert("%z", "%z");
        m.insert("%Z", "%Z");
        m
    }

    /// Create a Python strftime to MySQL format mapping
    pub fn python_to_mysql() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("%Y", "%Y");
        m.insert("%y", "%y");
        m.insert("%m", "%m");
        m.insert("%d", "%d");
        m.insert("%H", "%H");
        m.insert("%I", "%h");
        m.insert("%M", "%i");
        m.insert("%S", "%s");
        m.insert("%f", "%f");
        m.insert("%p", "%p");
        m.insert("%j", "%j");
        m.insert("%W", "%U");
        m.insert("%w", "%w");
        m.insert("%b", "%b");
        m.insert("%B", "%M");
        m.insert("%a", "%a");
        m.insert("%A", "%W");
        m
    }

    /// Create a Python strftime to PostgreSQL format mapping
    pub fn python_to_postgres() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("%Y", "YYYY");
        m.insert("%y", "YY");
        m.insert("%m", "MM");
        m.insert("%d", "DD");
        m.insert("%H", "HH24");
        m.insert("%I", "HH12");
        m.insert("%M", "MI");
        m.insert("%S", "SS");
        m.insert("%f", "US");
        m.insert("%p", "AM");
        m.insert("%j", "DDD");
        m.insert("%W", "WW");
        m.insert("%w", "D");
        m.insert("%b", "Mon");
        m.insert("%B", "Month");
        m.insert("%a", "Dy");
        m.insert("%A", "Day");
        m.insert("%z", "OF");
        m.insert("%Z", "TZ");
        m
    }

    /// Create a Python strftime to Oracle format mapping
    pub fn python_to_oracle() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("%Y", "YYYY");
        m.insert("%y", "YY");
        m.insert("%m", "MM");
        m.insert("%d", "DD");
        m.insert("%H", "HH24");
        m.insert("%I", "HH");
        m.insert("%M", "MI");
        m.insert("%S", "SS");
        m.insert("%f", "FF6");
        m.insert("%p", "AM");
        m.insert("%j", "DDD");
        m.insert("%W", "WW");
        m.insert("%w", "D");
        m.insert("%b", "MON");
        m.insert("%B", "MONTH");
        m.insert("%a", "DY");
        m.insert("%A", "DAY");
        m.insert("%z", "TZH:TZM");
        m.insert("%Z", "TZR");
        m
    }

    /// Create a Python strftime to Spark format mapping
    pub fn python_to_spark() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("%Y", "yyyy");
        m.insert("%y", "yy");
        m.insert("%m", "MM");
        m.insert("%d", "dd");
        m.insert("%H", "HH");
        m.insert("%I", "hh");
        m.insert("%M", "mm");
        m.insert("%S", "ss");
        m.insert("%f", "SSSSSS");
        m.insert("%p", "a");
        m.insert("%j", "D");
        m.insert("%W", "w");
        m.insert("%w", "u");
        m.insert("%b", "MMM");
        m.insert("%B", "MMMM");
        m.insert("%a", "E");
        m.insert("%A", "EEEE");
        m.insert("%z", "XXX");
        m.insert("%Z", "z");
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_time_basic() {
        let mut mapping = HashMap::new();
        mapping.insert("%Y", "YYYY");

        let result = format_time("%Y", &mapping, None);
        assert_eq!(result, Some("YYYY".to_string()));
    }

    #[test]
    fn test_format_time_multiple() {
        let mut mapping = HashMap::new();
        mapping.insert("%Y", "YYYY");
        mapping.insert("%m", "MM");
        mapping.insert("%d", "DD");

        let result = format_time("%Y-%m-%d", &mapping, None);
        assert_eq!(result, Some("YYYY-MM-DD".to_string()));
    }

    #[test]
    fn test_format_time_empty() {
        let mapping = HashMap::new();
        assert_eq!(format_time("", &mapping, None), None);
    }

    #[test]
    fn test_format_time_no_mapping() {
        let mapping = HashMap::new();
        let result = format_time("hello", &mapping, None);
        assert_eq!(result, Some("hello".to_string()));
    }

    #[test]
    fn test_format_time_partial_match() {
        let mut mapping = HashMap::new();
        mapping.insert("%Y", "YYYY");
        // %y is not in mapping

        let result = format_time("%Y %y", &mapping, None);
        // %Y matches, %y doesn't match but should pass through
        assert_eq!(result, Some("YYYY %y".to_string()));
    }

    #[test]
    fn test_subsecond_precision_none() {
        assert_eq!(subsecond_precision("2023-01-01 12:13:14"), 0);
    }

    #[test]
    fn test_subsecond_precision_milliseconds() {
        assert_eq!(subsecond_precision("2023-01-01 12:13:14.123"), 3);
        assert_eq!(subsecond_precision("2023-01-01 12:13:14.100"), 3);
    }

    #[test]
    fn test_subsecond_precision_microseconds() {
        assert_eq!(subsecond_precision("2023-01-01 12:13:14.123456"), 6);
        assert_eq!(subsecond_precision("2023-01-01 12:13:14.123456+00:00"), 6);
    }

    #[test]
    fn test_subsecond_precision_with_timezone() {
        assert_eq!(subsecond_precision("2023-01-01 12:13:14.123+00:00"), 3);
        assert_eq!(subsecond_precision("2023-01-01T12:13:14.123456Z"), 6);
    }

    #[test]
    fn test_is_valid_timezone() {
        assert!(is_valid_timezone("UTC"));
        assert!(is_valid_timezone("utc"));
        assert!(is_valid_timezone("America/New_York"));
        assert!(is_valid_timezone("america/new_york"));
        assert!(is_valid_timezone("Europe/London"));
        assert!(!is_valid_timezone("Invalid/Timezone"));
        assert!(!is_valid_timezone("NotATimezone"));
    }

    #[test]
    fn test_python_to_snowflake() {
        let mapping = format_mappings::python_to_snowflake();
        let result = format_time("%Y-%m-%d %H:%M:%S", &mapping, None);
        assert_eq!(result, Some("YYYY-MM-DD HH24:MI:SS".to_string()));
    }

    #[test]
    fn test_python_to_spark() {
        let mapping = format_mappings::python_to_spark();
        let result = format_time("%Y-%m-%d %H:%M:%S", &mapping, None);
        assert_eq!(result, Some("yyyy-MM-dd HH:mm:ss".to_string()));
    }
}
