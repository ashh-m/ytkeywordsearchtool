#!/usr/bin/env python3
"""
YouTube scraper (Apify-compatible)

This version includes:
- Resilient navigation and consent handling.
- Per-type search collection (video/shorts/channel/playlist/live/movie) with per-type caps.
- Canonicalization of collected URLs and defensive filtering of malformed URLs.
- Channel scraping updated to collect both regular videos and shorts from channel pages (reel shelves).
- Forced navigation to canonical shorts URL before extracting shorts metadata to ensure DOM overlay is present.
- Playwright fallback uses player JSON when available to populate metadata fields when API key is not set.

Run with input JSON that can include:
- searchVideoTypes: ["video","shorts"] (or CSV string)
- maxVideosPerTerm, maxShortsPerTerm, maxResults, etc.
- youtubeApiKey (optional, recommended for richer metadata)
"""
import json
import os
import re
import logging
import signal
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urlparse, parse_qs
import time

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, BrowserContext, Page
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import requests

# Optional Apify dataset/KVS support
try:
    from apify_client import ApifyClient
    HAS_APIFY_CLIENT = True
except Exception:
    HAS_APIFY_CLIENT = False

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "") or os.environ.get("youtubeApiKey", "")
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# Global flags / state
STOP_FLAG = False
SAVED_IDS = set()  # dedupe IDs per run


def _signal_handler(signum, frame):
    global STOP_FLAG
    logging.warning("Received signal %s, setting STOP_FLAG", signum)
    STOP_FLAG = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# -----------------------
# YouTube API helpers
# -----------------------
def get_video_details_from_api(video_id: str) -> Optional[Dict[str, Any]]:
    if not YOUTUBE_API_KEY:
        logging.debug("No YouTube API key configured")
        return None
    try:
        url = f"{YOUTUBE_API_BASE}/videos"
        params = {"part": "snippet,statistics,contentDetails,status", "id": video_id, "key": YOUTUBE_API_KEY}
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 403:
            logging.error("YouTube API quota exceeded or invalid key")
            return None
        resp.raise_for_status()
        data = resp.json()
        if not data.get("items"):
            return None
        item = data["items"][0]
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})
        duration_seconds = parse_iso8601_duration(content_details.get("duration", "") or "")
        return {
            "video_id": video_id,
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "channel_id": snippet.get("channelId"),
            "channel_title": snippet.get("channelTitle"),
            "published_at": snippet.get("publishedAt"),
            "thumbnails": snippet.get("thumbnails", {}),
            "view_count": int(statistics.get("viewCount")) if statistics.get("viewCount") else None,
            "like_count": int(statistics.get("likeCount")) if statistics.get("likeCount") else None,
            "comment_count": int(statistics.get("commentCount")) if statistics.get("commentCount") else None,
            "duration_seconds": duration_seconds,
            "tags": snippet.get("tags", []),
            "comments_disabled": not statistics.get("commentCount"),
        }
    except Exception as e:
        logging.debug("YouTube API error: %s", e)
        return None


def get_channel_details_from_api(channel_id: str) -> Optional[Dict[str, Any]]:
    if not YOUTUBE_API_KEY:
        return None
    try:
        url = f"{YOUTUBE_API_BASE}/channels"
        params = {"part": "snippet,statistics,brandingSettings", "id": channel_id, "key": YOUTUBE_API_KEY}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("items"):
            return None
        item = data["items"][0]
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        branding = item.get("brandingSettings", {}).get("channel", {})
        return {
            "channel_id": channel_id,
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "custom_url": snippet.get("customUrl"),
            "published_at": snippet.get("publishedAt"),
            "thumbnails": snippet.get("thumbnails", {}),
            "subscriber_count": int(statistics.get("subscriberCount")) if statistics.get("subscriberCount") else None,
            "keywords": branding.get("keywords", "")
        }
    except Exception as e:
        logging.debug("Channel API error: %s", e)
        return None


def parse_iso8601_duration(duration_str: str) -> Optional[int]:
    if not duration_str:
        return None
    try:
        s = duration_str.replace("PT", "")
        h = m = sec = 0
        if "H" in s:
            parts = s.split("H")
            h = int(parts[0]); s = parts[1]
        if "M" in s:
            parts = s.split("M")
            m = int(parts[0]); s = parts[1]
        if "S" in s:
            sec = int(s.replace("S", ""))
        return h*3600 + m*60 + sec
    except Exception:
        return None


# -----------------------
# Utilities
# -----------------------
def parse_count_text_to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    try:
        t = str(text).lower().strip().replace(",", "")
        t = re.sub(r'[^\x00-\x7F]+', '', t)
        m = re.search(r'([\d\.]+)\s*(k|m|b)?', t)
        if not m:
            d = re.sub(r'[^0-9]', '', t)
            return int(d) if d else None
        num = float(m.group(1)); suf = m.group(2)
        if suf == 'k': return int(num*1_000)
        if suf == 'm': return int(num*1_000_000)
        if suf == 'b': return int(num*1_000_000_000)
        return int(num)
    except Exception:
        return None


def seconds_to_hms(seconds: Optional[int]) -> Optional[str]:
    if seconds is None:
        return None
    try:
        s = int(seconds)
        h = s // 3600; m = (s % 3600) // 60; sec = s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}" if h > 0 else f"{m:02d}:{sec:02d}"
    except Exception:
        return None


def extract_hashtags_from_text(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return re.findall(r"#(\w+)", text)


# -----------------------
# Storage helpers (dedupe)
# -----------------------
def save_dataset(items: List[Dict[str, Any]]):
    global SAVED_IDS
    if not items:
        return
    new_items = []
    for it in items:
        vid = it.get("id") or it.get("video_id")
        if not vid:
            new_items.append(it)
            continue
        if vid in SAVED_IDS:
            logging.debug("Skipping already-saved id=%s", vid)
            continue
        SAVED_IDS.add(vid)
        new_items.append(it)
    if not new_items:
        return
    ds_id = os.getenv("APIFY_DEFAULT_DATASET_ID")
    token = os.getenv("APIFY_TOKEN")
    api_base = os.getenv("APIFY_API_BASE_URL")
    if HAS_APIFY_CLIENT and ds_id and token:
        try:
            client = ApifyClient(token, api_url=api_base) if api_base else ApifyClient(token)
            dataset = client.dataset(ds_id)
            CHUNK = 100
            for i in range(0, len(new_items), CHUNK):
                dataset.push_items(new_items[i:i + CHUNK])
            logging.info("Pushed %d new items to dataset %s", len(new_items), ds_id)
            return
        except Exception as e:
            logging.warning("Apify client push failed, falling back to local file: %s", e)
    # Local fallback: append to a local NDJSON
    dataset_path = os.environ.get("APIFY_DATASET_PATH", "./dataset.ndjson")
    try:
        with open(dataset_path, "a", encoding="utf-8") as f:
            for it in new_items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
        logging.info("Appended %d new items to %s", len(new_items), dataset_path)
    except Exception as e:
        logging.error("Failed to write local dataset: %s", e)


# -----------------------
# Navigation / availability
# -----------------------
def handle_consent_aggressive(page: Page) -> bool:
    selectors = [
        'button:has-text("Accept all")', 'button:has-text("I agree")', 'button:has-text("Agree")',
        'button[aria-label*="Accept"]', 'button[aria-label*="Agree"]', 'button#introAgreeButton'
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el and el.is_visible(timeout=1200):
                try:
                    el.click(timeout=3000)
                    page.wait_for_timeout(700)
                    logging.info("Clicked consent selector: %s", sel)
                    return True
                except Exception:
                    continue
        except Exception:
            continue
    # try frames
    try:
        for frame in page.frames:
            for sel in selectors:
                try:
                    el = frame.locator(sel).first
                    if el and el.is_visible(timeout=1000):
                        el.click(timeout=2000)
                        logging.info("Clicked consent in frame: %s", sel)
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def is_page_unavailable(page: Page) -> bool:
    """Detect the YouTube 'This page isn't available' screen or similar."""
    try:
        if page.locator("ytd-page-not-found-renderer").count() > 0:
            return True
        body_text = page.locator("body").inner_text(timeout=2000)
        if body_text:
            lowered = body_text.lower()
            if "this page isn't available" in lowered or "sorry about that" in lowered or "channel is not available" in lowered:
                return True
        content = page.content() or ""
        if "page not found" in content.lower() or "channel unavailable" in content.lower():
            return True
    except Exception:
        return False
    return False


@retry(wait=wait_exponential(multiplier=1, min=2, max=8), stop=stop_after_attempt(1), retry=retry_if_exception_type(Exception))
def goto_and_ready(page: Page, url: str):
    """
    Resilient navigation:
    - Try ytInitial* wait
    - On timeout, try fallback selectors, document.readyState, HTML scan
    - Detect explicit unavailable page and return early
    - Capture screenshot for diagnostics, but do NOT raise on missing init data
    """
    global STOP_FLAG
    logging.info("Navigating to: %s", url)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45_000)
    except Exception as e:
        logging.warning("page.goto failed for %s: %s", url, e)

    if STOP_FLAG:
        raise Exception("Stop requested")

    try:
        handle_consent_aggressive(page)
    except Exception:
        logging.debug("Consent handling failed")

    # Early unavailable check
    try:
        if is_page_unavailable(page):
            logging.warning("Detected YouTube unavailable page for %s", url)
            try_capture(page, key=f"CHANNEL_UNAVAILABLE_{int(time.time())}.png")
            return
    except Exception:
        pass

    # Primary wait for ytInitial*
    try:
        page.wait_for_function(
            "() => Boolean(window.ytInitialPlayerResponse) || Boolean(window.ytInitialData)",
            timeout=20_000
        )
        logging.info("Found ytInitial* data on %s", url)
        return
    except Exception:
        logging.debug("ytInitial* wait timed out, trying fallbacks for %s", url)

    # Fallback selectors
    fallback_selectors = [
        "ytd-browse", "ytd-rich-grid-renderer", "ytd-rich-item-renderer", "#contents ytd-rich-item-renderer",
        "ytd-channel-name", "#items ytd-grid-video-renderer", "ytd-section-list-renderer", "ytd-reel-shelf-renderer"
    ]
    for sel in fallback_selectors:
        try:
            el = page.locator(sel).first
            if el and el.is_visible(timeout=3000):
                logging.info("Found fallback selector '%s' on %s", sel, url)
                return
        except Exception:
            continue

    # document ready + recheck
    try:
        page.wait_for_timeout(1000)
        ready = page.evaluate("() => document.readyState") or ""
        if ready.lower() in ("complete", "interactive"):
            has_init = page.evaluate("() => Boolean(window.ytInitialData) || Boolean(window.ytInitialPlayerResponse)")
            if has_init:
                logging.info("Document ready and ytInitial* present on %s", url)
                return
    except Exception:
        logging.debug("Document.ready fallback failed for %s", url)

    # HTML keyword scan
    try:
        html = page.content() or ""
        if "/shorts/" in html or "watch?v=" in html or "ytd-rich-grid-renderer" in html:
            logging.info("Found keywords in HTML for %s; proceeding without ytInitial*", url)
            return
    except Exception:
        logging.debug("HTML scan failed for %s", url)

    # final unavailable re-check (after attempts)
    try:
        if is_page_unavailable(page):
            logging.warning("Detected YouTube unavailable page for %s (post-fallback)", url)
            try_capture(page, key=f"CHANNEL_UNAVAILABLE_{int(time.time())}.png")
            return
    except Exception:
        pass

    # capture screenshot for debugging and continue (do not raise)
    try:
        try_capture(page, key=f"NO_INITDATA_{int(time.time())}.png")
    except Exception:
        logging.debug("Screenshot capture failed")
    logging.warning("Could not detect ytInitial* or fallback selectors on %s — continuing", url)
    return


# -----------------------
# YouTube helpers
# -----------------------
def clean_video_url(url: str) -> str:
    """
    Return canonical URL to navigate to:
     - watch?v=VIDEOID (strip extraneous params except v)
     - shorts/VIDEOID preserved
     - youtu.be/VIDEOID converted to watch URL for extraction
    """
    if not url:
        return url
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
        # shorts
        m = re.search(r"/shorts/([A-Za-z0-9_-]{11})", path)
        if m:
            return f"https://www.youtube.com/shorts/{m.group(1)}"
        # youtu.be shortlink
        if parsed.netloc and "youtu.be" in parsed.netloc:
            vid = path.strip("/").split("/")[0]
            if vid:
                return f"https://www.youtube.com/watch?v={vid}"
        # watch URL: keep only v param
        if "watch" in path and parsed.query:
            qs = parse_qs(parsed.query)
            if "v" in qs and qs["v"]:
                return f"https://www.youtube.com/watch?v={qs['v'][0]}"
        # channel or other pages: return original cleaned of fragment
        return url.split("#")[0]
    except Exception:
        return url.split("#")[0]


def extract_video_id(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        if parsed.netloc and "youtu.be" in parsed.netloc:
            return parsed.path.strip("/").split("/")[0]
        if "/watch" in parsed.path and parsed.query:
            qs = parse_qs(parsed.query)
            if "v" in qs: return qs["v"][0]
        m = re.search(r"/shorts/([A-Za-z0-9_-]{11})", parsed.path or "")
        if m: return m.group(1)
    except Exception:
        return None
    return None


def is_shorts_url(url: str) -> bool:
    return "/shorts/" in (url or "").lower()


def is_video_url(url: str) -> bool:
    u = (url or "").lower()
    return ("watch?v=" in u) or ("/shorts/" in u) or ("youtu.be/" in u)


def detect_content_type(url: str, page: Page = None) -> str:
    """
    Detect content type: 'short' or 'video'. Use URL check first, then page content heuristics.
    """
    if is_shorts_url(url):
        return "short"
    if page:
        try:
            c = page.content()
            if 'ytd-reel-video-renderer' in c or 'shorts' in (page.url or ""):
                return "short"
        except Exception:
            pass
    return "video"


def try_get_initial_data(page: Page) -> Dict[str, Any]:
    try:
        return page.evaluate("() => window.ytInitialData || {}") or {}
    except Exception:
        return {}


def try_get_player_json(page: Page) -> Dict[str, Any]:
    for expr in [
        "window.ytInitialPlayerResponse || null",
        "window.ytplayer && window.ytplayer.config && window.ytplayer.config.args && JSON.parse(window.ytplayer.config.args.player_response) || null"
    ]:
        try:
            data = page.evaluate(f"() => {expr}")
            if data:
                return data
        except Exception:
            continue
    return {}


def _text_from(obj: Any) -> Optional[str]:
    if obj is None: return None
    if isinstance(obj, str): return obj.strip() or None
    if isinstance(obj, dict):
        if "simpleText" in obj and isinstance(obj["simpleText"], str): return obj["simpleText"].strip() or None
        if "runs" in obj and isinstance(obj["runs"], list): return "".join(r.get("text", "") for r in obj["runs"] if "text" in r).strip() or None
    return str(obj).strip() if obj else None


def find_nested_key(data: Any, key: str) -> Any:
    if isinstance(data, dict):
        if key in data: return data[key]
        for v in data.values():
            found = find_nested_key(v, key)
            if found is not None: return found
    elif isinstance(data, list):
        for item in data:
            found = find_nested_key(item, key)
            if found is not None: return found
    return None


# -----------------------
# Subtitles & Shorts extractor
# -----------------------
def extract_subtitles(page: Page) -> Optional[List[Dict[str, Any]]]:
    try:
        page.locator("#description-inline-expander").click(timeout=3000)
        page.wait_for_timeout(200)
        page.locator('#info-container > #menu button[aria-label="More actions"]').click(timeout=3000)
        page.wait_for_timeout(200)
        page.locator("ytd-menu-service-item-renderer a:has-text('Show transcript')").click(timeout=3000)
        page.wait_for_selector("ytd-transcript-segment-renderer", timeout=5000)
        segments = page.query_selector_all("ytd-transcript-segment-renderer")
        out = []
        for seg in segments:
            try:
                ts = seg.query_selector(".segment-timestamp").inner_text()
                txt = seg.query_selector(".segment-text").inner_text()
                out.append({"timestamp": ts, "text": txt})
            except Exception:
                continue
        return out
    except Exception as e:
        logging.debug("Subtitles not available: %s", e)
        return None


def extract_shorts_metadata(video_id: str, url: str, page: Page) -> Dict[str, Any]:
    """
    Extract metadata for a YouTube Short. Uses API first if available, then falls back to Playwright extraction.
    """
    logging.info("Extracting metadata for SHORT: %s", video_id)
    
    # Fix 4: Try API first for shorts (same as regular videos)
    api_data = get_video_details_from_api(video_id)
    if api_data:
        logging.info("Using YouTube API for shorts metadata: %s", video_id)
        thumbnails = api_data.get("thumbnails", {})
        thumbnail = thumbnails.get("maxres", {}).get("url") or thumbnails.get("high", {}).get("url") or f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"
        return {
            "video_id": video_id,
            "title": api_data.get("title"),
            "description": api_data.get("description"),
            "video_view_count": api_data.get("view_count"),
            "upload_date_iso": api_data.get("published_at"),
            "duration_seconds": api_data.get("duration_seconds"),
            "duration_text": seconds_to_hms(api_data.get("duration_seconds")),
            "thumbnail_url": thumbnail,
            "like_count": api_data.get("like_count"),
            "comments_count": api_data.get("comment_count"),
            "comments_off": api_data.get("comments_disabled", False),
            "channel_id": api_data.get("channel_id"),
            "channel_name": api_data.get("channel_title"),
            "channel_url": f"https://www.youtube.com/channel/{api_data.get('channel_id')}" if api_data.get("channel_id") else None,
            "video_url": url,
            "hashtags": extract_hashtags_from_text(api_data.get("description")),
            "content_type": "short",
            "data_source": "youtube_api"
        }
    
    # Fall back to Playwright extraction
    logging.info("Falling back to Playwright extraction for shorts: %s", video_id)
    try:
        initial = try_get_initial_data(page)
        player_json = try_get_player_json(page)
        overlay = None
        if initial:
            overlay = find_nested_key(initial, "reelPlayerOverlayRenderer") or find_nested_key(initial, "shortsPlayerOverlayRenderer")
        title = None; view_count = None; like_count = None; channel_name = None; channel_id = None; description = None
        channel_username = None; subscriber_count = None; upload_date_iso = None; comments_count = None
        
        # Try to get data from player_json.videoDetails first (most reliable)
        if player_json and isinstance(player_json, dict):
            vd = player_json.get("videoDetails", {}) or {}
            if vd:
                title = title or vd.get("title")
                description = description or vd.get("shortDescription") or vd.get("description")
                view_count = view_count or (int(vd.get("viewCount")) if vd.get("viewCount") and str(vd.get("viewCount")).isdigit() else parse_count_text_to_int(vd.get("viewCount")))
                channel_name = channel_name or vd.get("author")
                channel_id = channel_id or vd.get("channelId")
            
            # Fix 1: Extract date from microformat (multiple sources)
            microformat = player_json.get("microformat", {}).get("playerMicroformatRenderer", {}) or {}
            if microformat:
                upload_date_iso = upload_date_iso or microformat.get("publishDate") or microformat.get("uploadDate")
        
        if overlay:
            title = title or _text_from(overlay.get("reelTitleText") or overlay.get("shortsTitleText"))
            view_count = view_count or parse_count_text_to_int(_text_from(find_nested_key(overlay, "viewCountText")))
            like_count = like_count or parse_count_text_to_int(_text_from(find_nested_key(overlay, "likeButton")))
            ch = find_nested_key(overlay, "navigationEndpoint") or find_nested_key(overlay, "videoOwner")
            if ch and isinstance(ch, dict):
                channel_id = channel_id or ch.get("browseEndpoint", {}).get("browseId")
            channel_name = channel_name or _text_from(find_nested_key(overlay, "channelTitleText"))
        
        # Fix 1: Improved structured data extraction - handle both single object and array formats
        if not title or not upload_date_iso or not channel_name:
            try:
                ld_data = page.evaluate("""() => {
                    const ldJsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const script of ldJsonScripts) {
                        try {
                            const data = JSON.parse(script.textContent);
                            // Handle both single object and array formats
                            const items = Array.isArray(data) ? data : [data];
                            for (const item of items) {
                                if (item['@type'] === 'VideoObject') {
                                    return {
                                        name: item.name || null,
                                        description: item.description || null,
                                        uploadDate: item.uploadDate || item.datePublished || null,
                                        author: item.author ? (item.author.name || null) : null,
                                        authorUrl: item.author ? (item.author.url || null) : null,
                                        interactionCount: item.interactionStatistic ? 
                                            (Array.isArray(item.interactionStatistic) ? 
                                                item.interactionStatistic.find(s => s.interactionType && s.interactionType.includes('Watch'))?.userInteractionCount : 
                                                item.interactionStatistic.userInteractionCount) : null
                                    };
                                }
                            }
                        } catch(e) {}
                    }
                    return null;
                }""")
                if ld_data and isinstance(ld_data, dict):
                    title = title or ld_data.get('name')
                    description = description or ld_data.get('description')
                    upload_date_iso = upload_date_iso or ld_data.get('uploadDate')
                    if ld_data.get('author') and ld_data['author'].lower() not in ['shopping', 'youtube']:
                        channel_name = channel_name or ld_data['author']
                    if ld_data.get('authorUrl'):
                        if '/@' in ld_data['authorUrl']:
                            match = re.search(r'/@([A-Za-z0-9_.-]+)', ld_data['authorUrl'])
                            if match:
                                channel_username = channel_username or match.group(1)
                        if '/channel/' in ld_data['authorUrl']:
                            match = re.search(r'/channel/([A-Za-z0-9_-]+)', ld_data['authorUrl'])
                            if match:
                                channel_id = channel_id or match.group(1)
                    if ld_data.get('interactionCount'):
                        view_count = view_count or parse_count_text_to_int(str(ld_data['interactionCount']))
            except Exception:
                pass
        
        # Fallback: Try og:title meta tag and page title
        if not title:
            try:
                title = page.evaluate("""() => {
                    // Try og:title meta tag
                    const ogTitle = document.querySelector('meta[property="og:title"]');
                    if (ogTitle) {
                        const content = ogTitle.getAttribute('content');
                        if (content && content !== 'YouTube') return content;
                    }
                    // Try meta name title
                    const metaTitle = document.querySelector('meta[name="title"]');
                    if (metaTitle) {
                        const content = metaTitle.getAttribute('content');
                        if (content && content !== 'YouTube') return content;
                    }
                    // Last resort: page title but filter out just "YouTube"
                    const pageTitle = document.title;
                    if (pageTitle && pageTitle !== 'YouTube' && !pageTitle.match(/^YouTube\\s*$/)) {
                        return pageTitle.replace(/ - YouTube$/, '');
                    }
                    return null;
                }""")
            except Exception:
                title = None
        
        # Fix 1: Extract date from meta tags if still missing
        if not upload_date_iso:
            try:
                upload_date_iso = page.evaluate("""() => {
                    // Try meta itemprop uploadDate or datePublished
                    const uploadMeta = document.querySelector('meta[itemprop="uploadDate"], meta[itemprop="datePublished"]');
                    if (uploadMeta) {
                        const content = uploadMeta.getAttribute('content');
                        if (content) return content;
                    }
                    // Try og:video:release_date
                    const releaseMeta = document.querySelector('meta[property="og:video:release_date"]');
                    if (releaseMeta) {
                        const content = releaseMeta.getAttribute('content');
                        if (content) return content;
                    }
                    return null;
                }""")
            except Exception:
                pass
        
        if not view_count:
            try:
                # First try #metadata-line which is more reliable
                v = page.evaluate("""() => {
                    const metaLine = document.querySelector('#metadata-line');
                    if (metaLine) {
                        const text = metaLine.innerText || metaLine.textContent || '';
                        const match = text.match(/([\\d,\\.]+[KMB]?)\\s*views?/i);
                        if (match) return match[0];
                    }
                    // Fallback: look for aria-label with digits AND the word "view"
                    const el = [...document.querySelectorAll('span,div')].find(n => {
                        const label = n.getAttribute && n.getAttribute('aria-label');
                        // Must contain digits and the word "view" to avoid "Shopping" or other buttons
                        return label && /\\d/.test(label) && /views?/i.test(label);
                    });
                    return el ? el.getAttribute('aria-label') : null;
                }""")
                view_count = parse_count_text_to_int(v)
            except Exception:
                view_count = None
        
        # Extract like_count from DOM if still missing (Shorts use a different UI)
        if not like_count:
            try:
                like_text = page.evaluate("""() => {
                    // Shorts like button - look for aria-label containing "like" and a number
                    const buttons = document.querySelectorAll('button[aria-label]');
                    for (const btn of buttons) {
                        const label = btn.getAttribute('aria-label') || '';
                        // Must contain "like" (case insensitive) and have digits, but not "dislike"
                        if (/like/i.test(label) && !/dislike/i.test(label) && /\\d/.test(label)) {
                            const match = label.match(/([\\d,\\.]+[KMB]?)/i);
                            if (match) return match[1];
                        }
                    }
                    // Try text inside like button
                    const likeCount = document.querySelector('#like-button span, ytd-toggle-button-renderer span');
                    if (likeCount) {
                        const text = likeCount.innerText || likeCount.textContent || '';
                        if (/\\d/.test(text)) return text;
                    }
                    return null;
                }""")
                like_count = parse_count_text_to_int(like_text)
            except Exception:
                pass
        
        # Extract channel info - be very specific to avoid "Shopping" or other buttons
        if not channel_name or channel_name.lower() == 'shopping':
            try:
                channel_info = page.evaluate("""() => {
                    // Look specifically in the shorts player overlay for channel info
                    // The channel link should be in the video owner section, not in the sidebar
                    const ownerLinks = document.querySelectorAll('ytd-channel-name a, .ytd-reel-player-overlay-renderer a[href*="/@"], .ytd-reel-player-overlay-renderer a[href*="/channel/"]');
                    for (const link of ownerLinks) {
                        const text = (link.innerText || link.textContent || '').trim();
                        // Skip if it's "Shopping" or empty
                        if (text && text.toLowerCase() !== 'shopping' && text.toLowerCase() !== 'youtube') {
                            return {name: text, href: link.href};
                        }
                    }
                    // Try structured data - handle array format
                    const ldJsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const script of ldJsonScripts) {
                        try {
                            const data = JSON.parse(script.textContent);
                            const items = Array.isArray(data) ? data : [data];
                            for (const item of items) {
                                if (item.author && item.author.name) {
                                    return {name: item.author.name, href: item.author.url || null};
                                }
                            }
                        } catch(e) {}
                    }
                    // Try og:site_name or author meta
                    const authorMeta = document.querySelector('meta[name="author"]');
                    if (authorMeta) {
                        const content = authorMeta.getAttribute('content');
                        if (content && content.toLowerCase() !== 'shopping' && content.toLowerCase() !== 'youtube') {
                            return {name: content, href: null};
                        }
                    }
                    return null;
                }""")
                if channel_info and isinstance(channel_info, dict):
                    if channel_info.get('name') and channel_info['name'].lower() not in ['shopping', 'youtube']:
                        channel_name = channel_info['name']
                    if channel_info.get('href'):
                        # Extract channel_id and username from href
                        href = channel_info['href']
                        if '/channel/' in href:
                            match = re.search(r'/channel/([A-Za-z0-9_-]+)', href)
                            if match:
                                channel_id = channel_id or match.group(1)
                        if '/@' in href:
                            match = re.search(r'/@([A-Za-z0-9_.-]+)', href)
                            if match:
                                channel_username = channel_username or match.group(1)
            except Exception:
                pass
        
        # Extract channel_username (handle like @username)
        if not channel_username:
            try:
                channel_username = page.evaluate("""() => {
                    // Look for channel link with /@username pattern - be specific
                    const handleLinks = document.querySelectorAll('ytd-channel-name a[href*="/@"], .ytd-reel-player-overlay-renderer a[href*="/@"]');
                    for (const link of handleLinks) {
                        const match = link.href.match(/@([A-Za-z0-9_.-]+)/);
                        if (match) return match[1];
                    }
                    // Try structured data - handle array format
                    const ldJsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const script of ldJsonScripts) {
                        try {
                            const data = JSON.parse(script.textContent);
                            const items = Array.isArray(data) ? data : [data];
                            for (const item of items) {
                                if (item.author && item.author.url) {
                                    const match = item.author.url.match(/@([A-Za-z0-9_.-]+)/);
                                    if (match) return match[1];
                                }
                            }
                        } catch(e) {}
                    }
                    return null;
                }""")
            except Exception:
                pass
        
        # Extract subscriber_count from DOM
        try:
            sub_text = page.evaluate("""() => {
                // Try subscriber count in shorts overlay
                const subCount = document.querySelector('ytd-reel-video-renderer #subscriber-count, #owner-sub-count, .ytd-reel-player-overlay-renderer #subscriber-count');
                if (subCount) return subCount.innerText || subCount.textContent;
                return null;
            }""")
            subscriber_count = parse_count_text_to_int(sub_text)
        except Exception:
            pass
        
        # Fix 1: Extract comment count for shorts
        try:
            comments_text = page.evaluate("""() => {
                // Try comments entry point header
                const commentsSection = document.querySelector('ytd-comments-entry-point-header-renderer');
                if (commentsSection) {
                    const countEl = commentsSection.querySelector('#vote-count-middle, #vote-count-left, yt-formatted-string');
                    if (countEl) {
                        const text = countEl.innerText || countEl.textContent || '';
                        if (/\\d/.test(text)) return text;
                    }
                }
                // Try comments button in shorts overlay
                const commentsBtn = document.querySelector('ytd-reel-video-renderer [aria-label*="comment"], button[aria-label*="comment"]');
                if (commentsBtn) {
                    const label = commentsBtn.getAttribute('aria-label') || '';
                    const match = label.match(/([\\d,\\.]+[KMB]?)/i);
                    if (match) return match[1];
                }
                return null;
            }""")
            comments_count = parse_count_text_to_int(comments_text)
        except Exception:
            pass
        
        # Get description from structured data or meta
        if not description:
            try:
                description = page.evaluate("""() => {
                    // Try structured data first - handle array format
                    const ldJsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const script of ldJsonScripts) {
                        try {
                            const data = JSON.parse(script.textContent);
                            const items = Array.isArray(data) ? data : [data];
                            for (const item of items) {
                                if (item.description) return item.description;
                            }
                        } catch(e) {}
                    }
                    // Try og:description
                    const ogDesc = document.querySelector('meta[property="og:description"]');
                    if (ogDesc) {
                        const content = ogDesc.getAttribute('content');
                        // Filter out generic YouTube description
                        if (content && !content.startsWith('Enjoy the videos and music')) return content;
                    }
                    // Try meta description
                    const metaDesc = document.querySelector('meta[name="description"]');
                    if (metaDesc) {
                        const content = metaDesc.getAttribute('content');
                        if (content && !content.startsWith('Enjoy the videos and music')) return content;
                    }
                    return null;
                }""")
            except Exception:
                pass
        
        hashtags = extract_hashtags_from_text(description)
        return {
            "video_id": video_id, "title": title, "description": description,
            "video_view_count": view_count, "like_count": like_count,
            "upload_date_iso": upload_date_iso, "comments_count": comments_count,
            "channel_id": channel_id, "channel_name": channel_name,
            "channel_username": channel_username, "subscriber_count": subscriber_count,
            "channel_url": f"https://www.youtube.com/channel/{channel_id}" if channel_id else None,
            "video_url": url, "hashtags": hashtags, "content_type": "short", "data_source": "playwright_shorts"
        }
    except Exception as e:
        logging.error("Shorts extractor failed: %s", e, exc_info=True)
        return {"video_id": video_id, "video_url": url, "data_source": "error"}


# -----------------------
# Hybrid video metadata extractor
# -----------------------
def extract_video_metadata_hybrid(video_id: str, url: str, page: Page = None) -> Dict[str, Any]:
    """
    Use API if available, otherwise use Playwright. Playwright fallback prefers player_json.videoDetails
    when present (watch page), then falls back to initialData renderers.
    """
    api_data = get_video_details_from_api(video_id)
    if api_data:
        thumbnails = api_data.get("thumbnails", {})
        thumbnail = thumbnails.get("maxres", {}).get("url") or thumbnails.get("high", {}).get("url") or f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"
        return {
            "video_id": video_id, "title": api_data.get("title"), "description": api_data.get("description"),
            "video_view_count": api_data.get("view_count"), "upload_date_iso": api_data.get("published_at"),
            "duration_seconds": api_data.get("duration_seconds"), "duration_text": seconds_to_hms(api_data.get("duration_seconds")),
            "thumbnail_url": thumbnail, "like_count": api_data.get("like_count"), "comments_count": api_data.get("comment_count"),
            "comments_off": api_data.get("comments_disabled", False), "channel_id": api_data.get("channel_id"),
            "channel_name": api_data.get("channel_title"), "channel_url": f"https://www.youtube.com/channel/{api_data.get('channel_id')}" if api_data.get("channel_id") else None,
            "video_url": url, "hashtags": extract_hashtags_from_text(api_data.get("description")), "content_type": detect_content_type(url),
            "data_source": "youtube_api"
        }

    logging.warning("API data not used for %s; using Playwright", video_id)
    if not page:
        return {"video_id": video_id, "video_url": url, "data_source": "none"}

    # Use shorts extractor for shorts URLs
    if is_shorts_url(url):
        return extract_shorts_metadata(video_id, url, page)

    # Playwright fallback: try player JSON first, then initialData renderers
    try:
        initial_data = try_get_initial_data(page)
        player_json = try_get_player_json(page)

        title = description = upload_date_iso = None
        duration_seconds = None
        channel_id = channel_name = None
        view_count = like_count = comments_count = None

        # Prefer player_json.videoDetails if present — it's reliable for watch pages
        if player_json and isinstance(player_json, dict):
            vd = player_json.get("videoDetails", {}) or {}
            if vd:
                title = title or vd.get("title")
                description = description or vd.get("shortDescription") or vd.get("description")
                try:
                    duration_seconds = duration_seconds or (int(vd.get("lengthSeconds")) if vd.get("lengthSeconds") else None)
                except Exception:
                    duration_seconds = duration_seconds or None
                # viewCount may be string
                view_count = view_count or (int(vd.get("viewCount")) if vd.get("viewCount") and str(vd.get("viewCount")).isdigit() else parse_count_text_to_int(vd.get("viewCount")))
                channel_name = channel_name or vd.get("author")
                channel_id = channel_id or vd.get("channelId")

        # If title still missing, use initial_data path (videoPrimaryInfoRenderer)
        primary_info = find_nested_key(initial_data, "videoPrimaryInfoRenderer")
        if primary_info:
            title = title or _text_from(primary_info.get("title"))
            upload_date_iso = upload_date_iso or _text_from(primary_info.get("dateText"))
            vc = find_nested_key(primary_info, "videoViewCountRenderer")
            if vc and "viewCount" in vc:
                view_count = view_count or parse_count_text_to_int(_text_from(vc.get("viewCount")))

        secondary_info = find_nested_key(initial_data, "videoSecondaryInfoRenderer")
        if secondary_info:
            description = description or _text_from(secondary_info.get("description"))
            like_button = find_nested_key(secondary_info, "likeButton")
            if like_button:
                lr = find_nested_key(like_button, "toggleButtonRenderer")
                if lr:
                    like_count = like_count or parse_count_text_to_int(_text_from(lr.get("defaultText")))

        owner_info = find_nested_key(initial_data, "videoOwnerRenderer")
        if owner_info:
            channel_name = channel_name or _text_from(owner_info.get("title"))
            channel_id_nav = find_nested_key(owner_info, "navigationEndpoint")
            if channel_id_nav:
                channel_id = channel_id or channel_id_nav.get("browseEndpoint", {}).get("browseId")

        # DOM fallback for missing fields when ytInitialPlayerResponse is missing
        # Extract uploadDate from #info-strings if not available
        if not upload_date_iso:
            try:
                date_text = page.evaluate("""() => {
                    // Try structured data (ld+json) - iterate through all scripts to find video data
                    const ldJsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const ldJson of ldJsonScripts) {
                        try {
                            const data = JSON.parse(ldJson.textContent);
                            // Check if this is video data (has @type VideoObject or uploadDate)
                            if (data.uploadDate) return data.uploadDate;
                            if (data.datePublished) return data.datePublished;
                        } catch(e) {}
                    }
                    // Try #info-strings (common location for upload date)
                    const infoStrings = document.querySelector('#info-strings yt-formatted-string');
                    if (infoStrings) return infoStrings.innerText || infoStrings.textContent;
                    // Try publish date meta tag
                    const publishDate = document.querySelector('meta[itemprop="uploadDate"], meta[itemprop="datePublished"]');
                    if (publishDate) {
                        const content = publishDate.getAttribute('content');
                        if (content) return content;
                    }
                    // Fallback: look for date patterns in info container
                    const info = document.querySelector('#info-container #info, #info');
                    if (info) {
                        const text = info.innerText || info.textContent || '';
                        const dateMatch = text.match(/(\\w+\\s+\\d{1,2},\\s+\\d{4}|\\d{1,2}\\s+\\w+\\s+\\d{4})/);
                        if (dateMatch) return dateMatch[0];
                    }
                    return null;
                }""")
                upload_date_iso = date_text or upload_date_iso
            except Exception:
                pass

        # Extract channel_id from channel link href if still missing
        if not channel_id:
            try:
                channel_id_from_href = page.evaluate("""() => {
                    // Try structured data (ld+json) - iterate through all scripts
                    const ldJsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const ldJson of ldJsonScripts) {
                        try {
                            const data = JSON.parse(ldJson.textContent);
                            if (data.author && data.author.url) {
                                const channelMatch = data.author.url.match(/\\/channel\\/([A-Za-z0-9_-]+)/);
                                if (channelMatch) return channelMatch[1];
                            }
                        } catch(e) {}
                    }
                    // Look for channel link with /channel/CHANNELID pattern
                    const channelLink = document.querySelector('a[href*="/channel/"]');
                    if (channelLink) {
                        const match = channelLink.href.match(/\\/channel\\/([A-Za-z0-9_-]+)/);
                        if (match) return match[1];
                    }
                    // Also check owner links
                    const ownerLink = document.querySelector('#owner a[href*="/channel/"], ytd-video-owner-renderer a[href*="/channel/"]');
                    if (ownerLink) {
                        const match = ownerLink.href.match(/\\/channel\\/([A-Za-z0-9_-]+)/);
                        if (match) return match[1];
                    }
                    return null;
                }""")
                channel_id = channel_id_from_href or channel_id
            except Exception:
                pass

        # Extract channel_username (handle like @username) from DOM
        channel_username = None
        try:
            channel_username = page.evaluate("""() => {
                // Look for channel link with /@username pattern
                const handleLink = document.querySelector('#owner a[href*="/@"], ytd-video-owner-renderer a[href*="/@"], a.yt-simple-endpoint[href*="/@"]');
                if (handleLink) {
                    const match = handleLink.href.match(/@([A-Za-z0-9_.-]+)/);
                    if (match) return match[1];
                }
                // Check for custom URL displayed in owner area
                const channelHandle = document.querySelector('#channel-handle, ytd-channel-name #text');
                if (channelHandle) {
                    const text = channelHandle.innerText || channelHandle.textContent || '';
                    const match = text.match(/@([A-Za-z0-9_.-]+)/);
                    if (match) return match[1];
                }
                return null;
            }""")
        except Exception:
            pass

        # Extract duration from DOM if still missing
        if not duration_seconds:
            try:
                duration_text_dom = page.evaluate("""() => {
                    // Try duration badge on video page
                    const badge = document.querySelector('.ytp-time-duration');
                    if (badge) return badge.innerText || badge.textContent;
                    // Try video duration in structured data
                    const ld = document.querySelector('script[type="application/ld+json"]');
                    if (ld) {
                        try {
                            const data = JSON.parse(ld.textContent);
                            if (data.duration) return data.duration;
                        } catch(e) {}
                    }
                    return null;
                }""")
                if duration_text_dom:
                    # Parse duration text like "10:30" or "1:02:45" or ISO 8601 like "PT10M30S"
                    if duration_text_dom.startswith('PT'):
                        duration_seconds = parse_iso8601_duration(duration_text_dom)
                    else:
                        parts = duration_text_dom.split(':')
                        if len(parts) == 2:
                            duration_seconds = int(parts[0]) * 60 + int(parts[1])
                        elif len(parts) == 3:
                            duration_seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            except Exception:
                pass

        # Extract like_count from DOM if still missing (YouTube's new like button format)
        if not like_count:
            try:
                like_text = page.evaluate("""() => {
                    // New YouTube like button format
                    const likeBtn = document.querySelector('ytd-menu-renderer button[aria-label*="like"], like-button-view-model button, ytd-toggle-button-renderer[is-icon-button] button[aria-label*="like"]');
                    if (likeBtn) {
                        const label = likeBtn.getAttribute('aria-label');
                        if (label) {
                            const match = label.match(/([\\d,\\.]+[KMB]?)/i);
                            if (match) return match[1];
                        }
                    }
                    // Try segmented like button
                    const segmented = document.querySelector('ytd-segmented-like-dislike-button-renderer');
                    if (segmented) {
                        const text = segmented.innerText || '';
                        const match = text.match(/([\\d,\\.]+[KMB]?)/i);
                        if (match) return match[1];
                    }
                    // Try like count span
                    const likeCount = document.querySelector('#segmented-like-button span, .YtLikeButtonViewModelHost span');
                    if (likeCount) return likeCount.innerText || likeCount.textContent;
                    return null;
                }""")
                like_count = parse_count_text_to_int(like_text)
            except Exception:
                pass

        # Extract subscriber_count from DOM (displayed in owner section)
        subscriber_count = None
        try:
            sub_text = page.evaluate("""() => {
                // Try owner subscriber count
                const subCount = document.querySelector('#owner-sub-count, ytd-video-owner-renderer #owner-sub-count');
                if (subCount) return subCount.innerText || subCount.textContent;
                // Try channel header subscriber count
                const headerSubs = document.querySelector('yt-formatted-string#subscriber-count');
                if (headerSubs) return headerSubs.innerText || headerSubs.textContent;
                return null;
            }""")
            subscriber_count = parse_count_text_to_int(sub_text)
        except Exception:
            pass

        hashtags = extract_hashtags_from_text(description)

        return {
            "video_id": video_id,
            "title": title,
            "description": description,
            "video_view_count": view_count,
            "upload_date_iso": upload_date_iso,
            "duration_seconds": duration_seconds,
            "duration_text": seconds_to_hms(duration_seconds),
            "thumbnail_url": f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
            "like_count": like_count,
            "comments_count": comments_count,
            "comments_off": False,
            "channel_id": channel_id,
            "channel_name": channel_name,
            "channel_username": channel_username,
            "subscriber_count": subscriber_count,
            "channel_url": f"https://www.youtube.com/channel/{channel_id}" if channel_id else None,
            "video_url": url,
            "hashtags": hashtags,
            "content_type": detect_content_type(url, page),
            "data_source": "playwright_playerjson" if player_json else "playwright_full"
        }
    except Exception as e:
        logging.error("Playwright extraction failed for %s: %s", video_id, e, exc_info=True)
        return {"video_id": video_id, "video_url": url, "data_source": "error"}


# -----------------------
# Output builder
# -----------------------
def build_unified_output(video: Dict[str, Any], channel: Optional[Dict[str, Any]] = None, **kwargs) -> Dict[str, Any]:
    ch = channel or {}
    handle = ch.get("channel_handle") or ch.get("custom_url")
    channel_username = handle[1:] if handle and isinstance(handle, str) and handle.startswith("@") else handle
    channel_id = ch.get("channel_id") or video.get("channel_id")
    if handle and isinstance(handle, str) and handle.startswith("@"):
        channel_url = f"https://www.youtube.com/{handle}"
    elif channel_id:
        channel_url = f"https://www.youtube.com/channel/{channel_id}"
    else:
        channel_url = ch.get("channel_url") or video.get("channel_url")
    thumb = video.get("thumbnail_url") or (f"https://i.ytimg.com/vi/{video.get('video_id')}/maxresdefault.jpg" if video.get("video_id") else None)
    vid_description = video.get("description") or ""
    vid_links = re.findall(r"(https?://[^\s<>]+)", vid_description)
    unique_links = []
    seen = set()
    for l in vid_links:
        if l in seen: continue
        seen.add(l); unique_links.append({"url": l, "text": l})
    return {
        "title": video.get("title"), "translatedTitle": None, "type": video.get("content_type", "video"),
        "id": video.get("video_id") or video.get("id"), "url": video.get("video_url") or video.get("url"),
        "thumbnailUrl": thumb, "viewCount": video.get("video_view_count"), "date": video.get("upload_date_iso"),
        "likes": video.get("like_count"), "location": video.get("location"),
        "channelName": video.get("channel_name") or ch.get("title") or ch.get("channel_title"),
        "channelUrl": channel_url, "channelUsername": video.get("channel_username") or channel_username,
        "collaborators": None, "channelId": channel_id, "numberOfSubscribers": video.get("subscriber_count") or ch.get("subscriber_count"),
        "duration": video.get("duration_text"), "commentsCount": video.get("comments_count"),
        "text": vid_description, "translatedText": None, "descriptionLinks": unique_links,
        "hashtags": video.get("hashtags", []), "subtitles": video.get("subtitles"),
        "isMonetized": bool(ch.get("is_monetized")) if ch.get("is_monetized") is not None else None,
        "commentsTurnedOff": bool(video.get("comments_off")) if video.get("comments_off") is not None else None
    }


# -----------------------
# Core flows
# -----------------------
def advanced_extract_video(page: Page, context: BrowserContext, url: str, channel_cache: Dict[str, Any], input_options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Navigate to canonical URL, extract metadata (API preferred, otherwise Playwright),
    attempt subtitles if requested, enrich channel via API if available, push deduped item.
    """
    global STOP_FLAG
    if STOP_FLAG:
        logging.info("Stop requested before extracting %s", url)
        return {}
    video_id = extract_video_id(url)
    if not video_id:
        logging.error("Could not extract video id from %s", url)
        return {}

    # Navigate to canonical URL for extraction
    nav_url = clean_video_url(url)
    try:
        # Always navigate when target is a shorts URL — Playwright DOM is needed for shorts metadata.
        if is_shorts_url(nav_url):
            goto_and_ready(page, nav_url)
            # Fix 3: Better waiting for shorts-specific elements
            # Wait for ytInitial* data first
            try:
                page.wait_for_function("() => Boolean(window.ytInitialPlayerResponse) || Boolean(window.ytInitialData)", timeout=12000)
            except Exception:
                logging.debug("Shorts page didn't expose ytInitial* quickly — continuing with DOM extraction")
            # Wait specifically for shorts elements (the player overlay takes longer to render)
            try:
                page.wait_for_selector('ytd-reel-video-renderer, .ytd-reel-video-renderer, ytd-shorts', timeout=8000)
            except Exception:
                logging.debug("Shorts player renderer not found — will try with available DOM")
            # Additional wait for overlay with channel info (the most important metadata source)
            try:
                page.wait_for_selector('.ytd-reel-player-overlay-renderer a[href*="/@"], ytd-channel-name a', timeout=5000)
            except Exception:
                logging.debug("Shorts channel info overlay not found — will rely on structured data and API")
        else:
            # Non-shorts: only navigate automatically when no API key (we rely on API if present)
            if not YOUTUBE_API_KEY:
                current_base = (page.url.split('?')[0] if page.url else "")
                desired_base = nav_url.split('?')[0]
                if current_base != desired_base:
                    goto_and_ready(page, nav_url)
                    try:
                        page.wait_for_function("() => Boolean(window.ytInitialPlayerResponse) || Boolean(window.ytInitialData)", timeout=15000)
                    except Exception:
                        logging.debug("ytInitial* not present after navigation; will use player_json fallback")
            else:
                # API present: still navigate when subtitles or other Playwright-only extraction requested
                if input_options.get("downloadSubtitles"):
                    current_base = (page.url.split('?')[0] if page.url else "")
                    desired_base = nav_url.split('?')[0]
                    if current_base != desired_base:
                        goto_and_ready(page, nav_url)
    except Exception as e:
        logging.error("Navigation failed for %s: %s", nav_url, e)
        return {"video_id": video_id, "video_url": url, "data_source": "error"}

    # Perform hybrid extraction (API preferred)
    # For shorts, always pass the page to enable Playwright fallback since we've already navigated
    meta = extract_video_metadata_hybrid(video_id, nav_url, page=(page if is_shorts_url(nav_url) else (None if YOUTUBE_API_KEY else page)))

    # Subtitles: only for non-shorts (shorts transcript rarely available)
    if meta.get("data_source") not in ["playwright_shorts"] and input_options.get("downloadSubtitles"):
        try:
            if page.url.split('?')[0] != nav_url.split('?')[0]:
                goto_and_ready(page, nav_url)
            sub = extract_subtitles(page)
            if sub:
                meta["subtitles"] = sub
        except Exception:
            pass

    # Channel enrichment via API if available
    channel_id = meta.get("channel_id")
    channel_enrich = None
    if channel_id and YOUTUBE_API_KEY:
        channel_enrich = channel_cache.get(channel_id)
        if not channel_enrich:
            channel_enrich = get_channel_details_from_api(channel_id)
            if channel_enrich:
                channel_cache[channel_id] = channel_enrich

    result = build_unified_output(meta, channel_enrich)

    # Push deduped item
    try:
        if result and result.get("id"):
            save_dataset([result])
    except Exception as e:
        logging.warning("LIVE DATA: Failed to push single video item: %s", e)

    return result


def scrape_channel_all_videos(page: Page, context: BrowserContext, channel_url: str, input_options: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Navigate to a channel and collect video + shorts links.
    Fix 2: Restructured to navigate to BOTH /videos and /shorts tabs when both types are requested,
    then combine results up to the caps.
    """
    global STOP_FLAG

    base = channel_url.split('?')[0].rstrip("/")
    videos_url = base + "/videos"
    shorts_url = base + "/shorts"
    
    types = normalize_search_video_types(input_options)
    logging.info("  Channel scraping for types: %s", types)
    
    # Get per-type caps
    default_cap = int(input_options.get("maxResults", 10))
    video_cap = get_cap_for_type("video", input_options, default_cap) if "video" in types else 0
    shorts_cap = get_cap_for_type("shorts", input_options, default_cap) if "shorts" in types else 0
    
    all_video_urls: List[str] = []
    links_seen: Set[str] = set()
    
    def collect_urls_from_page(target_url: str, url_pattern: str, cap: int) -> List[str]:
        """Helper to collect URLs from a channel tab page."""
        nonlocal links_seen
        if cap <= 0:
            return []
        
        collected: List[str] = []
        try:
            goto_and_ready(page, target_url)
            if is_page_unavailable(page):
                logging.info("  Tab %s reports unavailable", target_url)
                return []
            
            # Wait for content to load
            try:
                page.wait_for_selector("ytd-rich-grid-renderer, ytd-reel-shelf-renderer, ytd-browse, ytd-section-list-renderer", timeout=5000)
            except Exception:
                logging.debug("  Content selectors not found on %s", target_url)
            
            rounds = 0
            max_rounds = 60
            
            while len(collected) < cap and rounds < max_rounds and not STOP_FLAG:
                try:
                    if "/shorts/" in url_pattern:
                        # Collect shorts URLs
                        hrefs = page.evaluate("""
                            () => {
                                const out = new Set();
                                document.querySelectorAll('a[href*="/shorts/"]').forEach(a => {
                                    try { if (a.href) out.add(a.href.split('#')[0]); } catch(e){}
                                });
                                return Array.from(out);
                            }
                        """) or []
                    else:
                        # Collect regular video URLs
                        hrefs = page.evaluate("""
                            () => {
                                const out = new Set();
                                const sel = ['a#video-title', 'a#video-title-link', 'a[href*="/watch?v="]'];
                                sel.forEach(s => {
                                    document.querySelectorAll(s).forEach(a => {
                                        try { if (a.href && !a.href.includes('/shorts/')) out.add(a.href.split('#')[0]); } catch(e){}
                                    });
                                });
                                return Array.from(out);
                            }
                        """) or []
                except Exception:
                    hrefs = []
                
                for h in hrefs:
                    if STOP_FLAG or len(collected) >= cap:
                        break
                    try:
                        full = h if h.startswith("http") else f"https://www.youtube.com{h}"
                        full = clean_video_url(full)
                        vid = extract_video_id(full)
                        if not vid:
                            continue
                        # Check both the pattern match and global deduplication
                        if url_pattern in full and full not in links_seen:
                            links_seen.add(full)
                            collected.append(full)
                    except Exception:
                        continue
                
                if len(collected) >= cap:
                    break
                page.mouse.wheel(0, 2500)
                page.wait_for_timeout(350)
                rounds += 1
            
            return collected
        except Exception as e:
            logging.warning("  Failed to collect from %s: %s", target_url, e)
            return collected
    
    # Fix 2: Collect regular videos if requested
    if "video" in types and video_cap > 0 and not STOP_FLAG:
        logging.info("  Collecting up to %d regular videos from %s", video_cap, videos_url)
        video_urls = collect_urls_from_page(videos_url, "/watch", video_cap)
        all_video_urls.extend(video_urls)
        logging.info("  Collected %d regular video URLs", len(video_urls))
    
    # Fix 2: Collect shorts if requested
    if "shorts" in types and shorts_cap > 0 and not STOP_FLAG:
        logging.info("  Collecting up to %d shorts from %s", shorts_cap, shorts_url)
        shorts_urls = collect_urls_from_page(shorts_url, "/shorts/", shorts_cap)
        all_video_urls.extend(shorts_urls)
        logging.info("  Collected %d shorts URLs", len(shorts_urls))
    
    if STOP_FLAG:
        return []
    
    # Defensive: ensure only valid ids remain
    all_video_urls = [u for u in all_video_urls if extract_video_id(u)]
    logging.info("  Total collected %d candidate URLs (videos + shorts) from channel %s", len(all_video_urls), channel_url)
    
    # Proceed to extract each video/short
    out_rows: List[Dict[str, Any]] = []
    channel_cache: Dict[str, Dict[str, Any]] = {}
    total_cap = int(input_options.get("maxResults", 50))
    
    for idx, video_url in enumerate(all_video_urls[:total_cap], 1):
        if STOP_FLAG:
            break
        logging.info("  Processing video %d/%d: %s", idx, len(all_video_urls), video_url)
        try:
            row = advanced_extract_video(page, context, video_url, channel_cache, input_options)
            if row and row.get("id"):
                out_rows.append(row)
        except Exception as e:
            logging.error("  Failed to process %s: %s", video_url, e, exc_info=True)
            continue

    logging.info("  ✅ Finished scraping %d items from channel %s", len(out_rows), channel_url)
    return out_rows

# -----------------------
# Search helpers: multi-type support & robust collection
# -----------------------
def normalize_search_video_types(input_options: Dict[str, Any]) -> List[str]:
    """
    Accepts:
      - searchVideoType: "any"|"video"|"shorts"|...
      - searchVideoTypes: ["shorts","video"] OR comma-separated string "video,shorts"
    Returns a normalized ordered list.
    
    Also considers per-type caps: if maxShortsPerTerm is 0, exclude 'shorts' from default types.
    """
    single = input_options.get("searchVideoType")
    multiple = input_options.get("searchVideoTypes")
    out: List[str] = []

    # Support CSV string for convenience
    if multiple and isinstance(multiple, str):
        multiple = [s.strip() for s in multiple.split(",") if s.strip()]

    if multiple and isinstance(multiple, (list, tuple)):
        for t in multiple:
            if isinstance(t, str):
                out.append(t.strip().lower())
    if single and isinstance(single, str):
        s = single.strip().lower()
        if s == "any":
            if not out:
                # Check per-type caps to determine what to include
                default_types = []
                if input_options.get("maxVideosPerTerm", 10) > 0:
                    default_types.append("video")
                if input_options.get("maxShortsPerTerm", 0) > 0:
                    default_types.append("shorts")
                return default_types if default_types else ["video"]
        elif s == "short":
            # Normalize "short" to "shorts" for consistency
            out = ["shorts"]
        else:
            if not out:
                out = [s]
    # Normalize any 'short' to 'shorts' in the final list
    out = ["shorts" if t == "short" else t for t in out]
    if not out:
        # Default behavior: check per-type caps
        default_types = []
        if input_options.get("maxVideosPerTerm", 10) > 0:
            default_types.append("video")
        if input_options.get("maxShortsPerTerm", 0) > 0:
            default_types.append("shorts")
        return default_types if default_types else ["video"]
    if "any" in out:
        # Check per-type caps to determine what to include
        default_types = []
        if input_options.get("maxVideosPerTerm", 10) > 0:
            default_types.append("video")
        if input_options.get("maxShortsPerTerm", 0) > 0:
            default_types.append("shorts")
        return default_types if default_types else ["video"]
    return out


def gather_shorts_hrefs(page: Page) -> List[str]:
    """Aggressive collection of /shorts/ hrefs from anchors & HTML, only valid 11-char IDs."""
    hrefs = set()
    try:
        anchors = page.evaluate("""
            () => {
                const out = new Set();
                document.querySelectorAll('a[href*="/shorts/"]').forEach(a => { try { if (a.href) out.add(a.href); } catch(e){} });
                return Array.from(out);
            }
        """) or []
        for h in anchors:
            if not h: continue
            m = re.search(r"/shorts/([A-Za-z0-9_-]{11})", h)
            if m:
                hrefs.add(f"https://www.youtube.com/shorts/{m.group(1)}")
    except Exception:
        pass

    # regex fallback on HTML (also ensures 11-char ids)
    try:
        html = page.content() or ""
        ids = re.findall(r"/shorts/([A-Za-z0-9_-]{11})", html)
        for vid in ids:
            hrefs.add(f"https://www.youtube.com/shorts/{vid}")
    except Exception:
        pass

    return list(hrefs)


def gather_regular_hrefs(page: Page) -> List[str]:
    """Collect watch?v= links from ytd-video-renderer elements, excluding shelf renderers (People also watched, Related)."""
    hrefs = []
    try:
        anchors = page.evaluate("""
            () => {
                const out = new Set();
                // Target ytd-video-renderer specifically and exclude those inside shelf renderers
                document.querySelectorAll('ytd-video-renderer').forEach(renderer => {
                    try {
                        // Exclude if ytd-video-renderer is inside ytd-shelf-renderer (People also watched, Related, etc.)
                        if (renderer.closest('ytd-shelf-renderer')) return;
                        // Find the video title link within this renderer
                        const link = renderer.querySelector('a#video-title, a#video-title-link');
                        if (link && link.href) {
                            out.add(link.href);
                        }
                    } catch(e){}
                });
                return Array.from(out);
            }
        """) or []
        hrefs = [h for h in anchors if h]
    except Exception:
        hrefs = []
    # HTML fallback
    if not hrefs:
        try:
            html = page.content() or ""
            ids = re.findall(r"watch\\?v=([A-Za-z0-9_-]{11})", html)
            hrefs = [f"https://www.youtube.com/watch?v={i}" for i in ids]
        except Exception:
            hrefs = []
    # normalize dedupe & canonicalize
    seen = set(); out = []
    for h in hrefs:
        try:
            full = clean_video_url(h)
        except Exception:
            full = h
        if full not in seen:
            seen.add(full); out.append(full)
    return out


def apply_search_filters(page: Page, input_options: Dict[str, Any]):
    """
    Best-effort UI Shorts filter click. Not relied upon for results.
    """
    types = normalize_search_video_types(input_options)
    if "shorts" not in types:
        return
    try:
        btn = page.locator('button:has(yt-icon.ytd-search-sub-menu-renderer)').first
        if btn and btn.is_visible(timeout=4000):
            btn.click(); page.wait_for_timeout(400)
            sf = page.locator('ytd-search-filter-renderer:has-text("Shorts")').first
            if sf and sf.is_visible():
                sf.click(); page.wait_for_timeout(1200); logging.info("Applied Shorts filter")
    except Exception as e:
        logging.debug("Could not apply UI Shorts filter: %s", e)


def get_cap_for_type(type_name: str, input_options: Dict[str, Any], default_overall: int) -> int:
    """
    Map a requested type to its per-term cap. Falls back to a sensible default.
    """
    try:
        if type_name == "shorts":
            return int(input_options.get("maxShortsPerTerm", input_options.get("maxResults", default_overall)))
        if type_name == "video":
            return int(input_options.get("maxVideosPerTerm", input_options.get("maxResults", default_overall)))
        if type_name == "live":
            return int(input_options.get("maxStreamsPerTerm", input_options.get("maxResults", default_overall)))
        if type_name == "playlist":
            return int(input_options.get("maxPlaylistsPerTerm", input_options.get("maxResults", default_overall)))
        if type_name == "channel":
            return int(input_options.get("maxChannelsPerTerm", input_options.get("maxResults", default_overall)))
        if type_name == "movie":
            return int(input_options.get("maxMoviesPerTerm", input_options.get("maxResults", default_overall)))
    except Exception:
        pass
    return int(input_options.get("maxResults", default_overall))


def scrape_search(page: Page, context: BrowserContext, keyword: str, input_options: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Collect results per requested type using per-type caps.
    """
    global STOP_FLAG
    if STOP_FLAG:
        return []

    url = f"https://www.youtube.com/results?search_query={keyword.replace(' ', '+')}"
    goto_and_ready(page, url)

    # best-effort UI filter (optional)
    try:
        apply_search_filters(page, input_options)
    except Exception:
        pass

    # normalize types into an ordered list
    types_order = normalize_search_video_types(input_options)  # returns list like ["video","shorts"]
    default_overall = int(input_options.get("maxResults", 10))

    logging.info("Search '%s' requested types=%s", keyword, types_order)

    collected_urls: List[str] = []
    seen: Set[str] = set()

    # For each type in order, collect up to that type's cap.
    for t in types_order:
        if STOP_FLAG:
            break

        cap_for_t = get_cap_for_type(t, input_options, default_overall)
        if cap_for_t <= 0:
            continue

        logging.info("Collecting up to %d items of type '%s' for keyword='%s'", cap_for_t, t, keyword)

        rounds = 0
        max_rounds = 80

        # TYPE: SHORTS
        if t == "shorts":
            while len([u for u in collected_urls if "/shorts/" in u]) < cap_for_t and rounds < max_rounds and not STOP_FLAG:
                hrefs = gather_shorts_hrefs(page)
                for h in hrefs:
                    if STOP_FLAG or len([u for u in collected_urls if "/shorts/" in u]) >= cap_for_t:
                        break
                    if h not in seen:
                        seen.add(h); collected_urls.append(h)
                if len([u for u in collected_urls if "/shorts/" in u]) >= cap_for_t:
                    break
                try:
                    page.mouse.wheel(0, 3000); page.wait_for_timeout(900)
                except Exception:
                    page.wait_for_timeout(1200)
                rounds += 1

        # TYPE: VIDEO (watch?v=)
        elif t == "video":
            while len([u for u in collected_urls if "/watch" in u]) < cap_for_t and rounds < max_rounds and not STOP_FLAG:
                hrefs = gather_regular_hrefs(page)
                for h in hrefs:
                    if STOP_FLAG or len([u for u in collected_urls if "/watch" in u]) >= cap_for_t:
                        break
                    if h not in seen:
                        seen.add(h); collected_urls.append(h)
                if len([u for u in collected_urls if "/watch" in u]) >= cap_for_t:
                    break
                try:
                    page.mouse.wheel(0, 2200); page.wait_for_timeout(400)
                except Exception:
                    page.wait_for_timeout(600)
                rounds += 1

        # Other types...
        elif t == "channel":
            while len([u for u in collected_urls if "/channel/" in u or "/@" in u]) < cap_for_t and rounds < max_rounds and not STOP_FLAG:
                try:
                    hrefs = page.evaluate("""
                        () => {
                            const out = new Set();
                            document.querySelectorAll('a[href*="/channel/"], a[href*="/@"]').forEach(a => { try { if (a.href) out.add(a.href.split('#')[0]); } catch(e){} });
                            return Array.from(out);
                        }
                    """) or []
                except Exception:
                    hrefs = []
                for h in hrefs:
                    if STOP_FLAG or len([u for u in collected_urls if "/channel/" in u or "/@" in u]) >= cap_for_t:
                        break
                    if h not in seen:
                        seen.add(h); collected_urls.append(h)
                if len([u for u in collected_urls if "/channel/" in u or "/@" in u]) >= cap_for_t:
                    break
                try:
                    page.mouse.wheel(0, 2200); page.wait_for_timeout(400)
                except Exception:
                    page.wait_for_timeout(600)
                rounds += 1

        elif t == "playlist":
            while len([u for u in collected_urls if "/playlist" in u]) < cap_for_t and rounds < max_rounds and not STOP_FLAG:
                try:
                    hrefs = page.evaluate("""
                        () => {
                            const out = new Set();
                            document.querySelectorAll('a[href*="/playlist?"]').forEach(a => { try { if (a.href) out.add(a.href.split('#')[0]); } catch(e){} });
                            return Array.from(out);
                        }
                    """) or []
                except Exception:
                    hrefs = []
                for h in hrefs:
                    if STOP_FLAG or len([u for u in collected_urls if "/playlist" in u]) >= cap_for_t:
                        break
                    if h not in seen:
                        seen.add(h); collected_urls.append(h)
                if len([u for u in collected_urls if "/playlist" in u]) >= cap_for_t:
                    break
                try:
                    page.mouse.wheel(0, 2200); page.wait_for_timeout(400)
                except Exception:
                    page.wait_for_timeout(600)
                rounds += 1

        elif t in ("live", "movie"):
            target_keyword = "live" if t == "live" else "movie"
            while len([u for u in collected_urls if target_keyword in u or "/watch" in u]) < cap_for_t and rounds < max_rounds and not STOP_FLAG:
                hrefs = gather_regular_hrefs(page)
                for h in hrefs:
                    if STOP_FLAG or len([u for u in collected_urls if target_keyword in u or "/watch" in u]) >= cap_for_t:
                        break
                    if h not in seen:
                        seen.add(h); collected_urls.append(h)
                if len([u for u in collected_urls if target_keyword in u or "/watch" in u]) >= cap_for_t:
                    break
                try:
                    page.mouse.wheel(0, 2200); page.wait_for_timeout(400)
                except Exception:
                    page.wait_for_timeout(600)
                rounds += 1

        else:
            logging.debug("Unsupported search type requested: %s", t)

    logging.info("Total collected %d URLs for keyword '%s'", len(collected_urls), keyword)

    # Defensive filter: keep only URLs with valid video id
    collected_urls = [u for u in collected_urls if extract_video_id(u)]

    # Now scrape each collected URL
    results: List[Dict[str, Any]] = []
    channel_cache: Dict[str, Dict[str, Any]] = {}
    for u in collected_urls:
        if STOP_FLAG:
            break
        try:
            row = advanced_extract_video(page, context, u, channel_cache, input_options)
            if row and row.get("id"):
                results.append(row)
        except Exception as e:
            logging.error("Failed to extract %s: %s", u, e)
            continue

    return results


# -----------------------
# Entrypoint & helpers
# -----------------------
def get_input() -> Dict[str, Any]:
    default = {"startUrls": [], "searchTerms": []}
    is_on_platform = bool(os.environ.get("APIFY_IS_AT_HOME"))
    logging.info("="*60)
    logging.info("🔧 APIFY PLATFORM DETECTED" if is_on_platform else "💻 LOCAL MODE")
    logging.info("="*60)
    if is_on_platform and HAS_APIFY_CLIENT:
        try:
            token = os.environ.get("APIFY_TOKEN"); kvs_id = os.environ.get("APIFY_DEFAULT_KEY_VALUE_STORE_ID")
            input_key = os.environ.get("APIFY_INPUT_KEY", "INPUT"); api_base = os.environ.get("APIFY_API_BASE_URL")
            client = ApifyClient(token, api_url=api_base) if api_base else ApifyClient(token)
            kvs = client.key_value_store(kvs_id)
            rec = kvs.get_record(input_key)
            if rec and rec.get("value"):
                data = rec["value"]; return data
        except Exception:
            pass
    for path in ["./input.local.json", "./input.json"]:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f: return json.load(f)
            except Exception:
                pass
    return default


def kvs_set_bytes(key: str, data: bytes, content_type: str = "application/octet-stream"):
    if not HAS_APIFY_CLIENT: return
    try:
        kvs_id = os.getenv("APIFY_DEFAULT_KEY_VALUE_STORE_ID"); token = os.getenv("APIFY_TOKEN")
        api_base = os.getenv("APIFY_API_BASE_URL")
        client = ApifyClient(token, api_url=api_base) if api_base else ApifyClient(token)
        client.key_value_store(kvs_id).set_record(key, data, content_type=content_type)
    except Exception:
        pass


def try_capture(page: Page, key: str = "LAST_SCREENSHOT.png"):
    if not HAS_APIFY_CLIENT:
        try:
            png = page.screenshot(full_page=True)
            with open(key, "wb") as f: f.write(png)
        except Exception:
            pass
        return
    try:
        png = page.screenshot(full_page=True)
        kvs_set_bytes(key, png, content_type="image/png")
    except Exception:
        pass


def is_channel_url(url: str) -> bool:
    u = (url or "").lower()
    return ("/@" in u) or ("/channel/" in u) or ("/c/" in u)


def main():
    input_options = get_input()
    logging.info("ACTOR INPUT RECEIVED")
    global YOUTUBE_API_KEY, STOP_FLAG, SAVED_IDS
    if input_options.get("youtubeApiKey"):
        YOUTUBE_API_KEY = input_options["youtubeApiKey"]; os.environ["YOUTUBE_API_KEY"] = YOUTUBE_API_KEY
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") not in ("0", "false", "no")
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=headless)
        except Exception as e:
            logging.error("Browser launch failed: %s", e); return
        context = browser.new_context(
            user_agent=os.getenv("USER_AGENT", "Mozilla/5.0"),
            locale=os.getenv("LOCALE", "en-US"), timezone_id=os.getenv("TZ", "UTC"),
            extra_http_headers={"Accept-Language": os.getenv("ACCEPT_LANGUAGE", "en-US,en;q=0.9")},
            viewport={"width": 1366, "height": 768}
        )
        context.set_default_navigation_timeout(45_000); context.set_default_timeout(20_000)
        try:
            context.add_cookies([{"name": "CONSENT", "value": os.getenv("CONSENT_VALUE", "YES+cb"), "domain": ".youtube.com", "path": "/"}])
        except Exception:
            pass
        page = context.new_page()
        try:
            raw_start = input_options.get("startUrls") or []
            raw_direct = input_options.get("directUrls") or []
            all_urls = []
            for u in raw_start:
                if isinstance(u, dict) and u.get("url"): all_urls.append(u.get("url"))
                elif isinstance(u, str): all_urls.append(u)
            for d in raw_direct:
                if isinstance(d, str): all_urls.append(d)
            search_terms = input_options.get("searchTerms") or []
            
            # Global cap on total results
            global_max = int(input_options.get("maxResults", 50))
            total_scraped = 0
            
            if all_urls:
                logging.info("PROCESSING %d START URLs (global max: %d)", len(all_urls), global_max)
                for idx, url in enumerate(all_urls, 1):
                    if STOP_FLAG:
                        logging.info("Stop requested; exiting URL loop"); break
                    if total_scraped >= global_max:
                        logging.info("Reached global max of %d results; stopping URL processing", global_max)
                        break
                    logging.info("URL %d/%d: %s (total scraped so far: %d)", idx, len(all_urls), url, total_scraped)
                    try:
                        if is_channel_url(url):
                            results = scrape_channel_all_videos(page, context, url, input_options)
                            total_scraped += len(results) if results else 0
                        elif is_video_url(url):
                            result = advanced_extract_video(page, context, url, {}, input_options)
                            if result and result.get("id"):
                                total_scraped += 1
                        else:
                            results = scrape_search(page, context, url, input_options)
                            total_scraped += len(results) if results else 0
                    except Exception as e:
                        logging.error("Failed URL %s: %s", url, e, exc_info=True)
            if search_terms and not STOP_FLAG and total_scraped < global_max:
                logging.info("PROCESSING %d SEARCH TERMS (global max: %d, already scraped: %d)", len(search_terms), global_max, total_scraped)
                for idx, kw in enumerate(search_terms, 1):
                    if STOP_FLAG:
                        logging.info("Stop requested; exiting search loop"); break
                    if total_scraped >= global_max:
                        logging.info("Reached global max of %d results; stopping search processing", global_max)
                        break
                    logging.info("SEARCH %d/%d: %s (total scraped so far: %d)", idx, len(search_terms), kw, total_scraped)
                    try:
                        results = scrape_search(page, context, kw, input_options)
                        total_scraped += len(results) if results else 0
                    except Exception as e:
                        logging.error("Search failed %s: %s", kw, e, exc_info=True)
            if not all_urls and not search_terms:
                logging.warning("No startUrls/directUrls/searchTerms provided")
            logging.info("TOTAL RESULTS SCRAPED: %d", total_scraped)
        except Exception as e:
            logging.error("Unhandled error: %s", e, exc_info=True)
            try_capture(page, key="FATAL_ERROR.png")
        finally:
            logging.info("SCRAPING FINISHED — cleaning up")
            try:
                page.close()
            except Exception:
                pass
            browser.close()


if __name__ == "__main__":
    main()