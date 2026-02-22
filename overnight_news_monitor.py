"""
Overnight News Monitor (6 PM - 8 AM EST)
Gathers market-moving news about public companies and sends to Discord.
Uses DeepSeek AI to assess newsworthiness and validates with stock price reactions.
"""

import os
import re
import json
import time
import logging
import requests
import feedparser
import yfinance as yf
from datetime import datetime, timedelta, timezone
from openai import OpenAI
import pytz

# ─── Configuration ───────────────────────────────────────────────────────────

DISCORD_WEBHOOK = os.getenv(
    "DISCORD_WEBHOOK",
    "https://discordapp.com/api/webhooks/1474960441741410375/WBQe2gNHx_xqDjSoM-u1zOEHx3GSZRQ3gmbTUVTRjhZ05SXD5R4MeNBQs5WtoSwwsWDa"
)

DEEPSEEK_API_KEY = os.getenv(
    "DEEPSEEK_API_KEY",
    "sk-1cf5b2ab46a14eb6978ff7ba7ce3f3e3"
)

EST = pytz.timezone("US/Eastern")

# DeepSeek client (OpenAI-compatible)
deepseek = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com"
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("OvernightNews")

# ─── Major Ticker Mapping (top companies by market cap + common movers) ──────

COMPANY_TICKERS = {
    "apple": "AAPL", "microsoft": "MSFT", "amazon": "AMZN", "alphabet": "GOOGL",
    "google": "GOOGL", "meta": "META", "facebook": "META", "nvidia": "NVDA",
    "tesla": "TSLA", "berkshire": "BRK-B", "broadcom": "AVGO", "jpmorgan": "JPM",
    "johnson & johnson": "JNJ", "visa": "V", "walmart": "WMT", "exxon": "XOM",
    "unitedhealth": "UNH", "mastercard": "MA", "procter": "PG", "costco": "COST",
    "home depot": "HD", "abbvie": "ABBV", "coca-cola": "KO", "salesforce": "CRM",
    "netflix": "NFLX", "chevron": "CVX", "merck": "MRK", "pepsi": "PEP",
    "pepsico": "PEP", "adobe": "ADBE", "cisco": "CSCO", "thermo fisher": "TMO",
    "accenture": "ACN", "mcdonald": "MCD", "qualcomm": "QCOM", "intel": "INTC",
    "amd": "AMD", "advanced micro": "AMD", "texas instruments": "TXN",
    "boeing": "BA", "caterpillar": "CAT", "goldman sachs": "GS",
    "morgan stanley": "MS", "disney": "DIS", "nike": "NKE", "paypal": "PYPL",
    "starbucks": "SBUX", "uber": "UBER", "airbnb": "ABNB", "palantir": "PLTR",
    "snowflake": "SNOW", "shopify": "SHOP", "coinbase": "COIN", "robinhood": "HOOD",
    "gamestop": "GME", "amc": "AMC", "rivian": "RIVN", "lucid": "LCID",
    "nio": "NIO", "sofi": "SOFI", "draftkings": "DKNG", "roku": "ROKU",
    "crowdstrike": "CRWD", "datadog": "DDOG", "cloudflare": "NET",
    "twilio": "TWLO", "okta": "OKTA", "zscaler": "ZS", "confluent": "CFLT",
    "mongodb": "MDB", "elastic": "ESTC", "splunk": "SPLK", "servicenow": "NOW",
    "workday": "WDAY", "hubspot": "HUBS", "fortinet": "FTNT",
    "palo alto": "PANW", "arista": "ANET", "lululemon": "LULU",
    "target": "TGT", "kroger": "KR", "dollar general": "DG",
    "moderna": "MRNA", "pfizer": "PFE", "eli lilly": "LLY", "novo nordisk": "NVO",
    "regeneron": "REGN", "gilead": "GILD", "amgen": "AMGN", "biogen": "BIIB",
    "vertex": "VRTX", "illumina": "ILMN", "intuitive surgical": "ISRG",
    "dexcom": "DXCM", "edwards lifesciences": "EW", "abiomed": "ABMD",
    "general motors": "GM", "ford": "F", "toyota": "TM", "gm": "GM",
    "general electric": "GE", "3m": "MMM", "honeywell": "HON",
    "lockheed": "LMT", "raytheon": "RTX", "northrop": "NOC",
    "general dynamics": "GD", "bae systems": "BAESY", "l3harris": "LHX",
    "ibm": "IBM", "oracle": "ORCL", "sap": "SAP", "dell": "DELL",
    "hewlett": "HPE", "hp inc": "HPQ", "micron": "MU", "applied materials": "AMAT",
    "lam research": "LRCX", "asml": "ASML", "tsmc": "TSM", "arm": "ARM",
    "marvell": "MRVL", "on semiconductor": "ON", "skyworks": "SWKS",
    "snap": "SNAP", "pinterest": "PINS", "twitter": "X", "spotify": "SPOT",
    "roblox": "RBLX", "unity": "U", "electronic arts": "EA",
    "take-two": "TTWO", "activision": "ATVI", "warner bros": "WBD",
    "paramount": "PARA", "fox": "FOX", "comcast": "CMCSA",
    "at&t": "T", "verizon": "VZ", "t-mobile": "TMUS",
    "devon energy": "DVN", "pioneer natural": "PXD", "marathon": "MPC",
    "conocophillips": "COP", "schlumberger": "SLB", "halliburton": "HAL",
    "freeport": "FCX", "newmont": "NEM", "southern copper": "SCCO",
    "ups": "UPS", "fedex": "FDX", "union pacific": "UNP",
    "csx": "CSX", "delta": "DAL", "united airlines": "UAL",
    "american airlines": "AAL", "southwest": "LUV",
    "zillow": "Z", "redfin": "RDFN", "opendoor": "OPEN",
    "block": "SQ", "square": "SQ", "stripe": "STRIP",
    "toast": "TOST", "affirm": "AFRM", "upstart": "UPST",
    "mara": "MARA", "riot": "RIOT", "marathon digital": "MARA",
    "microstrategy": "MSTR", "hut 8": "HUT",
    "supermicro": "SMCI", "super micro": "SMCI", "dell technologies": "DELL",
    "c3.ai": "AI", "soundhound": "SOUN", "bigbear": "BBAI",
    "symbotic": "SYM", "recursion": "RXRX",
}

# ─── News Sources (RSS Feeds) ───────────────────────────────────────────────

RSS_FEEDS = {
    # Major financial news
    "Reuters Business": "https://feeds.reuters.com/reuters/businessNews",
    "Reuters Company": "https://feeds.reuters.com/reuters/companyNews",
    "CNBC Top News": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "CNBC Earnings": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135",
    "MarketWatch Top": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "MarketWatch Breaking": "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex",
    "Seeking Alpha Market News": "https://seekingalpha.com/market_currents.xml",
    "Seeking Alpha Top News": "https://seekingalpha.com/feed.xml",
    # Press Releases
    "BusinessWire": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWQ==",
    "PR Newswire": "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
    "GlobeNewsWire": "https://www.globenewswire.com/RssFeed/subjectcode/14-Earnings/feedTitle/GlobeNewswire%20-%20Earnings",
    # Supplemental
    "Bloomberg Markets": "https://feeds.bloomberg.com/markets/news.rss",
    "WSJ Markets": "https://feeds.a.wsj.com/rss/RSSMarketsMain.xml",
    "Barrons": "https://feeds.barrons.com/barrons/market_data",
    "Investor's Business Daily": "https://www.investors.com/feed/",
    "Benzinga": "https://www.benzinga.com/feed",
}

# ─── Helper: Extract tickers from text ──────────────────────────────────────

def extract_tickers(text: str) -> list[str]:
    """Extract stock tickers mentioned in text using company name mapping and $TICKER patterns."""
    found = set()
    text_lower = text.lower()

    # Match $TICKER patterns
    ticker_pattern = re.findall(r'\$([A-Z]{1,5})\b', text)
    found.update(ticker_pattern)

    # Match explicit TICKER: or (TICKER) or [TICKER] patterns
    explicit = re.findall(r'(?:\(|\[|:)\s*([A-Z]{1,5})\s*(?:\)|\])', text)
    found.update(explicit)

    # Match "NASDAQ: TICKER" or "NYSE: TICKER" patterns
    exchange = re.findall(r'(?:NASDAQ|NYSE|AMEX|OTC):\s*([A-Z]{1,5})', text)
    found.update(exchange)

    # Match company names
    for company, ticker in COMPANY_TICKERS.items():
        if company in text_lower:
            found.add(ticker)

    # Filter out common false positives
    false_positives = {"A", "I", "IT", "AI", "AM", "AN", "AS", "AT", "BE", "BY",
                       "DO", "GO", "IF", "IN", "IS", "ME", "MY", "NO", "OF", "OH",
                       "OK", "ON", "OR", "SO", "TO", "UP", "US", "WE", "CEO", "FDA",
                       "SEC", "IPO", "ETF", "GDP", "CPI", "FED", "IMF", "DOJ", "EPA",
                       "FAQ", "API", "CEO", "CFO", "COO", "CTO", "NEW", "THE", "FOR",
                       "ARE", "BUT", "NOT", "YOU", "ALL", "CAN", "HAS", "HER", "WAS",
                       "ONE", "OUR", "OUT", "HIS", "HOW", "ITS", "MAY", "SAY", "SHE",
                       "TWO", "WAY", "WHO", "OIL", "TOP", "NOW", "OLD", "SEE", "WAR"}
    found -= false_positives

    return list(found)


# ─── Helper: Check stock price reaction ──────────────────────────────────────

def check_stock_reaction(ticker: str) -> dict | None:
    """Check if a stock has moved significantly in after-hours/pre-market."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info or {}

        # Get current/last price and previous close
        current = info.get("currentPrice") or info.get("regularMarketPrice")
        prev_close = info.get("previousClose") or info.get("regularMarketPreviousClose")

        if current and prev_close and prev_close > 0:
            change_pct = ((current - prev_close) / prev_close) * 100
            return {
                "ticker": ticker,
                "current": round(current, 2),
                "prev_close": round(prev_close, 2),
                "change_pct": round(change_pct, 2),
                "significant": abs(change_pct) >= 2.0  # 2%+ is notable
            }

        # Try using history as fallback
        hist = stock.history(period="5d")
        if len(hist) >= 2:
            current = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[-2]
            change_pct = ((current - prev) / prev) * 100
            return {
                "ticker": ticker,
                "current": round(current, 2),
                "prev_close": round(prev, 2),
                "change_pct": round(change_pct, 2),
                "significant": abs(change_pct) >= 2.0
            }
    except Exception as e:
        log.debug(f"Could not check {ticker}: {e}")
    return None


# ─── Fetch News from RSS ────────────────────────────────────────────────────

def fetch_rss_news(cutoff_hours: int = 14) -> list[dict]:
    """Fetch news articles from all RSS feeds published within cutoff window."""
    articles = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=cutoff_hours)

    for source, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:30]:  # top 30 per feed
                # Parse published date
                pub_date = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                    pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

                # If no date, skip (can't verify recency)
                if not pub_date:
                    continue

                # Only include articles within our window
                if pub_date < cutoff:
                    continue

                title = entry.get('title', '').strip()
                summary = entry.get('summary', '').strip()
                link = entry.get('link', '')

                if not title:
                    continue

                # Clean HTML from summary
                summary = re.sub(r'<[^>]+>', '', summary)[:500]

                articles.append({
                    "title": title,
                    "summary": summary,
                    "link": link,
                    "source": source,
                    "published": pub_date.isoformat(),
                    "pub_date": pub_date,
                })
        except Exception as e:
            log.warning(f"Failed to fetch {source}: {e}")

    log.info(f"Fetched {len(articles)} articles from RSS feeds")
    return articles


# ─── Filter for Company-Related News ────────────────────────────────────────

def filter_company_news(articles: list[dict]) -> list[dict]:
    """Filter articles that mention public companies."""
    company_articles = []
    for art in articles:
        text = f"{art['title']} {art['summary']}"
        tickers = extract_tickers(text)
        if tickers:
            art['tickers'] = tickers[:5]  # limit to 5 tickers per article
            company_articles.append(art)
    log.info(f"Filtered to {len(company_articles)} company-related articles")
    return company_articles


# ─── DeepSeek AI: Assess Newsworthiness ─────────────────────────────────────

def assess_newsworthiness_batch(articles: list[dict]) -> list[dict]:
    """Use DeepSeek to rate articles for market-moving potential. Processes in batches."""
    if not articles:
        return []

    rated = []
    batch_size = 15  # process 15 at a time

    for i in range(0, len(articles), batch_size):
        batch = articles[i:i + batch_size]
        batch_text = ""
        for idx, art in enumerate(batch):
            batch_text += f"\n[{idx}] TICKERS: {', '.join(art.get('tickers', []))}\n"
            batch_text += f"    HEADLINE: {art['title']}\n"
            batch_text += f"    SUMMARY: {art['summary'][:300]}\n"
            batch_text += f"    SOURCE: {art['source']}\n"

        prompt = f"""You are a financial news analyst. Rate each article below for its potential to move stock prices overnight/pre-market. 

Consider these factors:
- Earnings beats/misses, guidance changes
- M&A activity, major partnerships, divestitures  
- FDA approvals/rejections, clinical trial results
- Major contract wins/losses
- Executive changes (CEO/CFO departures)
- Regulatory actions, lawsuits, investigations
- Product launches/failures, recalls
- Analyst upgrades/downgrades with significant price target changes
- Geopolitical events affecting specific companies
- Significant layoffs or restructuring

Rate each article 1-10:
- 8-10: Almost certainly market-moving (earnings surprise, M&A, FDA decision)
- 5-7: Likely notable move (analyst action, contract win, exec change)  
- 3-4: Minor/uncertain impact
- 1-2: Not market-moving (routine news, opinion pieces)

Respond ONLY in JSON array format. Each object must have:
- "index": the article index number
- "score": integer 1-10
- "reason": brief 1-sentence explanation
- "category": one of [earnings, ma, fda, contract, executive, regulatory, product, analyst, guidance, restructuring, other]

Articles:
{batch_text}

JSON response:"""

        try:
            response = deepseek.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=2000,
            )
            content = response.choices[0].message.content.strip()

            # Extract JSON from response
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                ratings = json.loads(json_match.group())
                for rating in ratings:
                    idx = rating.get("index", -1)
                    if 0 <= idx < len(batch):
                        batch[idx]["ai_score"] = rating.get("score", 0)
                        batch[idx]["ai_reason"] = rating.get("reason", "")
                        batch[idx]["ai_category"] = rating.get("category", "other")
            else:
                log.warning("Could not parse DeepSeek response as JSON")
                for art in batch:
                    art["ai_score"] = 0
        except Exception as e:
            log.error(f"DeepSeek API error: {e}")
            for art in batch:
                art["ai_score"] = 0

        # Rate limit
        time.sleep(1)

    # Filter to score >= 5 (noteworthy+)
    for art in articles:
        if art.get("ai_score", 0) >= 5:
            rated.append(art)

    log.info(f"DeepSeek rated {len(rated)} articles as noteworthy (score >= 5)")
    return rated


# ─── Validate with Stock Reactions ───────────────────────────────────────────

def validate_with_price_action(articles: list[dict]) -> list[dict]:
    """Check stock price reactions to boost confidence in ratings."""
    checked_tickers = {}  # cache

    for art in articles:
        reactions = []
        for ticker in art.get("tickers", []):
            if ticker not in checked_tickers:
                checked_tickers[ticker] = check_stock_reaction(ticker)
                time.sleep(0.2)  # rate limit yfinance

            reaction = checked_tickers[ticker]
            if reaction:
                reactions.append(reaction)
                # Boost score if stock moved significantly
                if reaction["significant"]:
                    art["ai_score"] = min(10, art.get("ai_score", 5) + 1)
                    art["price_validated"] = True

        art["reactions"] = reactions

    return articles


# ─── Deduplicate Similar Headlines ───────────────────────────────────────────

def deduplicate(articles: list[dict]) -> list[dict]:
    """Remove duplicate/very similar articles, keeping highest scored."""
    if not articles:
        return []

    # Sort by score descending
    articles.sort(key=lambda x: x.get("ai_score", 0), reverse=True)

    seen_titles = []
    unique = []

    for art in articles:
        title_lower = art["title"].lower()
        # Check if similar title already seen
        is_dup = False
        for seen in seen_titles:
            # Simple similarity: check if >60% of words overlap
            words_new = set(title_lower.split())
            words_seen = set(seen.split())
            if len(words_new) > 0:
                overlap = len(words_new & words_seen) / len(words_new)
                if overlap > 0.6:
                    is_dup = True
                    break

        if not is_dup:
            seen_titles.append(title_lower)
            unique.append(art)

    log.info(f"Deduplicated to {len(unique)} unique articles")
    return unique


# ─── Format & Send to Discord ────────────────────────────────────────────────

def get_category_emoji(category: str) -> str:
    emojis = {
        "earnings": "📊", "ma": "🤝", "fda": "💊", "contract": "📝",
        "executive": "👔", "regulatory": "⚖️", "product": "🚀",
        "analyst": "📈", "guidance": "🔮", "restructuring": "🔧", "other": "📰"
    }
    return emojis.get(category, "📰")


def format_discord_embed(article: dict) -> dict:
    """Format a single article as a Discord embed."""
    tickers = article.get("tickers", [])
    ticker_str = " ".join([f"`${t}`" for t in tickers])
    category = article.get("ai_category", "other")
    emoji = get_category_emoji(category)
    score = article.get("ai_score", 0)

    # Build description
    desc = f"{emoji} **Category:** {category.upper()}\n"
    desc += f"🎯 **Newsworthiness:** {score}/10\n"
    desc += f"🏷️ **Tickers:** {ticker_str}\n\n"

    if article.get("ai_reason"):
        desc += f"*{article['ai_reason']}*\n\n"

    # Add price reactions
    reactions = article.get("reactions", [])
    significant_reactions = [r for r in reactions if r.get("significant")]
    if significant_reactions:
        desc += "**📉 Price Reaction:**\n"
        for r in significant_reactions:
            arrow = "🟢" if r["change_pct"] > 0 else "🔴"
            desc += f"{arrow} `${r['ticker']}` {r['change_pct']:+.2f}% (${r['prev_close']} → ${r['current']})\n"
        desc += "\n"

    if article.get("summary"):
        summary = article["summary"][:250]
        if len(article["summary"]) > 250:
            summary += "..."
        desc += f"{summary}\n"

    # Color based on score
    if score >= 8:
        color = 0xFF0000  # red = high impact
    elif score >= 6:
        color = 0xFFA500  # orange
    else:
        color = 0x3498DB  # blue

    embed = {
        "title": article["title"][:256],
        "description": desc[:4096],
        "color": color,
        "url": article.get("link", ""),
        "footer": {"text": f"Source: {article.get('source', 'Unknown')} | Score: {score}/10"},
        "timestamp": article.get("published", datetime.now(timezone.utc).isoformat()),
    }

    if article.get("price_validated"):
        embed["author"] = {"name": "✅ PRICE ACTION CONFIRMED"}

    return embed


def send_to_discord(articles: list[dict]):
    """Send all articles to Discord webhook."""
    if not articles:
        # Send "no news" message
        payload = {
            "embeds": [{
                "title": "🌙 Overnight News Monitor",
                "description": "No significant market-moving news detected during the overnight session.",
                "color": 0x2ECC71,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        }
        try:
            requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        except Exception as e:
            log.error(f"Discord send error: {e}")
        return

    # Send header
    now_est = datetime.now(EST)
    header = {
        "embeds": [{
            "title": "🌙 Overnight News Digest",
            "description": (
                f"**{len(articles)} market-moving stories detected**\n"
                f"Window: 6:00 PM – 8:00 AM EST\n"
                f"Generated: {now_est.strftime('%B %d, %Y at %I:%M %p EST')}\n\n"
                f"🔴 = High Impact (8-10) | 🟠 = Notable (6-7) | 🔵 = Moderate (5)"
            ),
            "color": 0x1a1a2e,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }
    try:
        requests.post(DISCORD_WEBHOOK, json=header, timeout=10)
        time.sleep(1)
    except Exception as e:
        log.error(f"Discord header error: {e}")

    # Send articles in batches of 5 (Discord embed limit)
    for i in range(0, len(articles), 5):
        batch = articles[i:i + 5]
        embeds = [format_discord_embed(art) for art in batch]
        payload = {"embeds": embeds}

        try:
            resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
            if resp.status_code == 429:
                retry_after = resp.json().get("retry_after", 5)
                log.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
            elif resp.status_code != 204:
                log.warning(f"Discord returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.error(f"Discord send error: {e}")

        time.sleep(1.5)  # respect rate limits

    # Send footer summary
    top_tickers = {}
    for art in articles:
        for t in art.get("tickers", []):
            top_tickers[t] = max(top_tickers.get(t, 0), art.get("ai_score", 0))

    top_sorted = sorted(top_tickers.items(), key=lambda x: x[1], reverse=True)[:10]
    ticker_summary = " | ".join([f"`${t}` ({s}/10)" for t, s in top_sorted])

    footer = {
        "embeds": [{
            "title": "📋 Summary – Top Tickers Mentioned",
            "description": ticker_summary or "No tickers detected",
            "color": 0x1a1a2e,
            "footer": {"text": "Overnight News Monitor | Powered by DeepSeek AI"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }
    try:
        time.sleep(1)
        requests.post(DISCORD_WEBHOOK, json=footer, timeout=10)
    except Exception as e:
        log.error(f"Discord footer error: {e}")


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def run_pipeline():
    """Execute the full news gathering and filtering pipeline."""
    log.info("=" * 60)
    log.info("OVERNIGHT NEWS MONITOR - Starting pipeline")
    log.info("=" * 60)

    # Step 1: Fetch news from RSS feeds
    log.info("Step 1: Fetching RSS news...")
    articles = fetch_rss_news(cutoff_hours=14)

    if not articles:
        log.warning("No articles fetched from any source")
        send_to_discord([])
        return

    # Step 2: Filter for company-related news
    log.info("Step 2: Filtering for company mentions...")
    company_news = filter_company_news(articles)

    if not company_news:
        log.warning("No company-related articles found")
        send_to_discord([])
        return

    # Step 3: Deduplicate
    log.info("Step 3: Deduplicating articles...")
    company_news = deduplicate(company_news)

    # Step 4: AI assessment (send top 60 by recency to DeepSeek)
    log.info("Step 4: DeepSeek AI assessment...")
    company_news.sort(key=lambda x: x.get("pub_date", datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
    to_assess = company_news[:60]
    noteworthy = assess_newsworthiness_batch(to_assess)

    if not noteworthy:
        log.warning("No articles rated as noteworthy by AI")
        send_to_discord([])
        return

    # Step 5: Validate with price action
    log.info("Step 5: Checking stock price reactions...")
    validated = validate_with_price_action(noteworthy)

    # Step 6: Final sort and limit
    validated.sort(key=lambda x: x.get("ai_score", 0), reverse=True)
    top_stories = validated[:20]  # send top 20

    # Step 7: Send to Discord
    log.info(f"Step 6: Sending {len(top_stories)} stories to Discord...")
    send_to_discord(top_stories)

    log.info("Pipeline complete!")


# ─── Scheduling ──────────────────────────────────────────────────────────────

def is_overnight_window() -> bool:
    """Check if current time is within the 6PM-8AM EST overnight window."""
    now_est = datetime.now(EST)
    hour = now_est.hour
    return hour >= 18 or hour < 8


def run_with_schedule():
    """Run the monitor on a schedule: every 2 hours during overnight, plus summary at 7:45 AM."""
    import schedule

    def scheduled_run():
        if is_overnight_window():
            run_pipeline()
        else:
            log.info("Outside overnight window (6PM-8AM EST), skipping.")

    # Run every 2 hours
    schedule.every(2).hours.do(scheduled_run)

    # Run morning summary at 7:45 AM EST
    schedule.every().day.at("07:45").do(run_pipeline)

    log.info("Scheduler started. Running every 2 hours during overnight (6PM-8AM EST)")
    log.info("Morning summary at 7:45 AM EST")

    # Run immediately on start
    run_pipeline()

    while True:
        schedule.run_pending()
        time.sleep(60)


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if "--schedule" in sys.argv:
        run_with_schedule()
    else:
        # Single run mode
        run_pipeline()
