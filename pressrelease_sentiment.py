# Python script to extract AI sentiment from TecDAX press releases
# Nils Clausen
# 2025-08-27
#
import os
import re
import time
import csv
import requests
from typing import List, Tuple
from bs4 import BeautifulSoup
from tqdm import tqdm
from newspaper import Article
from langdetect import detect, DetectorFactory
import nltk
nltk.download("punkt", quiet=True)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pdfminer.high_level import extract_text as pdf_extract_text
from datetime import datetime

TECDAX_COMPANIES = {
    "elmos": "https://www.elmos.com/english/about-elmos/newsroom/press-releases.html",
    "hensoldt": "https://investors.hensoldt.net/news/",
    "ionos": "https://www.ionos-group.com/newsroom/",
    "nemetschek": "https://www.nemetschek.com/en/news-media/newsroom/",
    "sap": "https://news.sap.com/press-room/",
    "suess": "https://www.suss.com/en/news",
    "zeiss med": "https://www.zeiss.com/meditec-ag/en/media-news.html",
    "evotec": "https://www.evotec.com/ir-news/news",
    "formycon": "https://www.formycon.com/en/news-media/press-releases/",
    "sma solar": "https://www.sma.de/en/investor-relations/news",
    "stratec": "https://www.stratec.com/news",
    "verbio": "https://www.verbio.de/en/investor-relations/corporate-news/"
}

LOCAL_FALLBACK_DIR = "local_releases"
OUTPUT_CSV = "ai_sentiment_tecdax.csv"

AI_KEYWORDS_EN = [
    "artificial intelligence", "ai", "machine learning", "deep learning",
    "neural network", "nlp", "natural language processing", "computer vision",
    "generative ai", "foundation model", "large language model", "llm"
]

AI_KEYWORDS_DE = [
    "künstliche intelligenz", "ki", "maschinelles lernen", "deep learning",
    "neuronales netz", "nlp", "natürliche sprachverarbeitung", "computer vision",
    "generative ki", "foundation model", "großes sprachmodell", "llm"
]

MIN_SENTENCE_LEN = 20
SENTIMENT_THRESHOLDS = {"positive": 0.05, "negative": -0.05}

USE_PLAYWRIGHT = True
PLAYWRIGHT_WAIT_MS = 1500

# ---------------------------
# Date helpers 
# ---------------------------
MONTHS_DE = {
    "januar":1, "februar":2, "märz":3, "maerz":3, "april":4, "mai":5, "juni":6,
    "juli":7, "august":8, "september":9, "oktober":10, "november":11, "dezember":12
}
MONTHS_EN = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12
}

def _parse_date_string(s):
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    m = re.search(r'(?P<y>20\d{2})[.\-\/](?P<m>\d{1,2})[.\-\/](?P<d>\d{1,2})', s)
    if m:
        try:
            return datetime(int(m.group('y')), int(m.group('m')), int(m.group('d')))
        except Exception:
            pass
    m = re.search(r'(?P<d>\d{1,2})\s+(?P<mon>[A-Za-zäöüÄÖÜ]+)\s+(?P<y>20\d{2})', s)
    if m:
        mon = m.group('mon').lower()
        monnum = MONTHS_EN.get(mon) or MONTHS_DE.get(mon)
        if monnum:
            try:
                return datetime(int(m.group('y')), monnum, int(m.group('d')))
            except Exception:
                pass
    m = re.search(r'(?P<mon>[A-Za-zäöüÄÖÜ]+)\s+(?P<d>\d{1,2}),\s*(?P<y>20\d{2})', s)
    if m:
        mon = m.group('mon').lower()
        monnum = MONTHS_EN.get(mon) or MONTHS_DE.get(mon)
        if monnum:
            try:
                return datetime(int(m.group('y')), monnum, int(m.group('d')))
            except Exception:
                pass
    return None

def _find_date_in_text(text):
    if not text:
        return None
    m = re.search(r'(20\d{2})', text)
    if m:
        year = int(m.group(1))
        m2 = re.search(r'([A-Za-zäöüÄÖÜ]+)\s+' + m.group(1), text)
        if m2:
            mon = m2.group(1).lower()
            monnum = MONTHS_EN.get(mon) or MONTHS_DE.get(mon)
            if monnum:
                return datetime(year, monnum, 1)
        m3 = re.search(r'20\d{2}[.\-\/](\d{1,2})', text)
        if m3:
            try:
                return datetime(year, int(m3.group(1)), 1)
            except Exception:
                pass
        return datetime(year, 1, 1)
    return None

# ---------------------------
# Fetching & scraping 
# ---------------------------
def fetch_with_requests(url, timeout=15):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.text
    except Exception:
        return ""

def fetch_with_playwright(url, wait=PLAYWRIGHT_WAIT_MS):
    if not USE_PLAYWRIGHT:
        return ""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")
            page.wait_for_timeout(wait)
            html = page.content()
            browser.close()
            return html
    except Exception:
        return ""

def extract_press_links(domain_html, base_url):
    soup = BeautifulSoup(domain_html, "html.parser")
    links = set()
    try:
        base_domain = base_url.split("//")[-1].split("/")[0]
    except Exception:
        base_domain = ""
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/"):
            href = requests.compat.urljoin(base_url, href)
        if base_domain and base_domain in href and ("press" in href.lower() or "news" in href.lower() or "presse" in href.lower()):
            links.add(href)
    return list(links)

def scrape_press_release_page(url, company_key=None):
    html = fetch_with_requests(url)
    if not html:
        html = fetch_with_playwright(url)
    if not html:
        return []
    title = ""
    text = ""
    publish_date = None
    try:
        art = Article(url)
        art.download(input_html=html)
        art.parse()
        title = art.title or ""
        text = art.text or ""
        if hasattr(art, "publish_date") and art.publish_date:
            publish_date = art.publish_date
    except Exception:
        pass
    if not text:
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find(["h1", "h2"])
        title = title_tag.get_text(strip=True) if title_tag else title
        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        text = "\n\n".join(paragraphs) if paragraphs else text
        time_tags = soup.find_all(["time"])
        if time_tags:
            for t in time_tags:
                if t.get("datetime"):
                    try:
                        publish_date = datetime.fromisoformat(t.get("datetime"))
                        break
                    except Exception:
                        pass
                txt = t.get_text(" ", strip=True)
                parsed = _parse_date_string(txt)
                if parsed:
                    publish_date = parsed
                    break
        if not publish_date:
            all_text = soup.get_text(" ", strip=True)
            parsed = _find_date_in_text(all_text)
            if parsed:
                publish_date = parsed
    return [{"url": url, "title": title, "text": text, "publish_date": publish_date, "company": company_key}]

def collect_press_releases(timeout_per_site=10):
    results = []
    for name, url in tqdm(TECDAX_COMPANIES.items(), desc="Companies"):
        if not url:
            continue
        html = fetch_with_requests(url)
        if not html:
            html = fetch_with_playwright(url)
        if not html:
            continue
        links = extract_press_links(html, url)
        candidate_urls = set([url]) | set(links)
        for u in list(candidate_urls)[:30]:
            time.sleep(0.25)
            results.extend(scrape_press_release_page(u, company_key=name))
    if os.path.isdir(LOCAL_FALLBACK_DIR):
        for fname in os.listdir(LOCAL_FALLBACK_DIR):
            path = os.path.join(LOCAL_FALLBACK_DIR, fname)
            try:
                if fname.lower().endswith(".pdf"):
                    results.append({"url": path, "title": fname, "text": "", "local": True, "publish_date": None, "company": _infer_company_from_filename(fname)})
                else:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        pd = _find_date_in_text(fname)
                        results.append({"url": path, "title": fname, "text": f.read(), "local": True, "publish_date": pd, "company": _infer_company_from_filename(fname)})
            except Exception:
                continue
    return results

def _infer_company_from_filename(fname):
    lname = fname.lower()
    for key in TECDAX_COMPANIES.keys():
        if key.lower() in lname:
            return key
    return ""

# ---------------------------
# Text processing & sentiment 
# ---------------------------
DetectorFactory.seed = 0
def detect_lang(text):
    try:
        return detect(text)
    except Exception:
        return "unknown"

def split_sentences(text, lang):
    try:
        if lang and lang.startswith("de"):
            return nltk.tokenize.sent_tokenize(text, language="german")
        else:
            return nltk.tokenize.sent_tokenize(text, language="english")
    except Exception:
        return [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if s.strip()]

def sentence_mentions_ai(sentence, lang):
    s = sentence.lower()
    keys = AI_KEYWORDS_DE if lang and lang.startswith("de") else AI_KEYWORDS_EN
    for k in keys:
        if k in s:
            return True
    return False

def extract_ai_sentences(text):
    lang = detect_lang(text)
    sents = split_sentences(text, lang)
    out = []
    for s in sents:
        if len(s) < MIN_SENTENCE_LEN:
            continue
        if sentence_mentions_ai(s, lang):
            out.append((lang, s.strip()))
    return out

analyzer_en = SentimentIntensityAnalyzer()

GERMAN_LEXICON = {
    "gut": 2.0, "positiv": 1.5, "erfolgreich": 1.5, "stark": 1.0,
    "stärken": 1.0, "verbessert": 1.2,
    "schwach": -1.0, "problem": -1.5, "kritisch": -1.5, "fehler": -1.5,
    "risiko": -1.5, "negativ": -1.5, "verlust": -1.5
}

def score_sentence(lang, sentence):
    if lang and lang.startswith("de"):
        s = sentence.lower()
        score = 0.0
        for w, val in GERMAN_LEXICON.items():
            if re.search(r'\b' + re.escape(w) + r'\b', s):
                score += val
        if re.search(r'\b(nicht|kein|keine|nie|ohne)\b', s):
            score = -score
        if score > 0:
            return min(score / 3.0, 1.0)
        elif score < 0:
            return max(score / 3.0, -1.0)
        else:
            return 0.0
    else:
        v = analyzer_en.polarity_scores(sentence)
        return v["compound"]

def label_from_score(score):
    if score >= SENTIMENT_THRESHOLDS["positive"]:
        return "positive", score
    elif score <= SENTIMENT_THRESHOLDS["negative"]:
        return "negative", score
    else:
        return "neutral", score

def aggregate_release(scores):
    if not scores:
        return {"label": "neutral", "score": 0.0, "confidence": 0.0, "count": 0}
    avg = sum(scores) / len(scores)
    non_neutral = sum(1 for s in scores if abs(s) > 0.05)
    conf = float(non_neutral) / float(len(scores))
    label, _ = label_from_score(avg)
    return {"label": label, "score": avg, "confidence": conf, "count": len(scores)}

# ---------------------------
# Runner 
# ---------------------------
def ensure_text(item):
    if item.get("text"):
        return item
    if (item.get("url","").lower().endswith(".pdf")) or (item.get("local", False) and item["url"].lower().endswith(".pdf")):
        try:
            txt = pdf_extract_text(item["url"])
            item["text"] = txt
            return item
        except Exception:
            item["text"] = ""
            return item
    item["text"] = ""
    return item

def extract_year_month(publish_date, text, title, url):
    if publish_date and isinstance(publish_date, datetime):
        return publish_date.year, publish_date.month
    parsed = _find_date_in_text(text or "")
    if parsed:
        return parsed.year, parsed.month
    parsed = _find_date_in_text(title or "")
    if parsed:
        return parsed.year, parsed.month
    parsed = _find_date_in_text(url or "")
    if parsed:
        return parsed.year, parsed.month
    return "", ""

def run_pipeline():
    releases = collect_press_releases()
    rows = []
    for r in tqdm(releases, desc="Processing releases"):
        r = ensure_text(r)
        text = r.get("text", "")
        if not text or len(text) < 50:
            continue
        ai_sents = extract_ai_sentences(text)
        sent_scores = []
        examples = {"positive": [], "neutral": [], "negative": []}
        for lang, sent in ai_sents:
            sc = score_sentence(lang, sent)
            lbl, _ = label_from_score(sc)
            sent_scores.append(sc)
            if len(examples[lbl]) < 3:
                examples[lbl].append(sent)
        agg = aggregate_release(sent_scores)
        year, month = extract_year_month(r.get("publish_date"), text, r.get("title",""), r.get("url",""))
        company = r.get("company") or ""
        row = {
            "company": company,
            "title": r.get("title",""),
            "url": r.get("url",""),
            "year": year,
            "month": month,
            "label": agg["label"],
            "score": round(agg["score"], 4),
            "confidence": round(agg["confidence"], 3),
            "ai_sentence_count": agg["count"],
            "example_positive": " | ".join(examples["positive"]),
            "example_neutral": " | ".join(examples["neutral"]),
            "example_negative": " | ".join(examples["negative"]),
        }
        rows.append(row)
    if not rows:
        print("No releases processed. Check config and local_releases/.")
        return
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print("Wrote {} results to {}".format(len(rows), OUTPUT_CSV))

if __name__ == "__main__":
    run_pipeline()