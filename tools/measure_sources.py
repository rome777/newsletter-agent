"""소스 후보를 실제로 재서 관문 세 개(교안 10장)로 채택·탈락을 가른다.

    python tools/measure_sources.py      # store/sources.json 에 저장하고 표를 출력한다

G1 본문  최근 기사 3건의 원문에서 본문이 600자 넘게 뽑히는가 (3건 중 2건 이상)
G2 생존  최근 24시간 안에 글이 있는가
G3 접근  기사 페이지가 200 으로 열리고 robots.txt 가 막지 않는가
주제     피드 제목 중 "해외에서 실제로 일어난 기묘한 사건" 비율이 30% 이상인가 (LLM 이 제목마다 판정)
         종합 국제면 피드가 후보를 정치·사고 뉴스로 채우지 않게
"""
import json, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import robotparser
from urllib.parse import urlsplit

import feedparser, requests, trafilatura

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from graph import UA, MIN_BODY, MODEL, client, published_at, strip_tags, is_excluded
from pydantic import BaseModel, Field

CANDIDATES = [  # (이름, 피드, 언어, 구분)
    # 과제가 제외하라고 한 소스 — 재기만 하고 쓰지 않는다
    ("OpenAI",            "https://openai.com/blog/rss.xml", "en", "제외"),
    ("DeepMind",          "https://deepmind.google/blog/rss.xml", "en", "제외"),
    ("TechCrunch",        "https://techcrunch.com/category/artificial-intelligence/feed/", "en", "제외"),
    ("The Verge",         "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml", "en", "제외"),
    ("MIT TR",            "https://www.technologyreview.com/feed/", "en", "제외"),
    ("AI타임스",           "https://www.aitimes.com/rss/allArticle.xml", "ko", "제외"),
    # 기묘한 뉴스 전용 섹션 · 영어
    ("UPI Odd News",      "https://rss.upi.com/news/odd_news.rss", "en", "전용"),
    ("Sky News Strange",  "https://feeds.skynews.com/feeds/rss/strange.xml", "en", "전용"),
    ("Mirror Weird",      "https://www.mirror.co.uk/news/weird-news/?service=rss", "en", "전용"),
    ("Daily Star Weird",  "https://www.dailystar.co.uk/news/weird-news/?service=rss", "en", "전용"),
    ("Metro Weird",       "https://metro.co.uk/news/weird/feed/", "en", "전용"),
    ("Express Weird",     "https://www.express.co.uk/posts/rss/151/weird", "en", "전용"),
    ("CBC Offbeat",       "https://www.cbc.ca/cmlink/rss-offbeat", "en", "전용"),
    ("Global News Odd",   "https://globalnews.ca/tag/odd-news/feed/", "en", "전용"),
    ("NY Post Weird",     "https://nypost.com/tag/weird-but-true/feed/", "en", "전용"),
    ("Oddity Central",    "https://www.odditycentral.com/feed/", "en", "전용"),
    ("SoraNews24",        "https://soranews24.com/feed/", "en", "전용"),
    ("NPR Strange News",  "https://feeds.npr.org/1146/rss.xml", "en", "전용"),
    ("Irish Mirror Weird", "https://www.irishmirror.ie/news/weird-news/?service=rss", "en", "전용"),
    # 화제·바이럴 섹션 · 영어 (아시아 매체)
    ("HT Trending",       "https://www.hindustantimes.com/feeds/rss/trending/rssfeed.xml", "en", "화제"),
    ("Indian Express Trending", "https://indianexpress.com/section/trending/feed/", "en", "화제"),
    ("Mothership",        "https://mothership.sg/feed/", "en", "화제"),
    ("SCMP Trending China", "https://www.scmp.com/rss/322296/feed", "en", "화제"),
    ("Vice",              "https://www.vice.com/en/rss", "en", "화제"),
    # 잡학·화제 매체 · 영어
    ("Atlas Obscura",     "https://www.atlasobscura.com/feeds/latest", "en", "잡학"),
    ("Boing Boing",       "https://boingboing.net/feed", "en", "잡학"),
    ("Bored Panda",       "https://www.boredpanda.com/feed/", "en", "잡학"),
    ("Mental Floss",      "https://www.mentalfloss.com/rss.xml", "en", "잡학"),
    ("Good News Network", "https://www.goodnewsnetwork.org/feed/", "en", "잡학"),
    # 링크 모음 · 커뮤니티
    ("Fark",              "https://www.fark.com/fark.rss", "en", "모음"),
    ("Reddit r/offbeat",  "https://www.reddit.com/r/offbeat/top/.rss?t=day", "en", "모음"),
    ("Reddit r/nottheonion", "https://www.reddit.com/r/nottheonion/top/.rss?t=day", "en", "모음"),
    # 국제면 · 한국어 — 해외 화제 전용 피드를 찾지 못해 국제면 전체를 잰다
    ("서울신문 국제",       "https://www.seoul.co.kr/xml/rss/rss_international.xml", "ko", "종합"),
    ("연합뉴스 국제",       "https://www.yna.co.kr/rss/international.xml", "ko", "종합"),
    ("뉴시스 국제",         "https://www.newsis.com/RSS/international.xml", "ko", "종합"),
    ("동아일보 국제",       "https://rss.donga.com/international.xml", "ko", "종합"),
    ("국민일보 국제",       "https://www.kmib.co.kr/rss/data/kmibIntRss.xml", "ko", "종합"),
    ("경향신문 국제",       "https://www.khan.co.kr/rss/rssdata/kh_world.xml", "ko", "종합"),
]

class Odd(BaseModel):
    index: int  = Field(description="제목 번호")
    odd:   bool = Field(description="해외에서 실제로 일어난 기묘하거나 황당하거나 신기한 사건을 보도한 기사면 true")

class OddList(BaseModel):
    items: list[Odd]

SYS_ODD = ("아래 기사 제목마다 '해외에서 실제로 일어난 기묘하거나 황당하거나 신기한 사건을 보도한 기사'인지 판정하세요.\n"
           "false: 정치·경제·전쟁·재난 같은 일반 뉴스, 몇십 개를 모은 목록형 글, 퀴즈·운세·광고·쇼핑, "
           "만드는 법·여행지 소개, 한국 국내 사건, 기술·AI 뉴스. 모든 번호에 답하세요.")
MIN_ODD_RATIO = 0.3                              # AI 브리핑 때의 주제 기준선과 같게 둔다. 판정이 매체마다 흔들려 50%면 같은 출판사 섹션끼리도 갈린다

def odd_ratio(titles):                           # 제목을 LLM 에 한 번에 보여 주고 한 건씩 판정받는다
    titles = titles[:30]
    if not titles:
        return 0
    listing = "\n".join(f"{i}. {t}" for i, t in enumerate(titles))
    out = client().chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS_ODD}, {"role": "user", "content": listing}],
        response_format=OddList).choices[0].message.parsed
    odd = {o.index for o in out.items if o.odd and 0 <= o.index < len(titles)}
    return round(len(odd) / len(titles), 2)

def robots_ok(url):
    p = urlsplit(url)
    rp = robotparser.RobotFileParser()
    try:
        r = requests.get(f"{p.scheme}://{p.netloc}/robots.txt", headers=UA, timeout=10)
        if r.status_code >= 400:
            return True                          # robots.txt 가 없으면 막지 않은 것
        rp.parse(r.text.splitlines())
        return rp.can_fetch(UA["User-Agent"], url)
    except requests.RequestException:
        return True

def measure(name, url, lang, kind):
    row = {"name": name, "url": url, "lang": lang, "kind": kind, "excluded": is_excluded(url)}
    t0 = time.time()
    try:
        r = requests.get(url, headers=UA, timeout=20)
        row["status"] = r.status_code
        entries = feedparser.parse(r.content).entries if r.ok else []
    except requests.RequestException as e:
        row["status"], entries = type(e).__name__, []
    now = datetime.now(timezone.utc)
    dates = [d for d in (published_at(e) for e in entries) if d]
    row["entries"] = len(entries)
    row["last_24h"] = sum(d >= now - timedelta(hours=24) for d in dates)
    row["latest_h"] = round((now - max(dates)).total_seconds() / 3600, 1) if dates else None
    row["odd_ratio"] = odd_ratio([e.get("title", "") for e in entries])
    row["rss_summary"] =(sum(len(strip_tags(e.get("summary", ""))) for e in entries[:3]) // max(len(entries[:3]), 1)
                          if entries else 0)
    bodies, article = [], None
    for e in entries[:3]:
        link = e.get("link")
        if not link:
            continue
        article = article or link
        d = trafilatura.fetch_url(link)
        body = trafilatura.extract(d) if d else None
        bodies.append(len(body or ""))
    row["body_lens"] = bodies
    row["g1"] = sum(n > MIN_BODY for n in bodies) >= 2
    row["g2"] = row["last_24h"] > 0
    if article:
        try:
            row["article_status"] = requests.get(article, headers=UA, timeout=15).status_code
        except requests.RequestException as e:
            row["article_status"] = type(e).__name__
        row["robots_ok"] = robots_ok(article)
    else:
        row["article_status"], row["robots_ok"] = None, None
    row["g3"] = row["article_status"] == 200 and bool(row["robots_ok"])
    row["seconds"] = round(time.time() - t0, 1)
    if row["excluded"]:
        row["decision"] = "제외(과제)"
    elif row["status"] != 200 or not entries:
        row["decision"] = "탈락(피드 없음)"
    else:
        failed = [g.upper() for g in ("g1", "g2", "g3") if not row[g]] + (["주제"] if row["odd_ratio"] < MIN_ODD_RATIO else [])
        row["decision"] = "채택" if not failed else "탈락(" + "·".join(failed) + ")"
    return row

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    with ThreadPoolExecutor(8) as ex:
        rows = list(ex.map(lambda c: measure(*c), CANDIDATES))
    out = Path(__file__).resolve().parent.parent / "store" / "sources.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps({"measured_at": datetime.now().isoformat(timespec="minutes"), "rows": rows},
                              ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{'소스':<18}{'피드':>6}{'건수':>5}{'24h':>5}{'최신(h)':>8}{'기묘비율':>6}{'RSS요약':>7}  본문(3건)          기사  robots  판정")
    for r in rows:
        print(f"{r['name']:<18}{str(r['status']):>6}{r['entries']:>5}{r['last_24h']:>5}{str(r['latest_h']):>8}{r['odd_ratio']:>6}"
              f"{r['rss_summary']:>7}  {str(r['body_lens']):<18}{str(r['article_status']):>5}  {str(r['robots_ok']):<6}  {r['decision']}")
    adopted = [r for r in rows if r["decision"] == "채택"]
    print(f"\n채택 {len(adopted)}곳 · 최근 24시간 {sum(r['last_24h'] for r in adopted)}건")
