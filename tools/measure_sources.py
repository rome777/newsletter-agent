"""소스 후보를 실제로 재서 관문 세 개(교안 10장)로 채택·탈락을 가른다.

    python tools/measure_sources.py      # store/sources.json 에 저장하고 표를 출력한다

G1 본문  최근 기사 3건의 원문에서 본문이 600자 넘게 뽑히는가 (3건 중 2건 이상)
G2 생존  최근 24시간 안에 글이 있는가
G3 접근  기사 페이지가 200 으로 열리고 robots.txt 가 막지 않는가
주제     피드 제목 중 AI 관련 비율이 30% 이상인가 (종합 IT 피드가 후보를 AI 와 무관한 기사로 채우지 않게)
"""
import json, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import robotparser
from urllib.parse import urlsplit

import feedparser, requests, trafilatura

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from graph import UA, EXCLUDED_DOMAINS, MIN_BODY, published_at, strip_tags, is_excluded

CANDIDATES = [  # (이름, 피드, 언어, 1차/2차)
    # 과제가 제외하라고 한 소스 — 재기만 하고 쓰지 않는다
    ("OpenAI",          "https://openai.com/blog/rss.xml", "en", 1),
    ("DeepMind",        "https://deepmind.google/blog/rss.xml", "en", 1),
    ("TechCrunch",      "https://techcrunch.com/category/artificial-intelligence/feed/", "en", 2),
    ("The Verge",       "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml", "en", 2),
    ("MIT TR",          "https://www.technologyreview.com/feed/", "en", 2),
    ("AI타임스",         "https://www.aitimes.com/rss/allArticle.xml", "ko", 2),
    # 1차 소스 · 당사자 블로그
    ("Google AI",       "https://blog.google/technology/ai/rss/", "en", 1),
    ("Google Research", "https://research.google/blog/rss/", "en", 1),
    ("Hugging Face",    "https://huggingface.co/blog/feed.xml", "en", 1),
    ("NVIDIA",          "https://blogs.nvidia.com/feed/", "en", 1),
    ("AWS ML",          "https://aws.amazon.com/blogs/machine-learning/feed/", "en", 1),
    ("Microsoft Research", "https://www.microsoft.com/en-us/research/feed/", "en", 1),
    ("LangChain",       "https://blog.langchain.dev/rss/", "en", 1),
    # 2차 소스 · 영어
    ("Ars Technica",    "https://arstechnica.com/ai/feed/", "en", 2),
    ("Wired",           "https://www.wired.com/feed/tag/ai/latest/rss", "en", 2),
    ("The Decoder",     "https://the-decoder.com/feed/", "en", 2),
    ("MarkTechPost",    "https://www.marktechpost.com/feed/", "en", 2),
    ("InfoQ",           "https://feed.infoq.com/ai-ml-data-eng/", "en", 2),
    ("Simon Willison",  "https://simonwillison.net/atom/everything/", "en", 2),
    ("ZDNet",           "https://www.zdnet.com/topic/artificial-intelligence/rss.xml", "en", 2),
    ("SiliconANGLE",    "https://siliconangle.com/category/ai/feed/", "en", 2),
    ("Analytics India", "https://analyticsindiamag.com/feed/", "en", 2),
    ("VentureBeat",     "https://venturebeat.com/category/ai/feed/", "en", 2),
    ("The Register",    "https://www.theregister.com/software/ai_ml/headlines.atom", "en", 2),
    # 2차 소스 · 한국어
    ("GeekNews",        "https://news.hada.io/rss/news", "ko", 2),
    ("ZDNet Korea",     "https://feeds.feedburner.com/zdkorea", "ko", 2),
    ("전자신문",         "https://rss.etnews.com/Section901.xml", "ko", 2),
    ("인공지능신문",      "https://www.aitimes.kr/rss/allArticle.xml", "ko", 2),
    ("바이라인네트워크",   "https://byline.network/feed/", "ko", 2),
    ("블로터",           "https://www.bloter.net/rss/allArticle.xml", "ko", 2),
    ("요즘IT",           "https://yozm.wishket.com/magazine/feed/", "ko", 2),
]

AI_WORDS = re.compile(r"\bAI\b|A\.I\.|인공지능|생성형|LLM|GPT|챗봇|chatbot|에이전트|agent|모델|model|머신러닝|machine learning"
                      r"|딥러닝|deep learning|neural|Gemini|Claude|Llama|Copilot|transformer|추론|inference|학습|training", re.I)
MIN_AI_RATIO = 0.3

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

def measure(name, url, lang, tier):
    row = {"name": name, "url": url, "lang": lang, "tier": tier, "excluded": is_excluded(url)}
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
    row["ai_ratio"] = round(sum(bool(AI_WORDS.search(e.get("title", ""))) for e in entries) / len(entries), 2) if entries else 0
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
        failed = [g.upper() for g in ("g1", "g2", "g3") if not row[g]] + (["주제"] if row["ai_ratio"] < MIN_AI_RATIO else [])
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
    print(f"{'소스':<18}{'피드':>6}{'건수':>5}{'24h':>5}{'최신(h)':>8}{'AI비율':>6}{'RSS요약':>7}  본문(3건)          기사  robots  판정")
    for r in rows:
        print(f"{r['name']:<18}{str(r['status']):>6}{r['entries']:>5}{r['last_24h']:>5}{str(r['latest_h']):>8}{r['ai_ratio']:>6}"
              f"{r['rss_summary']:>7}  {str(r['body_lens']):<18}{str(r['article_status']):>5}  {str(r['robots_ok']):<6}  {r['decision']}")
    adopted = [r for r in rows if r["decision"] == "채택"]
    print(f"\n채택 {len(adopted)}곳 · 최근 24시간 {sum(r['last_24h'] for r in adopted)}건 (+ Hacker News)")
