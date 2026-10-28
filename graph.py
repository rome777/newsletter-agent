"""D00 뉴스레터 에이전트 — State · 노드 다섯 개 · build() · run()

불러오기만 해서는 아무 일도 일어나지 않는다. 실행은 run.py 에서 한다.
교안 코드에서 고친 곳은 [고침] 주석으로 표시했다.
"""
import json, math, operator, os, random, re, time
from collections import Counter
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Annotated, Literal, TypedDict
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser, requests, trafilatura
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send
from openai import OpenAI
from pydantic import BaseModel, Field

try:                                            # 로컬에선 .env, Actions 에선 Secrets → env
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).with_name(".env"))
except ImportError:
    pass

MODEL = "gpt-4.1-mini"
_client = None

def client():                                   # import 만 해도 키가 필요하지 않게 늦게 만든다
    global _client
    if _client is None:
        _client = OpenAI()                      # 키는 OPENAI_API_KEY 환경변수에서 읽는다
    return _client


# ── State ──────────────────────────────────────────────────────────────
class Brief(TypedDict):
    hours:     int                              # 수집 time window(시간)
    collected: list                             # ① 수집한 기사
    picked:    list                             # ② 선별해 남긴 다섯 건 + 예비 (rank 순)
    drafted:   Annotated[list, operator.add]    # ③ 취재한 초안 — 워커들이 나눠 채운다
    verified:  list                             # ④ 검수를 통과한 것
    sent:      str                              # ⑤ 발행 결과 — sent · dry-run · failed  [고침]
    trace:     dict                             # ② 왜 골랐고 왜 떨어졌는지 — run.py 가 파일로 남긴다  [고침]
    log:       Annotated[list, operator.add]    # 무슨 일이 있었는지

INIT = {"hours": 24, "collected": [], "picked": [], "drafted": [],
        "verified": [], "sent": "", "trace": {}, "log": []}


# ── ① 수집 · collect ───────────────────────────────────────────────────
UA = {"User-Agent": "Mozilla/5.0 (newsletter-agent-course)"}
SOURCES = [                                     # 하루 100건 이상이 되도록 넓혔다 (2026-10-28 실측 약 150건)
    # 1차 소스 · 당사자 블로그
    ("OpenAI",         "https://openai.com/blog/rss.xml"),
    ("DeepMind",       "https://deepmind.google/blog/rss.xml"),
    ("Google AI",      "https://blog.google/technology/ai/rss/"),
    ("Hugging Face",   "https://huggingface.co/blog/feed.xml"),
    ("NVIDIA",         "https://blogs.nvidia.com/feed/"),
    ("AWS ML",         "https://aws.amazon.com/blogs/machine-learning/feed/"),
    # 2차 소스 · 영어
    ("TechCrunch",     "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("The Verge",      "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"),
    ("Ars Technica",   "https://arstechnica.com/ai/feed/"),
    ("Wired",          "https://www.wired.com/feed/tag/ai/latest/rss"),
    ("MIT TR",         "https://www.technologyreview.com/topic/artificial-intelligence/feed"),
    ("The Decoder",    "https://the-decoder.com/feed/"),
    ("MarkTechPost",   "https://www.marktechpost.com/feed/"),
    ("InfoQ",          "https://feed.infoq.com/ai-ml-data-eng/"),
    ("Simon Willison", "https://simonwillison.net/atom/everything/"),
    ("ZDNet",          "https://www.zdnet.com/topic/artificial-intelligence/rss.xml"),
    ("SiliconANGLE",   "https://siliconangle.com/category/ai/feed/"),
    ("Analytics India","https://analyticsindiamag.com/feed/"),
    # 2차 소스 · 한국어 — GeekNews 는 기사 페이지가 403 이라 G3(접근)에서 탈락
    ("AI타임스",        "https://www.aitimes.com/rss/allArticle.xml"),
]
HN_MIN_POINTS = 30                              # Hacker News 는 공개 API(사다리 ①칸)로 가져온다

def strip_tags(s):
    return re.sub(r"<[^>]+>", "", s or "").strip()

def published_at(entry):                        # [고침] Atom 의 updated 도 본다
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    return datetime(*t[:6], tzinfo=timezone.utc) if t else None

def url_key(url):                              # 추적용 꼬리표(utm_* 등)만 떼고 비교한다
    p = urlsplit(url)                           # [고침] ?id= 처럼 글을 가리키는 값은 남긴다
    q = [(k, v) for k, v in parse_qsl(p.query) if not k.startswith("utm_") and k not in ("ref", "source")]
    return urlunsplit((p.scheme, p.netloc.lower(), p.path.rstrip("/"), urlencode(q), ""))

def rss_entries(name, url, cutoff):
    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()                        # [고침] 403·404·500 도 실패로 잡는다
    for e in feedparser.parse(r.content).entries:
        at = published_at(e)
        if at and at >= cutoff:                 # time window 밖이거나 날짜가 없으면 버린다
            yield {"title": e.title, "url": e.link, "source": name, "at": at,
                   "summary": strip_tags(e.get("summary", ""))[:300]}

def hn_entries(cutoff):
    r = requests.get("https://hn.algolia.com/api/v1/search_by_date", timeout=20,
                     params={"tags": "story", "query": "AI", "hitsPerPage": 100,
                             "numericFilters": f"created_at_i>{int(cutoff.timestamp())},"
                                               f"points>={HN_MIN_POINTS}"})
    r.raise_for_status()
    for h in r.json()["hits"]:
        yield {"title": h["title"], "source": "Hacker News",
               "url": h.get("url") or f"https://news.ycombinator.com/item?id={h['objectID']}",
               "at": datetime.fromtimestamp(h["created_at_i"], timezone.utc),
               "summary": f"HN {h.get('points', 0)}점 · 댓글 {h.get('num_comments', 0)}개"}

def collect(s: dict) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=s["hours"])
    fetchers = [(name, lambda n=name, u=url: rss_entries(n, u, cutoff)) for name, url in SOURCES]
    fetchers.append(("Hacker News", lambda: hn_entries(cutoff)))
    items, dead, empty, seen = [], [], [], set()
    for name, fetch in fetchers:
        try:
            entries = list(fetch())
        except Exception:
            dead.append(name)                   # 한 곳이 죽어도 나머지는 계속
            continue
        n = 0
        for it in entries:
            key = url_key(it["url"])
            if key in seen:
                continue
            seen.add(key)
            items.append(it)
            n += 1
        if n == 0:
            empty.append(name)                  # [고침] 0건인 소스도 이름을 남긴다
    return {"collected": items,
            "log": [f"① 수집   {s['hours']}시간 time window · {len(items)}건 · 소스 {len(fetchers)}곳"
                    + (f" · 응답 없음 {dead}" if dead else "")
                    + (f" · 0건 {empty}" if empty else "")]}


# ── ② 선별 · select ────────────────────────────────────────────────────
BATCH, MIN_HEATS = 40, 2                        # 예선 묶음 최대 크기, 최소 묶음 수 — 예선은 반드시 치른다
PASS, TARGET, SPARE = 8, 5, 3                   # 묶음당 통과, 최종 건수, 본선 예비 — 길게 받으면 비교 대신 목록을 베낀다
MAX_PER_SOURCE = 2                              # 한 매체가 브리핑을 독차지하지 않게 (20장)

CRITERIA = ("독자는 AI를 실제 제품에 붙이는 국내 개발팀입니다.\n"
            "고를 것:\n"
            "- 이번 주 일하는 방식이 바뀔 만한가\n"
            "- 지금 쓰는 도구·API의 가격·한도·정책이 실제로 변했나\n"
            "버릴 것 (절대 고르지 마세요):\n"
            "- 발표 예정·로드맵·예고만 있고 지금 쓸 수 있는 게 없는 소식\n"
            "- 투자 유치·펀딩·기업 가치·MOU·제휴 체결·수상 같은 홍보성 소식\n"
            "독자는 영어 기사도 읽습니다. 기사 언어나 국내 회사 여부는 판단 기준이 아닙니다.")

KINDS = ("출시·변경", "연구·기법", "사례·적용", "규제·정책", "로드맵·예고", "투자·MOU·수상", "기타")
DROP_KINDS = {"로드맵·예고", "투자·MOU·수상"}   # [고침] '버릴 것'을 프롬프트 부탁이 아니라 코드로 강제한다

class Pick(BaseModel):                          # [고침] 제목을 먼저 옮겨 적게 해 이유가 엉뚱한 기사에 붙지 않게 한다
    title: str = Field(description="고른 후보의 제목을 목록에 있는 그대로 복사")
    index: int = Field(description="그 후보의 목록 번호")
    event: str = Field(description="사건 라벨. 영어 소문자 '회사-대상' 형식(예: openai-gpt-6.1). "
                                   "같은 사건이면 기사 언어·매체가 달라도 같은 라벨")
    kind: Literal[KINDS] = Field(description="기사 성격. 투자·펀딩·MOU·수상이면 '투자·MOU·수상', "
                                             "아직 안 나온 계획·예고면 '로드맵·예고'")
    reason: str = Field(description="독자 기준으로 왜 골랐는지 한국어 한 문장")

class Shortlist(BaseModel):
    picks: list[Pick]

def ask_picks(items, n, extra=""):
    listing = "\n".join(f"{i}. [{it['source']}] {it['title']}"          # [고침] 제목만이 아니라 요약도 본다
                        + (f"\n    {it['summary'][:200]}" if it.get("summary") else "")
                        for i, it in enumerate(items))
    sys = (f"{CRITERIA}\n\n아래 후보의 제목과 요약을 함께 읽고, 중요한 순서대로 {n}건을 고르세요.\n"
           "목록 순서는 중요도와 무관합니다. 전부 비교한 뒤 가장 중요한 것부터 답하세요.\n"
           "같은 사건을 다룬 기사에는 같은 event 라벨을 붙이세요.\n"
           "kind는 기사 성격 그대로 정직하게 분류하세요." + (f"\n{extra}" if extra else ""))
    out = client().chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": sys},
                  {"role": "user", "content": listing}],
        response_format=Shortlist).choices[0].message.parsed
    picks, used = [], set()
    for p in out.picks:
        i = resolve(items, p)
        if i is None or i in used:              # 목록에 없는 기사와 같은 기사 두 번은 버린다
            continue
        used.add(i)
        p.index = i
        picks.append(p)
    return picks

def resolve(items, p):                          # 번호보다 옮겨 적은 제목을 믿는다 — 모델은 긴 목록에서 번호를 틀린다
    norm = lambda t: re.sub(r"\s+", " ", t).strip().lower()
    sim = lambda i: SequenceMatcher(None, norm(items[i]["title"]), norm(p.title)).ratio()
    if 0 <= p.index < len(items) and sim(p.index) >= 0.6:
        return p.index
    best = max(range(len(items)), key=sim)      # 번호가 틀렸으면 제목이 가장 비슷한 후보를 찾는다
    return best if sim(best) >= 0.6 else None

GENERIC = {"launch", "launches", "release", "releases", "released", "announce", "announces",
           "announcement", "update", "new", "출시", "공개", "발표"}

def event_key(label):                           # [고침] 라벨이 '출시'·'launch'처럼 달라도 같은 사건으로 본다
    words = re.findall(r"[0-9a-z가-힣.]+", label.lower())
    return "-".join(w for w in words if w not in GENERIC)

def same_event(a, b):
    ka, kb = event_key(a), event_key(b)
    if not ka or not kb:                        # [고침] 라벨을 비워 오면 빈 것끼리 같은 사건이 돼 버린다
        return False
    return SequenceMatcher(None, ka, kb).ratio() >= 0.8

PROMO = re.compile(                             # [고침] 제목만 봐도 홍보성인 기사는 LLM 에 넘기기 전에 거른다
    r"\braises? \$|\bseries [a-f]\b|\bfunding\b|\bseed round\b|\bvaluation\b|\bunicorn\b"
    r"|\bmou\b|\bwins?\b.*\baward|투자\s?유치|시리즈\s?[a-f]\b|펀딩|기업\s?가치|업무\s?협약|협약\s?체결|수상|시상"
    r"|조달|금융\s?지원|^\[게시판\]",               # [게시판] 은 AI타임스의 보도자료 단신 모음
    re.I)

def heats(items):                               # [고침] 후보가 적어도 묶음을 둘 이상 만든다
    n = max(MIN_HEATS, math.ceil(len(items) / BATCH))
    mixed = items[:]
    random.Random(datetime.now().strftime("%Y-%m-%d")).shuffle(mixed)   # 소스가 한 묶음에 몰리지 않게
    return [h for h in (mixed[k::n] for k in range(n)) if h]

def brief(it, p=None, result=None):             # 기록용 한 줄 — 왜 골랐고 어떻게 됐는지
    row = {"source": it["source"], "title": it["title"], "url": it["url"]}
    if p:
        row |= {"kind": p.kind, "event": p.event, "reason": p.reason}
    if result:
        row["result"] = result
    return row

def select(s: dict) -> dict:
    items = s["collected"]
    if not items:                               # [고침] 후보가 없으면 LLM 을 부르지 않는다
        return {"picked": [], "trace": {"candidates": 0}, "log": ["② 선별   0 → 0건"]}

    promo = [it for it in items if PROMO.search(it["title"])]
    pool = [it for it in items if not PROMO.search(it["title"])]

    survivors, trace_heats, dropped = [], [], 0
    for chunk in heats(pool):                   # 예선 — 묶음마다 여덟 건
        rows = []
        for p in ask_picks(chunk, PASS):
            ok = p.kind not in DROP_KINDS
            dropped += not ok
            rows.append(brief(chunk[p.index], p, "통과" if ok else f"버림({p.kind})"))
            if ok:
                survivors.append(chunk[p.index])
        trace_heats.append({"size": len(chunk), "picks": rows})

    random.Random(len(survivors)).shuffle(survivors)   # [고침] 몇 조 출신인지가 순위에 새지 않게 섞는다
    picked, events, per_src, rows, dup, ranked = [], [], Counter(), [], 0, set()
    for _ in range(3):                          # 본선 — 걸러져서 다섯 + 예비가 안 차면 남은 후보로 다시
        remaining = [it for it in survivors     # 상한이 찬 매체는 다음 판에 올리지 않는다
                     if id(it) not in ranked and per_src[it["source"]] < MAX_PER_SOURCE]
        if len(picked) >= TARGET + SPARE or not remaining:
            break
        finals = ask_picks(remaining, TARGET - len(picked) + SPARE,
                           f"같은 매체(대괄호 안 이름)에서는 최대 {MAX_PER_SOURCE}건까지만 고르세요.")
        for p in finals:
            it = remaining[p.index]
            ranked.add(id(it))
            if len(picked) == TARGET + SPARE:
                result = "남음"
            elif p.kind in DROP_KINDS:
                result, dropped = f"버림({p.kind})", dropped + 1
            elif any(same_event(p.event, e) for e in events):   # [고침] 같은 사건은 한 건만
                result, dup = "같은 사건", dup + 1
            elif per_src[it["source"]] >= MAX_PER_SOURCE:
                result = "매체 상한"
            else:                               # [고침] 예비도 같은 규칙으로 뽑아 두고 빈자리에 쓴다
                result = "선정" if len(picked) < TARGET else "예비"
                events.append(p.event)
                per_src[it["source"]] += 1
                picked.append({**it, "rank": len(picked), "event": p.event, "kind": p.kind,
                               "reason": p.reason})
            rows.append(brief(it, p, result))
    trace = {"candidates": len(items), "promo": [brief(it) for it in promo],
             "heats": trace_heats, "final": rows,
             "final_out": [brief(it) for it in survivors if id(it) not in ranked]}

    spare = len(picked[TARGET:])
    log = [f"② 선별   {len(items)} → 홍보 제외 {len(pool)} → 예선 {len(trace_heats)}조 {len(survivors)}"
           f" → {len(picked) - spare}건" + (f" + 예비 {spare}" if spare else "")
           + (f" · 버림 {dropped}" if dropped else "") + (f" · 같은 사건 {dup}" if dup else "")]
    log += [f"   {'선정' if it['rank'] < TARGET else '예비'} [{it['source']}] {it['title'][:50]} — {it['reason']}"
            for it in picked]
    return {"picked": picked, "trace": trace, "log": log}


# ── ③ 취재·요약 · report ───────────────────────────────────────────────
MIN_BODY = 600                                  # G1 기준선 (11장)
TOPICS = ("모델·API", "도구·프레임워크", "정책·규제", "사례·적용", "연구")

class Draft(BaseModel):
    headline: str = Field(description="30자 이내의 한국어 헤드라인")
    summary:  str = Field(description="한국어 세 문장 요약. 모든 문장을 '~합니다'로 끝낸다")
    why:      str = Field(description="국내 개발팀에게 왜 중요한지 한국어 한 문장, 80자 이내")
    topic:    Literal[TOPICS] = Field(description="기사 분류")      # [고침] 카드 색에 쓴다

SYS = ("당신은 국내 개발팀을 위한 AI 뉴스레터 기자입니다.\n"
       "아래 기사 본문을 읽고 헤드라인·요약·왜 중요한지를 반드시 한국어로 쓰세요.\n"
       "- 요약은 정확히 세 문장, 문장 끝은 모두 '~합니다'·'~했습니다' (~다체 금지)\n"
       "- why는 한 문장. 개발팀이 무엇을 확인하거나 바꿔야 하는지 구체적으로\n"
       "'주목된다·기대를 모은다·시사한다' 같은 기자체 표현은 쓰지 마세요.")

class ReportIn(TypedDict):                      # 워커가 받는 것은 기사 하나뿐
    item: dict

def extract_body(url):
    d = trafilatura.fetch_url(url)
    return trafilatura.extract(d) if d else None

def draft(body):
    return client().chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS},
                  {"role": "user", "content": body[:6000]}],
        response_format=Draft).choices[0].message.parsed

def fan_report(s: dict):                        # 기사 수만큼 워커를 펼친다
    return [Send("report", {"item": it}) for it in s["picked"]] or "verify"   # [고침] 0건이면 바로 검수로

def report(s: ReportIn) -> dict:
    it = s["item"]
    try:
        body = extract_body(it["url"])
        if not body or len(body) < MIN_BODY:
            return {"drafted": [],
                    "log": [f"   취재 제외 {it['source']} · 본문 {len(body or '')}자"]}
        d = draft(body)
    except Exception as e:                      # [고침] 한 갈래가 터져도 나머지는 계속
        return {"drafted": [], "log": [f"   취재 실패 {it['source']} · {type(e).__name__}"]}
    return {"drafted": [{**it, "body": body[:6000], **d.model_dump()}]}


# ── ④ 검수 · verify ────────────────────────────────────────────────────
class Verdict(BaseModel):
    ok:       bool      = Field(description="요약이 원문에 근거하면 true")
    problems: list[str] = Field(description="근거 없는 부분. 없으면 빈 목록")

SYS_CHECK = ("요약이 원문에서 뒷받침되는지 판정하세요.\n"
             "헤드라인과 요약만 보고 판단하고, 번역이나 단위 환산은 문제가 아닙니다.")

def check(d):                                   # 요약을 쓴 호출과 따로 부른다
    user = (f"[원문]\n{d['body'][:5000]}\n\n"
            f"[헤드라인]\n{d['headline']}\n\n[요약]\n{d['summary']}")
    return client().chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS_CHECK},
                  {"role": "user", "content": user}],
        response_format=Verdict).choices[0].message.parsed

def is_korean(d):                               # [고침] 24장 한글 검사를 연결한다
    return bool(re.search(r"[가-힣]", d["headline"] + d["summary"]))

def verify(s: dict) -> dict:
    kept, dropped = [], []
    for d in sorted(s["drafted"], key=lambda d: d["rank"]):   # [고침] 순위대로 보고 다섯이 차면 멈춘다
        if len(kept) == TARGET:
            break
        if not is_korean(d):
            dropped.append(f"{d['source']}(영어)")
            continue
        try:
            ok = check(d).ok
        except Exception as e:                  # [고침] 검수 호출이 터져도 그 기사만 빼고 계속
            dropped.append(f"{d['source']}({type(e).__name__})")
            continue
        if ok:
            kept.append(d)
        else:
            dropped.append(d["source"])
    spare = sum(d["rank"] >= TARGET for d in kept)
    return {"verified": kept,                   # 리듀서 없는 칸에 담는다
            "log": [f"④ 검수   {len(s['drafted'])} → {len(kept)}건"
                    + (f" · 불합격 {dropped}" if dropped else "")
                    + (f" · 예비 투입 {spare}" if spare else "")]}


# ── ⑤ 발행 · publish ───────────────────────────────────────────────────
COLORS = {"모델·API": 0x0B6E77, "도구·프레임워크": 0x4C7C9C,
          "정책·규제": 0x8F5606, "사례·적용": 0x2E7D5B, "연구": 0x6A4A9C}
DEFAULT = 0x5F7476
TITLE_MAX, DESC_MAX, EMBED_MAX, TOTAL_MAX = 256, 4096, 10, 5800   # 6000에서 여유를 둔다

def build_embeds(run_id, lead, articles):
    if not articles:                            # 조용한 날에도 한 장은 보낸다
        return [{"title": f"🗞️ {run_id}", "color": DEFAULT,
                 "description": "오늘은 조용합니다."}]
    embeds = [{"title": f"🗞️ {run_id} · AI 브리핑", "description": lead, "color": DEFAULT}]
    for i, a in enumerate(articles, 1):
        desc = a["summary"]
        if a.get("why"):
            desc += f"\n\n💡 **{a['why']}**"
        embeds.append({
            "title":       f"{i}. {a['headline']}"[:TITLE_MAX],
            "description": desc[:DESC_MAX],
            "url":         a["url"],
            "color":       COLORS.get(a.get("topic", ""), DEFAULT),
            "footer":      {"text": f"{a['source']} · {a['when']}"},
        })
    total = lambda es: sum(len(e.get("title", "")) + len(e.get("description", ""))
                           + len(e.get("footer", {}).get("text", "")) for e in es)
    while len(embeds) > EMBED_MAX or total(embeds) > TOTAL_MAX:
        embeds.pop()                            # 뒤에서부터 덜어낸다
    return embeds

SUPPRESS_NOTIFICATIONS = 1 << 12              # @silent — 푸시·데스크톱 알림을 안 보낸다
TRIES = 3

def post(webhook, payload):                     # [고침] 429·5xx·연결 실패면 기다렸다 다시 보낸다
    for n in range(1, TRIES + 1):
        try:
            r = requests.post(webhook, params={"wait": "true"}, json=payload, timeout=20)
        except requests.ConnectionError:        # 닿기 전에 끊긴 것이라 다시 보내도 두 번 올라가지 않는다
            if n == TRIES:
                raise
            time.sleep(5)
            continue
        if n == TRIES or (r.status_code != 429 and r.status_code < 500):
            return r
        time.sleep(min(float(r.headers.get("Retry-After") or 5), 60))   # 429 면 Discord 가 기다릴 초를 알려준다

def send(run_id, lead, articles, webhook=None, dry_run=True, silent=False):
    """보냈으면 'sent', 안 보냈으면 'dry-run', 거절당했으면 'failed'"""
    payload = {"username": "편집실", "embeds": build_embeds(run_id, lead, articles)}
    if silent:
        payload["flags"] = SUPPRESS_NOTIFICATIONS
    if dry_run or not webhook:
        print(f"[dry-run] embed {len(payload['embeds'])}개 · "
              f"{len(json.dumps(payload, ensure_ascii=False))}자 — 보내지 않음")
        for e in payload["embeds"]:
            print(json.dumps(e, ensure_ascii=False, indent=2))
        return "dry-run"
    try:
        r = post(webhook, payload)
    except requests.RequestException as e:      # 응답을 못 받았다 — 올라갔을 수도 있어 다시 보내지 않는다
        print(f"발행 실패 {type(e).__name__} — 채널에 올라갔는지 확인하세요")
        return "failed"
    if r.status_code in (200, 204):              # wait=true 면 200 과 함께 메시지가 돌아온다
        print(f"발행 메시지 ID {r.json()['id']}" + (" · silent" if silent else ""))
        return "sent"
    print(f"발행 실패 {r.status_code} {r.text[:120]}")
    return "failed"

def delete_message(message_id, webhook=None):  # 같은 웹훅으로 보낸 메시지를 지운다
    webhook = webhook or os.environ["DISCORD_WEBHOOK_URL"]
    r = requests.delete(f"{webhook.split('?')[0]}/messages/{message_id}", timeout=20)
    return r.status_code == 204

def make_lead(arts):
    if not arts:
        return ""
    srcs = ", ".join(dict.fromkeys(a["source"] for a in arts))
    return f"오늘은 {len(arts)}건을 골랐습니다. ({srcs})"

def publish(s: dict) -> dict:
    arts = [{"headline": a["headline"], "summary": a["summary"], "why": a["why"],
             "url": a["url"], "source": a["source"], "topic": a.get("topic", ""),
             "when": a["at"].astimezone().strftime("%m-%d %H:%M")}   # [고침] 현지 시각으로
            for a in s["verified"]]
    today = datetime.now().strftime("%Y-%m-%d")
    sent  = send(today, make_lead(arts), arts,
                 webhook=os.environ.get("DISCORD_WEBHOOK_URL"),
                 dry_run=os.environ.get("DRY_RUN", "1") == "1",   # 기본은 보내지 않음
                 silent=os.environ.get("SILENT", "0") == "1")     # 테스트 발행은 알림 없이
    label = f"{len(arts)}건" if arts else "조용합니다"
    return {"sent": sent, "log": [f"⑤ 발행   {label} · {sent}"]}


# ── 그래프 ──────────────────────────────────────────────────────────────
def build():
    g = StateGraph(Brief)
    g.add_node("collect", collect)
    g.add_node("select", select)
    g.add_node("report", report)
    g.add_node("verify", verify)
    g.add_node("publish", publish)
    g.add_edge(START, "collect")
    g.add_edge("collect", "select")
    g.add_conditional_edges("select", fan_report, ["report", "verify"])
    g.add_edge("report", "verify")
    g.add_edge("verify", "publish")
    g.add_edge("publish", END)
    return g

def run(hours=24):
    return build().compile().invoke({**INIT, "hours": hours})
