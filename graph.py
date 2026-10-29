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

import feedparser, requests, trafilatura, yaml
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
AUD = yaml.safe_load(Path(__file__).with_name("audience.yaml").read_text(encoding="utf-8"))   # 독자·기준·제외 조건
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
    rejected:  list                             # ④ 검수에서 떨어진 것과 이유 — metrics 에 남긴다
    sent:      str                             # ⑤ 발행 결과 — sent · dry-run · failed  [고침]
    trace:     dict                             # ② 왜 골랐고 왜 떨어졌는지 — run.py 가 파일로 남긴다  [고침]
    log:       Annotated[list, operator.add]    # 무슨 일이 있었는지

INIT = {"hours": 24, "collected": [], "picked": [], "drafted": [],
        "verified": [], "rejected": [], "sent": "", "trace": {}, "log": []}


# ── ① 수집 · collect ───────────────────────────────────────────────────
UA = {"User-Agent": "Mozilla/5.0 (newsletter-agent-course)"}
SOURCES = [                                     # tools/measure_sources.py 로 후보 38곳을 재서 관문을 다 넘은 7곳 (REPORT.md 소스 채택표)
    # 기묘한 뉴스 전용 섹션 · 영국
    ("Daily Star",       "https://www.dailystar.co.uk/news/weird-news/?service=rss"),
    ("Metro",            "https://metro.co.uk/news/weird/feed/"),
    # 화제 섹션 · 인도
    ("Hindustan Times",  "https://www.hindustantimes.com/feeds/rss/trending/rssfeed.xml"),
    ("Indian Express",   "https://indianexpress.com/section/trending/feed/"),
    # 화제·잡학 · 미국
    ("Vice",             "https://www.vice.com/en/rss"),
    ("Good News Network","https://www.goodnewsnetwork.org/feed/"),
    # 국제면 · 한국어 — 해외 화제 전용 피드는 찾지 못했고, 국제면 중 기묘한 기사 비율이 30%를 넘은 곳
    ("서울신문",          "https://www.seoul.co.kr/xml/rss/rss_international.xml"),
]

EXCLUDED_DOMAINS = AUD["excluded_domains"]      # 과제가 제외한 소스

def is_excluded(url):                           # www. 나 하위 도메인까지 같은 매체로 본다
    host = urlsplit(url).netloc.lower().removeprefix("www.")
    return any(host == d or host.endswith("." + d) for d in EXCLUDED_DOMAINS)

assert not [n for n, u in SOURCES if is_excluded(u)], "SOURCES 에 제외 소스가 들어 있다"

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

def collect(s: dict) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=s["hours"])
    fetchers = [(name, lambda n=name, u=url: rss_entries(n, u, cutoff)) for name, url in SOURCES]
    items, dead, empty, seen, excluded = [], [], [], set(), 0
    for name, fetch in fetchers:
        try:
            entries = list(fetch())
        except Exception:
            dead.append(name)                   # 한 곳이 죽어도 나머지는 계속
            continue
        n = 0
        for it in entries:
            if is_excluded(it["url"]):          # 피드가 제외 매체 기사를 실어 나른 경우
                excluded += 1
                continue
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
                    + (f" · 0건 {empty}" if empty else "")
                    + (f" · 제외 매체 {excluded}건 버림" if excluded else "")]}


# ── ② 선별 · select ────────────────────────────────────────────────────
BATCH, MIN_HEATS, PASS = 40, 2, 8               # 예선 묶음 최대 크기, 최소 묶음 수, 묶음당 통과 — 길게 받으면 비교 대신 목록을 베낀다
TARGET, SPARE = AUD["target"], AUD["spare"]     # 최종 건수, 본선 예비
MAX_PER_SOURCE = AUD["max_per_source"]          # 한 매체가 브리핑을 독차지하지 않게 (20장)
MAX_PER_COUNTRY = AUD["max_per_country"]        # 한 나라 이야기로 도배되지 않게 — 나라는 라벨의 country

def country_key(it):                            # 'United Kingdom'·'UK' 같은 표기 차이는 남지만, 모르는 나라는 상한을 걸지 않는다
    k = re.sub(r"[^a-z]", "", (it.get("country") or "").lower())
    return "" if k in ("", "unknown") else k

CRITERIA = (f"독자는 {AUD['reader']}입니다.\n"   # [고침] 기준을 audience.yaml 로 뺐다 (44장)
            "고를 것:\n" + "".join(f"- {c}\n" for c in AUD["criteria"])
            + "버릴 것 (절대 고르지 마세요):\n" + "".join(f"- {r}\n" for r in AUD["reject"])
            + AUD["notes"])

KINDS = ("동물", "사람·도전", "발견·장소", "법·행정", "음식·물건", "비극·범죄", "연예·운세", "일반 뉴스", "기타")
DROP_KINDS = set(AUD["drop_kinds"])             # [고침] '버릴 것'을 프롬프트 부탁이 아니라 코드로 강제한다
assert DROP_KINDS <= set(KINDS), "audience.yaml 의 drop_kinds 는 KINDS 중에서 고른다"

class Pick(BaseModel):                          # [고침] 제목을 먼저 옮겨 적게 해 이유가 엉뚱한 기사에 붙지 않게 한다
    title: str = Field(description="고른 후보의 제목을 목록에 있는 그대로 복사")
    index: int = Field(description="그 후보의 목록 번호")
    event: str = Field(description="사건 라벨. 영어 소문자 '장소-주인공-일' 형식으로, 반드시 사건이 일어난 나라나 도시 이름으로 시작 "
                                   "(예: florida-alligator-pool, japan-vending-machine-wedding, india-monkey-bank-robbery). "
                                   "한국어 기사도 영어로 쓰세요. 같은 사건이면 기사 언어·매체가 달라도 같은 라벨",
                       pattern=r"^[a-z0-9]+(-[a-z0-9.]+)+$")   # [고침] 프롬프트로는 절반이 비어 와서 형식으로 강제한다
    reason: str = Field(description="독자 기준으로 왜 골랐는지 한 문장. 영어 기사라도 반드시 한국어로")

class Shortlist(BaseModel):
    picks: list[Pick]

KOREA = re.compile(r"^(south-?korea|korea|seoul|busan|incheon|daegu|gwangju|daejeon|ulsan|jeju)\b")

class Label(BaseModel):                         # [고침] 순위 매기기와 따로, 후보마다 한 번만 성격을 붙인다
    index:    int  = Field(description="기사 번호")
    kind:     Literal[KINDS]
    country:  str = Field(description="사건이 일어난 나라 이름을 영어로 (예: India, United Kingdom, Australia, South Korea). "
                                      "모르면 Unknown")   # [고침] in_korea 참/거짓으로 물었더니 인도 기사에도 true 를 붙였다

class Labels(BaseModel):
    items: list[Label]

SYS_LABEL = ("아래 기사마다 성격(kind)과 사건이 일어난 나라(country)를 판정하세요. 고르지 말고 모든 번호에 답하세요.\n"
             "kind:\n"
             "- 동물 / 사람·도전 / 발견·장소 / 법·행정 / 음식·물건: 실제로 일어난 기묘하거나 신기하거나 훈훈한 이야기. "
             "이야기의 주인공으로 고르세요\n"
             "- 비극·범죄: 사망·부상·범죄가 이야기의 중심인 사건 사고\n"
             "- 연예·운세: 연예인 이야기, 운세·점성술\n"
             "- 일반 뉴스: 정치·전쟁·재난·경제, 기업 발표·신제품·기술 소식, 인터뷰·조언, 쇼핑·할인 광고\n"
             "- 기타: 어느 것도 아님\n"
             "웃기거나 신기한 사람·동물 이야기는 가볍더라도 '일반 뉴스'가 아닙니다.")

def label_all(items):                           # [고침] 본선에서 다시 분류하게 했더니 캥거루 구조까지 '일반 뉴스'가 됐다 — 한 번만 붙인다
    for k in range(0, len(items), BATCH):
        chunk = items[k:k + BATCH]
        listing = "\n".join(f"{i}. {it['title']}" + (f"\n    {it['summary'][:200]}" if it.get("summary") else "")
                            for i, it in enumerate(chunk))
        try:
            out = client().chat.completions.parse(
                model=MODEL, temperature=0,
                messages=[{"role": "system", "content": SYS_LABEL},
                          {"role": "user", "content": listing}],
                response_format=Labels).choices[0].message.parsed
        except Exception:                       # 분류가 실패하면 라벨 없이 예선으로 넘긴다
            continue
        for lb in out.items:
            if 0 <= lb.index < len(chunk):
                chunk[lb.index] |= {"kind": lb.kind, "country": lb.country}

def is_korea(country):
    return re.sub(r"[^a-z]", "", (country or "").lower()) in ("southkorea", "korea", "republicofkorea")

def drop_reason(it, event=""):                  # 프롬프트와 별개로 코드에서 버리는 이유 — 없으면 None
    if it.get("kind") in DROP_KINDS:
        return f"버림({it['kind']})"
    if is_korea(it.get("country")) or KOREA.match(event):   # [고침] 국내 제외를 부탁만 하면 고른 뒤 '국내외'라고 둘러댔다
        return "버림(국내)"
    return None

def ask_picks(items, n, extra=""):
    listing = "\n".join(f"{i}. [{it['source']}] {it['title']}"          # [고침] 제목만이 아니라 요약도 본다
                        + (f"\n    {it['summary'][:200]}" if it.get("summary") else "")
                        for i, it in enumerate(items))
    sys = (f"{CRITERIA}\n\n아래 후보의 제목과 요약을 함께 읽고, 중요한 순서대로 {n}건을 고르세요.\n"
           "목록 순서는 중요도와 무관합니다. 전부 비교한 뒤 가장 중요한 것부터 답하세요.\n"
           "같은 사건을 다룬 기사에는 같은 event 라벨을 붙이세요.\n"
           "reason은 목록이 모두 영어여도 한국어로 쓰세요." + (f"\n{extra}" if extra else ""))   # [고침] 영어 목록이면 영어로 따라 썼다
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
           "announcement", "update", "new", "출시", "공개", "발표",
           # [고침] 'product-update'·'infra-expansion' 처럼 회사·대상이 없는 라벨끼리 같은 사건이 돼
           # 서로 다른 기사(Gemini 4 Carbon 과 Airbnb 추천 개선)가 빠졌다 — 이런 낱말은 사건을 가리키지 못한다
           "product", "feature", "model", "tool", "service", "platform", "ai", "open", "tech",
           "infra", "expansion", "development", "news", "report", "partnership", "integration",
           # 기묘한 뉴스 라벨에 흔한 낱말 — 'uk-man-viral-video' 끼리 같은 사건이 되지 않게
           "man", "woman", "boy", "girl", "people", "viral", "video", "weird", "bizarre", "odd",
           "strange", "funny", "story", "incident"}

def event_key(label):                           # [고침] 라벨이 '출시'·'launch'처럼 달라도 같은 사건으로 본다
    words = re.findall(r"[0-9a-z가-힣.]+", label.lower())
    return "-".join(w for w in words if w not in GENERIC)

def subject_words(label):                       # 라벨에서 장소(첫 낱말)와 흔한 낱말을 뺀 나머지
    return set(event_key(label).split("-")[1:]) - {""}

def shares_subject(labels):                     # 묶인 기사마다 다른 기사와 겹치는 핵심 낱말이 하나는 있는가
    words = [subject_words(l) for l in labels]
    return all(any(w & v for j, v in enumerate(words) if j != i) for i, w in enumerate(words))

def same_event(a, b):
    ka, kb = event_key(a), event_key(b)
    if ka.count("-") < 1 or kb.count("-") < 1:  # [고침] 남은 낱말이 둘 미만이면(빈 라벨 포함) 사건을 특정할 수 없어 비교하지 않는다
        return False
    return SequenceMatcher(None, ka, kb).ratio() >= 0.8

class Group(BaseModel):                         # 사건을 먼저 적게 해 '같은 나라'만으로 묶지 않게 한다
    event: str      = Field(description="묶인 기사들이 모두 다룬 단 하나의 사건 (누가, 어디서, 무엇을)")
    ids:   list[int] = Field(description="그 사건을 다룬 기사 번호, 두 개 이상")

class Dupes(BaseModel):
    groups: list[Group] = Field(description="같은 사건 묶음. 없으면 빈 목록")

SYS_DUPES = ("아래 기사 중 같은 사건을 다룬 것끼리 번호를 묶으세요.\n"
             "같은 사건 = 같은 사람이나 동물이, 같은 곳에서 겪은 같은 일. 언어·매체·제목이 달라도 같은 사건이면 묶습니다.\n"
             "같은 나라의 다른 사건, 같은 종류(예: 탈출한 동물, 기네스 기록)의 다른 사건은 다른 사건입니다.\n"
             "'~관련 소식'처럼 넓게 말해야 묶이는 것은 다른 사건입니다. 확실하지 않으면 묶지 마세요.")

def dupes(picked, past):                        # [고침] 라벨 비교가 놓친 같은 사건을 LLM 이 한 번 더 묶는다
    """빼야 할 (기사, 이유) — 묶음에서 순위가 밀린 쪽, 최근에 이미 보낸 사건"""
    rows = [("최근 발행", h) for h in past] + [("오늘 후보", x) for x in picked]
    if len(picked) == 0 or len(rows) < 2:
        return []
    listing = "\n".join(f"{i}. ({tag}) [{x['source']}] {x['title']}" for i, (tag, x) in enumerate(rows))
    try:
        out = client().chat.completions.parse(
            model=MODEL, temperature=0,
            messages=[{"role": "system", "content": SYS_DUPES},
                      {"role": "user", "content": listing}],
            response_format=Dupes).choices[0].message.parsed
    except Exception:                           # 묶기가 실패해도 선별은 계속한다
        return []
    off, drop = len(past), {}
    for g in out.groups:
        g = sorted({i for i in g.ids if 0 <= i < len(rows)})
        today = [i - off for i in g if i >= off]
        if len(g) - len(today) > 1:             # 최근 발행끼리는 이미 서로 다른 사건 — 주제로 묶은 것이니 믿지 않는다
            continue
        if not shares_subject([rows[i][1].get("event", "") for i in g]):
            continue                            # [고침] 서로 다른 기사 7건을 한 묶음으로 뭉뚱그린 날이 있었다 — 라벨이 받쳐 줄 때만 믿는다
        old = len(today) < len(g)               # 최근에 보낸 기사와 묶였으면 오늘 것은 모두 뺀다
        for i in (today if old else today[1:]):
            drop.setdefault(i, "최근 발행" if old else "같은 사건")
    return [(picked[i], why) for i, why in sorted(drop.items())]

HISTORY = Path(__file__).with_name("history.json")   # [고침] 어제 보낸 사건을 오늘 또 고르지 않게
LOOKBACK, KEEP = 3, 14                          # 최근 3일과 비교, 파일에는 14일치만 둔다

def load_history():
    try:
        return json.loads(HISTORY.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return []

def days_ago(n):
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")

def recent_sent():
    hist = [h for h in load_history() if h["date"] >= days_ago(LOOKBACK)]
    if os.environ.get("GITHUB_EVENT_NAME", "local") != "schedule":   # 손으로 돌린 테스트는 그날 첫 실행처럼 — 오늘 보낸 기사로 거르지 않는다
        hist = [h for h in hist if h["date"] != days_ago(0)]
    return hist

def save_history(arts):                         # 실제로 보냈을 때만 run.py 가 부른다
    hist = [h for h in load_history() if h["date"] >= days_ago(KEEP)]
    hist += [{"date": days_ago(0), "source": a["source"], "title": a["title"],
              "url": a["url"], "event": a.get("event", "")} for a in arts]
    HISTORY.write_text(json.dumps(hist, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

PROMO = re.compile("|".join(AUD["promo_title_patterns"]), re.I)   # [고침] 제목만 봐도 홍보성인 기사는 LLM 에 넘기기 전에 거른다

def heats(items):                               # [고침] 후보가 적어도 묶음을 둘 이상 만든다
    n = max(MIN_HEATS, math.ceil(len(items) / BATCH))
    mixed = items[:]
    random.Random(datetime.now().strftime("%Y-%m-%d")).shuffle(mixed)   # 소스가 한 묶음에 몰리지 않게
    return [h for h in (mixed[k::n] for k in range(n)) if h]

def brief(it, p=None, result=None):             # 기록용 한 줄 — 왜 골랐고 어떻게 됐는지
    row = {"source": it["source"], "title": it["title"], "url": it["url"], "kind": it.get("kind")}
    if p:
        row |= {"event": p.event, "reason": p.reason}
    if result:
        row["result"] = result
    return row

def select(s: dict) -> dict:
    items = s["collected"]
    if not items:                               # [고침] 후보가 없으면 LLM 을 부르지 않는다
        return {"picked": [], "trace": {"candidates": 0}, "log": ["② 선별   0 → 0건"]}

    past = recent_sent()
    sent_urls = {url_key(h["url"]) for h in past}
    again = [it for it in items if url_key(it["url"]) in sent_urls]   # 이미 보낸 주소는 다시 올리지 않는다
    fresh = [it for it in items if url_key(it["url"]) not in sent_urls]
    promo = [it for it in fresh if PROMO.search(it["title"])]
    pool = [it for it in fresh if not PROMO.search(it["title"])]
    label_all(pool)                             # 성격 라벨 — 예선 전에 코드로 버린다
    labels = dict(Counter(it.get("kind", "없음") for it in pool))
    off = [brief(it, result=drop_reason(it)) for it in pool if drop_reason(it)]
    labeled = len(pool)
    pool = [it for it in pool if not drop_reason(it)]

    survivors, trace_heats, dropped = [], [], 0
    for chunk in heats(pool):                   # 예선 — 묶음마다 여덟 건
        rows = []
        n = min(PASS, max(TARGET, len(chunk) // 2))   # [고침] 라벨에서 많이 빠진 날 9건 중 8건이 통과해 예선이 고르지 않았다 — 작은 조는 절반만
        for p in ask_picks(chunk, n):
            rows.append(brief(chunk[p.index], p, "통과"))
            survivors.append(chunk[p.index])
        trace_heats.append({"size": len(chunk), "picks": rows})

    random.Random(len(survivors)).shuffle(survivors)   # [고침] 몇 조 출신인지가 순위에 새지 않게 섞는다
    picked, per_src, per_country, rows, row_of, ranked = [], Counter(), Counter(), [], {}, set()
    dup = Counter()
    full = lambda it: (per_src[it["source"]] >= MAX_PER_SOURCE
                       or bool(country_key(it)) and per_country[country_key(it)] >= MAX_PER_COUNTRY)
    for _ in range(3):                          # 본선 — 걸러져서 다섯 + 예비가 안 차면 남은 후보로 다시
        remaining = [it for it in survivors     # 상한이 찬 매체·나라는 다음 판에 올리지 않는다
                     if id(it) not in ranked and not full(it)]
        if len(picked) >= TARGET + SPARE or not remaining:
            break
        finals = ask_picks(remaining, TARGET - len(picked) + SPARE,
                           f"같은 매체(대괄호 안 이름)에서는 최대 {MAX_PER_SOURCE}건, "
                           f"같은 나라 이야기는 최대 {MAX_PER_COUNTRY}건까지만 고르세요.")
        for p in finals:
            it = remaining[p.index]
            ranked.add(id(it))
            if len(picked) == TARGET + SPARE:
                result = "남음"
            elif drop_reason(it, p.event):      # 라벨이 놓친 국내 사건 — 사건 라벨이 한국 지명으로 시작
                result, dropped = drop_reason(it, p.event), dropped + 1
            elif any(same_event(p.event, x["event"]) for x in picked):   # [고침] 같은 사건은 한 건만
                result = "같은 사건"
                dup[result] += 1
            elif per_src[it["source"]] >= MAX_PER_SOURCE:
                result = "매체 상한"
            elif full(it):                      # [고침] 인도 매체 두 곳이 2건씩 채워 인도 이야기로 도배될 수 있었다
                result = "나라 상한"
            else:                               # [고침] 예비도 같은 규칙으로 뽑아 두고 빈자리에 쓴다
                result = "선정"
                per_src[it["source"]] += 1
                per_country[country_key(it)] += 1
                picked.append({**it, "event": p.event, "reason": p.reason})
            row_of[it["url"]] = brief(it, p, result)
            rows.append(row_of[it["url"]])
        for x, why in dupes(picked, past):      # 라벨이 달라 위에서 못 거른 같은 사건
            picked = [y for y in picked if y is not x]
            per_src[x["source"]] -= 1
            per_country[country_key(x)] -= 1
            row_of[x["url"]]["result"] = why
            dup[why] += 1
    for r, x in enumerate(picked):              # 순위는 마지막에 매긴다 — 앞의 다섯이 선정, 나머지는 예비
        x["rank"] = r
        row_of[x["url"]]["result"] = "선정" if r < TARGET else "예비"
    trace = {"candidates": len(items), "already_sent": [brief(it) for it in again],
             "promo": [brief(it) for it in promo], "labels": labels, "label_dropped": off,
             "heats": trace_heats, "final": rows,
             "final_out": [brief(it) for it in survivors if id(it) not in ranked]}

    spare = len(picked[TARGET:])
    log = [f"② 선별   {len(items)}" + (f" → 이미 보냄 제외 {len(fresh)}" if again else "")
           + f" → 홍보 제외 {labeled} → 라벨 제외 {len(pool)} → 예선 {len(trace_heats)}조 {len(survivors)}"
           f" → {len(picked) - spare}건" + (f" + 예비 {spare}" if spare else "")
           + (f" · 버림 {dropped}" if dropped else "")
           + "".join(f" · {k} {n}" for k, n in dup.items())]
    log += [f"   {'선정' if it['rank'] < TARGET else '예비'} [{it['source']}] {it['title'][:50]} — {it['reason']}"
            for it in picked]
    return {"picked": picked, "trace": trace, "log": log}


# ── ③ 취재·요약 · report ───────────────────────────────────────────────
MIN_BODY = 600                                  # G1 기준선 (11장)
TOPICS = ("동물", "사람", "발견·장소", "법·행정", "음식·물건", "사건·사고", "연예·운세")

class Draft(BaseModel):
    headline: str = Field(description="30자 이내의 한국어 헤드라인")
    summary:  str = Field(description="한국어 세 문장 요약. 모든 문장을 '~합니다'로 끝낸다")
    why:      str = Field(description="알고 보면: 이 일이 왜 일어났는지, 또는 어떤 규칙·배경·숫자가 있는지 "
                                      "원문에 나온 내용으로 한국어 한 문장, 80자 이내")
    topic:    Literal[TOPICS] = Field(description="기사 분류")      # [고침] 카드 색에 쓴다

SYS = ("당신은 해외 화제 뉴스를 한국어로 전하는 뉴스레터 기자입니다.\n"
       "아래 기사 본문을 읽고 헤드라인·요약·'알고 보면'을 반드시 한국어로 쓰세요.\n"
       "- 요약은 정확히 세 문장, 문장 끝은 모두 '~합니다'·'~했습니다' (~다체 금지)\n"
       "- 누가, 어디서, 무엇을 했는지 첫 문장에 넣으세요\n"
       "- why는 '알고 보면' 한 문장. 요약에 없는 배경·이유·규칙·숫자를 원문에서 찾아 쓰세요. "
       "원문에 없는 이유를 짐작해 쓰지 마세요\n"
       "원문에 없는 숫자·이름·지명은 쓰지 마세요. 과장하거나 기사 속 사람을 놀리지 마세요.\n"
       "사고·범죄 기사는 피해자를 웃음거리로 만들지 말고 사실만 전하세요.\n"
       "돈은 원문 통화 그대로 쓰세요 (12,000달러, 50만 엔). 원화로 환산하지 마세요.\n"   # [고침] '$12,000'을 환율을 지어내 '1,200만원'으로 옮겼다
       "'충격·경악·화제다' 같은 낚시성 표현은 쓰지 마세요.\n"
       "topic은 이야기의 주인공이 무엇인지로 고르세요.\n"
       "- 동물: 동물이 주인공\n"
       "- 사람: 사람의 행동·도전·기록\n"
       "- 발견·장소: 유물·자연 현상·이상한 장소의 발견\n"
       "- 법·행정: 황당한 법·규칙·재판·관청 일\n"
       "- 음식·물건: 음식·상품·발명품\n"
       "- 사건·사고: 기묘한 사고·범죄·재판\n"
       "- 연예·운세: 연예인 이야기, 운세·점성술")

class ReportIn(TypedDict):                      # 워커가 받는 것은 기사 하나뿐
    item: dict

BIG_UNITS = re.compile(r"(\d+(?:\.\d+)?)\s*(lakh|lac|crore|million|billion)s?\b", re.I)
UNIT = {"lakh": 10**5, "lac": 10**5, "crore": 10**7, "million": 10**6, "billion": 10**9}

def expand_units(text):                         # [고침] '41.39 lakh'를 '4,139만'으로, '1.5 million yen'을 '1,500만 엔'으로 10배 틀렸다 — 숫자로 풀어 준다
    return BIG_UNITS.sub(lambda m: f"{float(m[1]) * UNIT[m[2].lower()]:,.0f}", text)

def extract_body(url):                          # 본문과 기사가 올린 대표 사진(og:image) — 사진은 만들지 않고 원문 것만 쓴다
    d = trafilatura.fetch_url(url)
    if not d:
        return None, None
    body = trafilatura.extract(d)
    meta = trafilatura.extract_metadata(d)
    image = meta.image if meta and (meta.image or "").startswith("https://") else None
    return (expand_units(body) if body else body), image

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
        body, image = extract_body(it["url"])
        if not body or len(body) < MIN_BODY:
            return {"drafted": [],
                    "log": [f"   취재 제외 {it['source']} · 본문 {len(body or '')}자"]}
        d = draft(body)
    except Exception as e:                      # [고침] 한 갈래가 터져도 나머지는 계속
        return {"drafted": [], "log": [f"   취재 실패 {it['source']} · {type(e).__name__}"]}
    return {"drafted": [{**it, "body": body[:6000], "image": image, **d.model_dump()}]}


# ── ④ 검수 · verify ────────────────────────────────────────────────────
class Verdict(BaseModel):
    real:     bool      = Field(description="원문이 출처가 드러난 실제 사건 보도면 true")
    country:  str       = Field(description="원문의 사건이 일어난 나라 이름을 영어로. 모르면 Unknown")   # [고침] 선별은 제목·요약 200자만 봐서 인천 이야기를 놓쳤다
    ok:       bool      = Field(description="헤드라인·요약·알고 보면이 모두 원문에 근거하면 true")
    problems: list[str] = Field(description="근거 없는 부분이나 출처 문제. 없으면 빈 목록")

SYS_CHECK = ("세 가지를 판정하세요. country는 원문의 사건이 일어난 나라입니다.\n"
             "1. real: 원문이 실제 사건 보도인가. 경찰·관청·당사자 인터뷰·현지 언론 등 출처가 원문에 나오면 true. "
             "풍자·창작, 출처 없는 소문, SNS 글 하나를 옮긴 것뿐이면 false.\n"
             "2. ok: 헤드라인·요약·알고 보면이 원문에서 뒷받침되는가. 원문에 없는 숫자·이름·지명·이유가 있으면 false. "
             "번역 표현 차이는 문제가 아니지만, 환산한 숫자는 원문 숫자로 직접 다시 계산해 틀리면 false. "
             "원문에 없는 통화로 바꾼 금액(달러를 원화로 등)도 false.")   # [고침] '단위 환산은 문제가 아니다'라고 적었더니 10배 틀린 환산도 통과시켰다

def check(d):                                   # 요약을 쓴 호출과 따로 부른다
    user = (f"[원문]\n{d['body'][:5000]}\n\n"
            f"[헤드라인]\n{d['headline']}\n\n[요약]\n{d['summary']}\n\n[알고 보면]\n{d['why']}")
    return client().chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS_CHECK},
                  {"role": "user", "content": user}],
        response_format=Verdict).choices[0].message.parsed

WON_IN_TEXT = re.compile(r"\d[\d,.]*\s*[만억천]?\s*원")
WON_IN_BODY = re.compile(r"₩|\bKRW\b|\d\s*won\b|원")

def made_up_won(d):                             # [고침] '$12,000'을 '1,200만원'으로 옮긴 요약을 검수 LLM 이 통과시켰다 — 원문에 없는 원화 금액
    return bool(WON_IN_TEXT.search(d["headline"] + d["summary"] + d.get("why", ""))) \
        and not WON_IN_BODY.search(d["body"])

def is_korean(d):                               # [고침] 24장 한글 검사를 연결한다
    return bool(re.search(r"[가-힣]", d["headline"] + d["summary"]))

def verify(s: dict) -> dict:
    kept, dropped, rejected = [], [], []
    def reject(d, why, problems=()):            # 떨어진 기사와 이유 — 스킵하고 예비로 채운다
        dropped.append(d["source"] + (f"({why})" if why != "근거 없음" else ""))
        rejected.append({"source": d["source"], "headline": d["headline"], "rank": d["rank"],
                         "why": why, "problems": list(problems)})
    for d in sorted(s["drafted"], key=lambda d: d["rank"]):   # [고침] 순위대로 보고 다섯이 차면 멈춘다
        if len(kept) == TARGET:
            break
        if not is_korean(d):
            reject(d, "영어")
            continue
        if made_up_won(d):
            reject(d, "원화 환산")
            continue
        try:
            v = check(d)
        except Exception as e:                  # [고침] 검수 호출이 터져도 그 기사만 빼고 계속
            reject(d, type(e).__name__)
            continue
        if not v.real:                          # 지어낸 이야기·소문은 요약이 맞아도 보내지 않는다
            reject(d, "출처 불명", v.problems)
        elif is_korea(v.country):
            reject(d, "국내")
        elif v.ok:
            kept.append(d)
        else:
            reject(d, "근거 없음", v.problems)
    spare = sum(d["rank"] >= TARGET for d in kept)
    return {"verified": kept, "rejected": rejected,   # 리듀서 없는 칸에 담는다
            "log": [f"④ 검수   {len(s['drafted'])} → {len(kept)}건"
                    + (f" · 불합격 {dropped}" if dropped else "")
                    + (f" · 예비 투입 {spare}" if spare else "")]}


# ── ⑤ 발행 · publish ───────────────────────────────────────────────────
COLORS = {"동물": 0x0B6E77, "사람": 0x3B6FB6,                  # [고침] 청록·초록이 비슷해 잘 안 갈렸다
          "발견·장소": 0x6A4A9C, "법·행정": 0xB7791F, "음식·물건": 0xB5475A,
          "사건·사고": 0x8A3B3B, "연예·운세": 0xC2569B}
DEFAULT = 0x5F7476
TITLE_MAX, DESC_MAX, EMBED_MAX, TOTAL_MAX = 256, 4096, 10, 5800   # 6000에서 여유를 둔다

def build_embeds(run_id, lead, articles):
    if not articles:                            # 조용한 날에도 한 장은 보낸다
        return [{"title": f"🗞️ {run_id}", "color": DEFAULT,
                 "description": "오늘은 조용합니다."}]
    embeds = [{"title": f"🗞️ {run_id} · 해외 기묘한 뉴스", "description": lead, "color": DEFAULT}]
    for i, a in enumerate(articles, 1):
        desc = a["summary"]
        if a.get("why"):
            desc += f"\n\n🔎 **알고 보면** {a['why']}"
        embeds.append({
            "title":       f"{i}. {a['headline']}"[:TITLE_MAX],
            "description": desc[:DESC_MAX],
            "url":         a["url"],
            "color":       COLORS.get(a.get("topic", ""), DEFAULT),
            "footer":      {"text": f"{a['source']} · {a['when']}"},
        } | ({"image": {"url": a["image"]}} if a.get("image") else {}))   # 사진이 없는 기사는 글만
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
             "url": a["url"], "source": a["source"], "topic": a.get("topic", ""), "image": a.get("image"),
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
