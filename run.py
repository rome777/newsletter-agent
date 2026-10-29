"""진입점 — graph.py 를 불러 한 번 실행한다.

    python run.py              # DRY_RUN 환경변수를 따른다 (없으면 보내지 않음)
    python run.py --dry-run    # 무조건 보내지 않음
    python run.py --silent     # 보내되 알림은 안 울리게 (테스트 발행)
    python run.py --delete ID  # 이 웹훅으로 보낸 메시지 ID 를 지운다
    python run.py --once       # 오늘 이미 보냈으면 건너뛴다 (Actions 예약 실행용)
"""
import json, os, sys, time
from collections import Counter
from datetime import datetime
from pathlib import Path

SENT_ON = Path(__file__).with_name("last_sent.txt")   # 마지막으로 실제 발행한 날 — Actions 가 커밋한다
STORE = Path(__file__).with_name("store")              # metrics.jsonl · last_run.json — Actions 가 커밋한다

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")   # 윈도우 콘솔에서 한글·이모지가 깨지지 않게
    today = datetime.now().strftime("%Y-%m-%d")
    if "--once" in sys.argv and SENT_ON.exists() and SENT_ON.read_text(encoding="utf-8").strip() == today:
        print(f"오늘({today}) 이미 발행함 — 건너뜀")   # 예약을 여러 번 걸어 두고 먼저 돈 한 번만 보낸다
        sys.exit(0)
    if "--delete" in sys.argv:
        from graph import delete_message
        mid = sys.argv[sys.argv.index("--delete") + 1]
        ok = delete_message(mid)
        print(f"메시지 {mid} 삭제", "성공" if ok else "실패")
        sys.exit(0 if ok else 1)
    if "--silent" in sys.argv:
        os.environ["SILENT"] = "1"
    if "--dry-run" in sys.argv:
        os.environ["DRY_RUN"] = "1"            # 35장의 dry_run 스위치

    from graph import run
    started = time.time()
    out = run()
    for line in out["log"]:
        print(line)                            # 이 출력이 Actions 로그에 그대로 남는다

    record = {                                 # 왜 골랐고 무엇을 보냈는지 실행마다 한 파일로 남긴다
        "at": datetime.now().isoformat(timespec="seconds"),
        "sent": out["sent"],
        "log": out["log"],
        "selection": out["trace"],
        "rejected": out["rejected"],
        "published": [{k: a.get(k) for k in ("source", "headline", "url", "kind", "topic", "event", "reason", "why")}
                      for a in out["verified"]],
    }
    Path("runs").mkdir(exist_ok=True)
    path = Path("runs") / f"{datetime.now():%Y-%m-%d_%H%M%S}.json"
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"기록   {path}")
    STORE.mkdir(exist_ok=True)                 # runs/ 는 커밋하지 않으니 마지막 한 회차는 저장소에 남긴다
    (STORE / "last_run.json").write_text(json.dumps(record, ensure_ascii=False, indent=2, default=str) + "\n",
                                         encoding="utf-8")

    trace, picked = out["trace"], out["picked"]
    metrics = {                                # 실행마다 한 줄 — 단계별 건수로 선별·검수가 의도대로 돌았는지 본다
        "at": record["at"],
        "trigger": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "sent": out["sent"],
        "seconds": round(time.time() - started),
        "collected": len(out["collected"]),
        "per_source": dict(Counter(it["source"] for it in out["collected"]).most_common()),
        "already_sent": len(trace.get("already_sent", [])),
        "promo_dropped": len(trace.get("promo", [])),
        "heats": [h["size"] for h in trace.get("heats", [])],
        "heat_passed": sum(r.get("result") == "통과" for h in trace.get("heats", []) for r in h["picks"]),
        "labels": trace.get("labels", {}),       # 후보 전체의 성격 라벨 분포
        "kind_dropped": len(trace.get("label_dropped", []))
                        + sum(r.get("result", "").startswith("버림") for r in trace.get("final", [])),
        "same_event_dropped": sum(r.get("result") in ("같은 사건", "최근 발행") for r in trace.get("final", [])),
        "picked": len(picked),
        "drafted": len(out["drafted"]),
        "body_ok_rate": round(len(out["drafted"]) / len(picked), 2) if picked else None,   # 12장 본문 추출 성공률
        "verify_checked": len(out["verified"]) + len(out["rejected"]),
        "verify_failed": [f"{r['source']}:{r['why']}" for r in out["rejected"]],
        "published": len(out["verified"]),
        "published_sources": [a["source"] for a in out["verified"]],
        "kinds": [a.get("kind") for a in out["verified"]],
        "topics": [a.get("topic") for a in out["verified"]],
    }
    with (STORE / "metrics.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(metrics, ensure_ascii=False) + "\n")

    if out["sent"] == "sent":                  # 실제로 보낸 것만 남겨 다음 날 같은 사건을 거른다
        from graph import save_history
        save_history(out["verified"])
        SENT_ON.write_text(today + "\n", encoding="utf-8")   # 0건인 날("조용합니다")도 보낸 날로 남긴다

    if out["sent"] == "failed":                # Discord 가 거절하면 실행을 실패로 끝내 메일이 오게 한다
        sys.exit(1)
