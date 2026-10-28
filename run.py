"""진입점 — graph.py 를 불러 한 번 실행한다.

    python run.py              # DRY_RUN 환경변수를 따른다 (없으면 보내지 않음)
    python run.py --dry-run    # 무조건 보내지 않음
    python run.py --silent     # 보내되 알림은 안 울리게 (테스트 발행)
    python run.py --delete ID  # 이 웹훅으로 보낸 메시지 ID 를 지운다
"""
import json, os, sys
from datetime import datetime
from pathlib import Path

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")   # 윈도우 콘솔에서 한글·이모지가 깨지지 않게
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
    out = run()
    for line in out["log"]:
        print(line)                            # 이 출력이 Actions 로그에 그대로 남는다

    record = {                                 # 왜 골랐고 무엇을 보냈는지 실행마다 한 파일로 남긴다
        "at": datetime.now().isoformat(timespec="seconds"),
        "sent": out["sent"],
        "log": out["log"],
        "selection": out["trace"],
        "published": [{k: a.get(k) for k in ("source", "headline", "url", "kind", "topic", "reason", "why")}
                      for a in out["verified"]],
    }
    Path("runs").mkdir(exist_ok=True)
    path = Path("runs") / f"{datetime.now():%Y-%m-%d_%H%M%S}.json"
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"기록   {path}")

    if out["sent"] == "sent":                  # 실제로 보낸 것만 남겨 다음 날 같은 사건을 거른다
        from graph import save_history
        save_history(out["verified"])

    if out["sent"] == "failed":                # Discord 가 거절하면 실행을 실패로 끝내 메일이 오게 한다
        sys.exit(1)
