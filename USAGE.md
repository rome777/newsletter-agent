# 실행과 기록

## 실행

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.lock
```

`.env`에 두 값을 넣는다. (`.env`는 커밋하지 않는다)

```
OPENAI_API_KEY=sk-...
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

```bash
python run.py --dry-run          # 보내지 않고 카드 내용만 출력
DRY_RUN=0 python run.py --silent # 알림 없이 실제 발행
DRY_RUN=0 python run.py          # 실제 발행
python run.py --delete <메시지ID> # 이 웹훅으로 보낸 메시지 삭제
python run.py --once             # 오늘 이미 보냈으면 건너뜀 (예약 실행용)
POST_NOTE="한 줄" DRY_RUN=0 python run.py  # 카드 위에 한 줄을 붙여 발행 (제출용 채널에서 보낸 사람 표시)
```

**자동 실행** — 저장소 Settings → Secrets에 같은 두 값을 등록하면 `.github/workflows/daily.yml`이 매일 돈다. GitHub는 예약 실행을 몇 시간씩 늦게 시작하기도 해서 KST 06:47·07:07·07:27에 세 번 걸어 둔다. 예약 실행은 `--once`로 돌아, 실제로 보낸 날을 적어 둔 `last_sent.txt`가 오늘이면 건너뛴다. 세 실행이 한꺼번에 밀려 시작해도 하나씩 차례로 돌고, 뒤 실행은 앞 실행이 커밋한 `last_sent.txt`를 받아 본다. 발행이 실패하면 날짜를 적지 않으므로 다음 예약이 다시 시도한다. 손으로 돌리려면 Actions → 매일 브리핑 → Run workflow(`dry_run` 체크 시 발행 안 함). 공개 저장소는 60일 동안 커밋이 없으면 예약 실행이 꺼지므로, 마지막 커밋이 50일을 넘기면 워크플로가 빈 커밋을 하나 올린다.

워크플로가 발행할 때마다 `history.json`을 커밋하므로, 로컬에서 고친 것을 올리기 전에 `git pull --rebase`를 먼저 한다.

## 기록

실행마다 `runs/날짜_시각.json`에 **왜 골랐는지**(선정 이유·사건 라벨·성격)와 **왜 떨어졌는지**(홍보 제거, 예선·본선 탈락, 같은 사건, 매체 상한, 검수 불합격), 발행한 기사를 남긴다. Actions에서는 실행 페이지의 artifact로 90일 보관. `runs/`는 커밋하지 않으므로 마지막 회차는 `store/last_run.json`에 덮어써 커밋한다.

`store/metrics.jsonl`에는 실행마다 한 줄씩 단계별 건수를 쌓는다. 소스별 수집 건수, 홍보 제외, 예선 묶음 크기와 통과 수, 같은 사건 제외, 본문 추출 성공률, 검수 불합격, 발행 건수와 성격·토픽이 들어간다. `trigger`가 `schedule`이면 예약 실행, `local`이면 로컬 실행이다.

소스를 다시 고를 때는 `python tools/measure_sources.py`로 후보를 재서 `store/sources.json`을 갱신한다. 기준은 [REPORT.md](REPORT.md) 2절에 있다.

실제로 보낸 기사는 `history.json`에 14일치를 남기고, 다음 날 선별에서 최근 3일치와 비교해 같은 사건을 거른다. 손으로 돌린 실행(로컬, Actions 수동 실행)은 오늘 보낸 기사로는 거르지 않는다. 같은 날 여러 번 테스트해도 그날 첫 실행과 같은 후보로 돈다.

## 파일

| 파일 | 내용 |
|---|---|
| `graph.py` | State, 노드 다섯 개, 그래프 |
| `run.py` | 진입점, 기록 저장 |
| `audience.yaml` | 타깃 독자, 중요도 기준, 제외 조건(홍보성 제목, 제외 매체), 발행 건수 |
| `REPORT.md` | 과제 수행 보고서 |
| `tools/measure_sources.py` | 소스 후보 측정 (관문 G1·G2·G3 + 기묘한 기사 비율) |
| `store/sources.json` | 소스 측정 결과 |
| `store/metrics.jsonl` | 실행마다 단계별 건수 (Actions가 커밋) |
| `store/last_run.json` | 마지막 회차의 선정·탈락 기록 (Actions가 커밋) |
| `history.json` | 최근 14일 동안 보낸 기사 (Actions가 커밋) |
| `last_sent.txt` | 마지막으로 실제 발행한 날 (Actions가 커밋) |
| `docs/` | 발행 화면 캡처 |
| `requirements.txt` / `.lock` | 직접 쓰는 패키지 / 실제 설치할 전체 버전 |
| `.github/workflows/daily.yml` | 매일 KST 06:47·07:07·07:27 예약 실행 |
