# pop_review_requests.py ─ 대시보드의 '🔄 리뷰 갱신' 요청을 꺼내고 지운다.
#
# index.html 의 버튼은 GitHub Actions 를 직접 부를 수 없다 — actions:write 토큰이
# 필요한데 그 페이지는 공개라 소스에 넣으면 그대로 노출된다. 그래서 버튼은 요청만
# Firebase 에 남기고, 5분마다 도는 daily_update.yml 이 이 스크립트로 그걸 꺼내
# 그 자리에서 generate_review.py 를 돌린다(PC 를 켜 둘 필요가 없다).
#
# 경로가 /analytics/review_request 인 이유: DB 규칙이 브라우저 쓰기를
# reviews·reviews_history·analytics 에만 허용한다(/requests 는 거부됨을 실측).
#
# 출력: 실행할 시장을 공백으로 구분해 stdout 에 한 줄로 찍는다(없으면 빈 줄).
#       진단 메시지는 전부 stderr 로 보낸다 — 워크플로가 stdout 을 그대로
#       `for m in $markets` 에 쓰기 때문에 섞이면 안 된다.
#
# 꺼내면서 바로 지우는 이유: 리뷰 생성이 4~6분이라 다음 5분 tick 과 겹칠 수 있다.
# 먼저 지워두면 겹친 실행이 같은 요청을 다시 집지 않는다.

import os, sys, json, re
from datetime import datetime, timezone

REQ_PATH        = 'analytics/review_request'
VALID_MARKETS   = ('kr', 'us', 'jp', 'cn')
MAX_AGE_MIN     = 30      # 이보다 오래된 요청은 버린다(크래시로 남은 것)
DATABASE_URL    = 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8')
    except Exception:
        pass


def note(msg):
    print(msg, file=sys.stderr)


def main():
    try:
        import firebase_admin
        from firebase_admin import credentials, db as firebase_db
    except Exception as e:
        note(f'[WARN] firebase_admin 로드 실패: {e}')
        print('')
        return

    try:
        cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
        try:
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        except ValueError:
            pass                                    # 이미 초기화됨
        ref = firebase_db.reference(f'/{REQ_PATH}')
        raw = ref.get() or {}
    except Exception as e:
        note(f'[WARN] 요청 조회 실패: {e}')
        print('')
        return

    if not isinstance(raw, dict) or not raw:
        note('대기 중인 리뷰 갱신 요청 없음')
        print('')
        return

    now = datetime.now(timezone.utc)
    todo = []
    for market, entry in sorted(raw.items()):
        # 어떤 키든 일단 지운다 — 실패해도 같은 요청이 영원히 반복되지 않게.
        try:
            firebase_db.reference(f'/{REQ_PATH}/{market}').delete()
        except Exception as e:
            note(f'[WARN] {market} 요청 삭제 실패: {e}')

        if market not in VALID_MARKETS:
            note(f'[WARN] 알 수 없는 시장 키 "{market}" — 버림')
            continue

        at = entry.get('at') if isinstance(entry, dict) else None
        if at:
            try:
                # 브라우저가 넣는 값은 ISO8601 UTC ('...Z')
                t = datetime.fromisoformat(str(at).replace('Z', '+00:00'))
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                age = (now - t).total_seconds() / 60
                if age > MAX_AGE_MIN:
                    note(f'[SKIP] {market}: 요청이 {age:.0f}분 전 것 — 버림')
                    continue
                note(f'[OK] {market}: {age:.1f}분 전 요청')
            except Exception:
                note(f'[OK] {market}: 시각 파싱 불가({at}) — 그대로 처리')
        else:
            note(f'[OK] {market}: 시각 없음 — 그대로 처리')
        todo.append(market)

    note(f'실행할 시장: {" ".join(todo) if todo else "(없음)"}')
    print(' '.join(todo))


if __name__ == '__main__':
    main()
