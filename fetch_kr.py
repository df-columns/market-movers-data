# fetch_kr.py  ─  국내 주식 데이터 수집 → Firebase /v1/kr

import requests, urllib3, warnings, json, re, os, sys
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from datetime import datetime, timedelta, timezone
import time
import firebase_admin
from firebase_admin import credentials, db as firebase_db

warnings.filterwarnings('ignore')
urllib3.disable_warnings()

# ── Firebase 초기화 ────────────────────────────────────────────────────────────
cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
try:
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'
    })
except ValueError:
    pass

# ── 설정 ──────────────────────────────────────────────────────────────────────
KOSPI_PAGES  = 8
KOSDAQ_PAGES = 4
MAX_WORKERS  = 20
HISTORY_DAYS = 400

# ── 네이버 접속 재시도·가드 설정 ───────────────────────────────────────────────
# finance.naver.com 은 장시간 응답하지 않는 구간이 있다.
#   실측: 2026-08-01(토) 00:00~10:30 KST, 10시간 30분 연속 connect timeout.
#   그 사이 133회 실행이 전부 실패했고, 리스트가 0종목이 되어 valid_dates[0] 에서
#   IndexError 로 죽었다. 죽으면 daily_update 의 뒤 스텝(us/jp/cn/fx)까지 skip 됐다.
# connect 타임아웃을 짧게 잡는 이유: 호스트가 블랙홀이면 파이썬이 DNS A레코드를
# 순서대로 다 시도하므로 (타임아웃 × IP수) 만큼 걸린다 — 실측 요청당 약 40초.
NAVER_TRIES     = 3
NAVER_BACKOFF   = 3          # 재시도 대기: 3초 → 6초
NAVER_TIMEOUT   = (5, 15)    # (connect, read)
MIN_STOCK_RATIO = 0.8        # 기존 종목 수의 이 비율 미만이면 업로드하지 않고 스킵
MIN_STOCK_ABS   = 200        # 기존 종목 수를 모를 때 쓰는 하한 (정상 약 289종목)

session = requests.Session()
session.verify = False
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com/'
})


def naver_get(url, tries=NAVER_TRIES, **kw):
    """네이버 GET — 지수 백오프 재시도. 전부 실패하면 마지막 예외를 올린다."""
    kw.setdefault('timeout', NAVER_TIMEOUT)
    last = None
    for i in range(tries):
        try:
            return session.get(url, **kw)
        except Exception as e:
            last = e
            if i < tries - 1:
                wait = NAVER_BACKOFF * (i + 1)
                print(f'  [RETRY {i + 1}/{tries - 1}] {url} — '
                      f'{type(e).__name__}, {wait}초 후 재시도')
                time.sleep(wait)
    raise last


def skip(reason):
    """수집이 온전하지 않을 때 — 기존 Firebase 데이터를 건드리지 않고 정상 종료.

    exit 0 인 이유: 5분마다 도는 워크플로에서 외부 사이트 장애를 실패로 처리하면
    뒤 스텝(us/jp/cn/fx)까지 못 돌고 알림만 시끄러워진다. 반쪽 데이터로
    덮어쓰지 않는 것이 목적이므로 '아무것도 하지 않고 끝내기'가 맞다.
    """
    print(f'[KR] 건너뜀 — {reason}')
    print('[KR] 기존 Firebase 데이터를 유지합니다. (에러 아님)')
    sys.exit(0)


def fetch_stock_list(market_code, pages):
    results = []
    for page in range(1, pages + 1):
        try:
            r = naver_get(
                'https://finance.naver.com/sise/sise_market_sum.nhn',
                params={'sosok': market_code, 'page': page}
            )
            text = r.content.decode('euc-kr', errors='replace')
            soup = BeautifulSoup(text, 'html.parser')
            found = False
            for row in soup.select('table.type_2 tr'):
                a = row.select_one('a.tltle')
                if not a: continue
                code_m = re.search(r'code=(\d{6})', a['href'])
                if not code_m: continue
                nums = [td.text.strip().replace(',', '') for td in row.select('td.number')]
                mktcap = int(nums[4]) if len(nums) > 4 and nums[4].isdigit() else 0
                results.append((code_m.group(1), a.text.strip(), mktcap))
                found = True
            if not found: break
        except Exception as e:
            print(f'  [WARN] page {page}: {e}')
            break   # 호스트가 응답하지 않는 상태에서 남은 페이지를 더 두드릴 이유가 없다
    seen, unique = set(), []
    for code, name, mktcap in results:
        if code not in seen:
            seen.add(code)
            unique.append((code, name.strip(), mktcap))
    return unique

def fetch_exclude_codes():
    codes = set()
    for url, key in [
        ('https://finance.naver.com/api/sise/etfItemList.nhn', 'etfItemList'),
        ('https://finance.naver.com/api/sise/etnItemList.nhn', 'etnItemList'),
    ]:
        try:
            for item in naver_get(url).json()['result'][key]:
                codes.add(item['itemcode'])
        except Exception as e:
            print(f'  [WARN] {url}: {e}')
    return codes

def is_preferred(name):
    return bool(re.search(r'\d*우[A-Z]?$', name))

print('[KR] 종목 리스트 수집 중...')
t0 = time.time()
exclude_codes = fetch_exclude_codes()
kospi  = fetch_stock_list(0, KOSPI_PAGES)
kosdaq = fetch_stock_list(1, KOSDAQ_PAGES)

all_stocks = [
    (code, name, mktcap) for code, name, mktcap in (kospi + kosdaq)
    if code not in exclude_codes and not is_preferred(name) and mktcap >= 10000
]
print(f'  최종: {len(all_stocks)}종목')

# ── 온전성 검사 ①: 종목 리스트 ────────────────────────────────────────────────
# 네이버가 안 되면 리스트가 비거나 반쪽이 된다. 그대로 진행하면 아래에서
# IndexError 로 죽거나, 더 나쁘게는 정상 데이터를 반쪽으로 덮어쓴다.
prev_count = 0
try:
    prev_count = int(firebase_db.reference('/v1/kr/stock_count').get() or 0)
except Exception as e:
    print(f'  [WARN] 기존 종목 수 조회 실패: {e}')
stock_floor = int(prev_count * MIN_STOCK_RATIO) if prev_count else MIN_STOCK_ABS
if len(all_stocks) < stock_floor:
    skip(f'종목 {len(all_stocks)}개 < 하한 {stock_floor}개 '
         f'(기존 {prev_count or "미상"}개) — 네이버 수집 실패로 판단')

end_dt   = datetime.today()
start_dt = end_dt - timedelta(days=int(HISTORY_DAYS * 1.5))
start_str, end_str = start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d')

def fetch_prices(code):
    try:
        # tries=2: 종목당 호출이라 재시도를 늘리면 전체 시간이 종목 수만큼 불어난다
        r = naver_get(
            f'https://api.stock.naver.com/chart/domestic/item/{code}/day',
            tries=2,
            params={'startDateTime': start_str+'000000', 'endDateTime': end_str+'235959'},
            headers={'Referer': 'https://finance.naver.com/'}
        )
        if r.status_code != 200: return code, {}
        prices = {}
        for item in r.json():
            d = item.get('localDate', '')
            close = item.get('closePrice')
            if d and close:
                prices[f'{d[:4]}-{d[4:6]}-{d[6:]}'] = int(close)
        return code, prices
    except Exception:
        return code, {}

print(f'\n[KR] 가격 수집 중 ({len(all_stocks)}종목)...')
price_map = {}
done = 0
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    futures = {ex.submit(fetch_prices, code): code for code, _, __ in all_stocks}
    for fut in as_completed(futures):
        code, prices = fut.result()
        price_map[code] = prices
        done += 1
        if done % 50 == 0 or done == len(all_stocks):
            print(f'  {done}/{len(all_stocks)} ({time.time()-t0:.0f}s)')

date_count = Counter()
for prices in price_map.values():
    for d in prices: date_count[d] += 1
valid_dates = sorted(
    [d for d, cnt in date_count.items() if cnt >= len(all_stocks) * 0.8], reverse=True
)
print(f'  유효 날짜: {len(valid_dates)}일')

# ── 온전성 검사 ②: 거래일 ─────────────────────────────────────────────────────
# 아래에서 valid_dates[0], valid_dates[1] 을 쓴다. 2일 미만이면 전일 대비를 못 만든다.
if len(valid_dates) < 2:
    skip(f'유효 거래일 {len(valid_dates)}일 (2일 이상 필요) — 가격 수집 실패')

# ── 지수 수집 ──────────────────────────────────────────────────────────────────
# KOSPI·KOSPI200: NAVER chart API (GitHub Actions에서 작동 확인)
# KOSDAQ 150: yfinance ^KQ11 (NAVER 미지원, 코스닥 종합으로 대체)
import yfinance as _yf

def _naver_idx(chart_code, name):
    """NAVER chart API — 최근 2거래일 종가 차이"""
    today = datetime.today()
    start = today - timedelta(days=10)
    try:
        r = naver_get(
            f'https://api.stock.naver.com/chart/domestic/index/{chart_code}/day',
            params={'startDateTime': start.strftime('%Y%m%d') + '000000',
                    'endDateTime':   today.strftime('%Y%m%d') + '235959'}
        )
        if r.status_code != 200:
            print(f'  [WARN] {name} NAVER/{chart_code}: HTTP {r.status_code}')
            return None
        items = r.json()
        if not items or len(items) < 2:
            print(f'  [WARN] {name} NAVER/{chart_code}: 데이터 {len(items) if items else 0}건')
            return None
        s = sorted(items, key=lambda x: x.get('localDate', ''), reverse=True)
        def _v(item):
            for k in ['closePrice', 'closeIndexPrice', 'close']:
                v = item.get(k)
                if v: return float(v)
            return 0.0
        curr, prev = _v(s[0]), _v(s[1])
        if not curr or not prev:
            return None
        change = curr - prev
        print(f'  {name}: {curr:,.2f} ({change:+.2f}, {change/prev*100:+.2f}%) '
              f'[NAVER {s[0].get("localDate","")}←{s[1].get("localDate","")}]')
        return {'name': name, 'value': round(curr, 2),
                'change': round(change, 2), 'changePct': round(change / prev * 100, 4)}
    except Exception as e:
        print(f'  [WARN] {name} NAVER/{chart_code}: {e}')
    return None

print('\n[KR] 시장 지수 수집 중...')
curr_date = valid_dates[0]
prev_date = valid_dates[1]
indices   = {}

# KOSPI, KOSPI 200: NAVER chart API
for chart_code, name, key in [('KOSPI', 'KOSPI', 'kospi'), ('KPI200', 'KOSPI 200', 'kospi200')]:
    result = _naver_idx(chart_code, name)
    if result:
        indices[key] = result

# 코스닥: yfinance ^KQ11 (코스닥 종합)
try:
    bd_p = datetime.strptime(prev_date, '%Y-%m-%d')
    bd_c = datetime.strptime(curr_date, '%Y-%m-%d')
    hist = _yf.Ticker('^KQ11').history(
        start=(bd_p - timedelta(days=7)).strftime('%Y-%m-%d'),
        end  =(bd_c + timedelta(days=2)).strftime('%Y-%m-%d'))
    if not hist.empty:
        d_strs = hist.index.strftime('%Y-%m-%d').tolist()
        closes = hist['Close'].tolist()
        c_list = [(d, c) for d, c in zip(d_strs, closes) if d <= curr_date]
        p_list = [(d, c) for d, c in zip(d_strs, closes) if d <= prev_date]
        if c_list and p_list and c_list[-1][0] != p_list[-1][0]:
            c_d, c_v = c_list[-1]
            p_d, p_v = p_list[-1]
            chg = float(c_v) - float(p_v)
            pct = chg / float(p_v) * 100
            print(f'  코스닥: {float(c_v):,.2f} ({chg:+.2f}, {pct:+.2f}%) [yfinance/^KQ11 {c_d}←{p_d}]')
            indices['kosdaq'] = {'name': '코스닥', 'value': round(float(c_v), 2),
                                 'change': round(chg, 2), 'changePct': round(pct, 4)}
except Exception as e:
    print(f'  [WARN] 코스닥 yfinance: {e}')

# ── Firebase 업로드 (지수 히스토리 날짜별 누적) ────────────────────────────────
print('\n[KR] Firebase 업로드 중...')
stocks_data = [{'c': code, 'n': name, 'm': mktcap} for code, name, mktcap in all_stocks]
prices_data = [
    [price_map.get(code, {}).get(date, 0) for code, _, __ in all_stocks]
    for date in valid_dates
]
KST = timezone(timedelta(hours=9))
collected_at = datetime.now(KST).strftime('%Y-%m-%d %H:%M')

# 기존 지수 히스토리 읽어서 오늘 날짜 추가 후 최근 400일만 유지
existing_raw = firebase_db.reference('/v1/kr/indices').get() or {}
import re as _re
_date_re = _re.compile(r'^\d{4}-\d{2}-\d{2}$')
existing_indices = {k: v for k, v in existing_raw.items() if _date_re.match(k)}
existing_indices[curr_date] = indices
all_idx_dates = sorted(existing_indices.keys(), reverse=True)
indices_history = {d: existing_indices[d] for d in all_idx_dates[:400]}

firebase_db.reference('/v1/kr').set({
    'updated': valid_dates[0], 'collected_at': collected_at,
    'stocks': stocks_data, 'stock_count': len(stocks_data),
    'dates': valid_dates, 'prices': prices_data,
    'indices': indices_history
})
print(f'[KR] 완료! ({time.time()-t0:.0f}초)')
