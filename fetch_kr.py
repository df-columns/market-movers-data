# fetch_kr.py  ─  국내 주식 데이터 수집 → Firebase /v1/kr

import requests, urllib3, warnings, json, re, os
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

session = requests.Session()
session.verify = False
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com/'
})

def fetch_stock_list(market_code, pages):
    results = []
    for page in range(1, pages + 1):
        try:
            r = session.get(
                'https://finance.naver.com/sise/sise_market_sum.nhn',
                params={'sosok': market_code, 'page': page}, timeout=10
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
            for item in session.get(url, timeout=10).json()['result'][key]:
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

end_dt   = datetime.today()
start_dt = end_dt - timedelta(days=int(HISTORY_DAYS * 1.5))
start_str, end_str = start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d')

def fetch_prices(code):
    try:
        r = session.get(
            f'https://api.stock.naver.com/chart/domestic/item/{code}/day',
            params={'startDateTime': start_str+'000000', 'endDateTime': end_str+'235959'},
            timeout=15, headers={'Referer': 'https://finance.naver.com/'}
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

# ── 지수 수집 (기준일: valid_dates[0]) ──────────────────────────────────────────
import yfinance as _yf

def fetch_index_yf(sym, name, base_date):
    """base_date(YYYY-MM-DD) 기준 종가와 전일 대비 변동폭을 yfinance로 수집"""
    try:
        bd    = datetime.strptime(base_date, '%Y-%m-%d')
        start = (bd - timedelta(days=14)).strftime('%Y-%m-%d')
        end   = (bd + timedelta(days=2)).strftime('%Y-%m-%d')
        hist  = _yf.Ticker(sym).history(start=start, end=end)
        if hist.empty or len(hist) < 2:
            print(f'  [WARN] {name}: 데이터 부족')
            return None
        date_strs = hist.index.strftime('%Y-%m-%d').tolist()
        closes    = hist['Close'].tolist()
        # base_date 이하 날짜만 추출 (기준일이 공휴일이면 그 직전 거래일 사용)
        valid = [(d, c) for d, c in zip(date_strs, closes) if d <= base_date]
        if len(valid) < 2:
            print(f'  [WARN] {name}: {base_date} 이전 데이터 부족')
            return None
        curr      = float(valid[-1][1])
        prev      = float(valid[-2][1])
        change    = curr - prev
        changePct = change / prev * 100
        print(f'  {name} ({sym}): {curr:,.2f} ({change:+.2f}, {changePct:+.2f}%) [{valid[-1][0]}]')
        return {'name': name, 'value': round(curr, 2),
                'change': round(change, 2), 'changePct': round(changePct, 4)}
    except Exception as e:
        print(f'  [WARN] {name} ({sym}): {e}')
        return None

print('\n[KR] 시장 지수 수집 중...')
base_date = valid_dates[0]
indices   = {}
for sym, name, key in [
    ('^KS11',  'KOSPI',      'kospi'),
    ('^KS200', 'KOSPI 200',  'kospi200'),
    ('^KQ11',  'KOSDAQ 150', 'kosdaq150'),
]:
    result = fetch_index_yf(sym, name, base_date)
    if result:
        indices[key] = result

# ── Firebase 업로드 ─────────────────────────────────────────────────────────────
print('\n[KR] Firebase 업로드 중...')
stocks_data = [{'c': code, 'n': name, 'm': mktcap} for code, name, mktcap in all_stocks]
prices_data = [
    [price_map.get(code, {}).get(date, 0) for code, _, __ in all_stocks]
    for date in valid_dates
]
KST = timezone(timedelta(hours=9))
collected_at = datetime.now(KST).strftime('%Y-%m-%d %H:%M')

firebase_db.reference('/v1/kr').set({
    'updated': valid_dates[0], 'collected_at': collected_at,
    'stocks': stocks_data, 'dates': valid_dates, 'prices': prices_data,
    'indices': indices
})
print(f'[KR] 완료! ({time.time()-t0:.0f}초)')
