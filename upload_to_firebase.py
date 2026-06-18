# upload_to_firebase.py  ─  GitHub Actions에서 실행되는 스크립트
# 네이버 금융 → 데이터 수집 → Firebase Realtime Database 업로드

import requests, urllib3, warnings, json, re, os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from datetime import datetime, timedelta
import time
import firebase_admin
from firebase_admin import credentials, db as firebase_db

warnings.filterwarnings('ignore')
urllib3.disable_warnings()

# ── Firebase 초기화 ────────────────────────────────────────────────────────────
firebase_key = json.loads(os.environ['FIREBASE_KEY'])
cred = credentials.Certificate(firebase_key)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'
})

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

# ── 1. 종목 리스트 ─────────────────────────────────────────────────────────────
def fetch_stock_list(market_code, pages):
    results = []
    for page in range(1, pages + 1):
        try:
            r = session.get(
                'https://finance.naver.com/sise/sise_market_sum.nhn',
                params={'sosok': market_code, 'page': page},
                timeout=10
            )
            text = r.content.decode('euc-kr', errors='replace')
            soup = BeautifulSoup(text, 'html.parser')
            found = False
            for row in soup.select('table.type_2 tr'):
                a = row.select_one('a.tltle')
                if not a:
                    continue
                code_m = re.search(r'code=(\d{6})', a['href'])
                if not code_m:
                    continue
                code = code_m.group(1)
                name = a.text.strip()
                nums = [td.text.strip().replace(',', '') for td in row.select('td.number')]
                mktcap = int(nums[4]) if len(nums) > 4 and nums[4].isdigit() else 0
                results.append((code, name, mktcap))
                found = True
            if not found:
                break
        except Exception as e:
            print(f'  [WARN] list page {page}: {e}')
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
            r = session.get(url, timeout=10)
            for item in r.json()['result'][key]:
                codes.add(item['itemcode'])
        except Exception as e:
            print(f'  [WARN] {url}: {e}')
    return codes

def is_preferred(name):
    return bool(re.search(r'\d*우[A-Z]?$', name))

print('종목 리스트 수집 중...')
exclude_codes = fetch_exclude_codes()
kospi  = fetch_stock_list(0, KOSPI_PAGES)
kosdaq = fetch_stock_list(1, KOSDAQ_PAGES)

all_stocks = [
    (code, name, mktcap) for code, name, mktcap in (kospi + kosdaq)
    if code not in exclude_codes and not is_preferred(name) and mktcap >= 10000
]
print(f'  최종: {len(all_stocks)}종목')

# ── 2. 가격 데이터 ─────────────────────────────────────────────────────────────
end_dt   = datetime.today()
start_dt = end_dt - timedelta(days=int(HISTORY_DAYS * 1.5))
start_str = start_dt.strftime('%Y%m%d')
end_str   = end_dt.strftime('%Y%m%d')

def fetch_prices(code):
    try:
        r = session.get(
            f'https://api.stock.naver.com/chart/domestic/item/{code}/day',
            params={'startDateTime': start_str + '000000', 'endDateTime': end_str + '235959'},
            timeout=15,
            headers={'Referer': 'https://finance.naver.com/'}
        )
        if r.status_code != 200:
            return code, {}
        prices = {}
        for item in r.json():
            d = item.get('localDate', '')
            close = item.get('closePrice')
            if d and close:
                prices[f'{d[:4]}-{d[4:6]}-{d[6:]}'] = int(close)
        return code, prices
    except Exception:
        return code, {}

print(f'\n가격 데이터 수집 중 ({len(all_stocks)}종목)...')
t0 = time.time()
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

# ── 3. 날짜 공통 인덱스 ────────────────────────────────────────────────────────
date_count = Counter()
for prices in price_map.values():
    for d in prices:
        date_count[d] += 1

threshold  = len(all_stocks) * 0.8
valid_dates = sorted(
    [d for d, cnt in date_count.items() if cnt >= threshold],
    reverse=True
)
print(f'\n유효 날짜: {len(valid_dates)}일 ({valid_dates[-1]} ~ {valid_dates[0]})')

# ── 4. Firebase 업로드 ─────────────────────────────────────────────────────────
print('\nFirebase 업로드 중...')

stocks_data = [{'c': code, 'n': name, 'm': mktcap} for code, name, mktcap in all_stocks]

prices_data = []
for date in valid_dates:
    row = [price_map.get(code, {}).get(date, 0) for code, _, __ in all_stocks]
    prices_data.append(row)

ref = firebase_db.reference('/v1')
ref.set({
    'updated': valid_dates[0],
    'stocks': stocks_data,
    'dates': valid_dates,
    'prices': prices_data
})

print(f'완료! 업데이트 일자: {valid_dates[0]}  ({time.time()-t0:.0f}초)')
