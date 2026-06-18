# fetch_us.py  ─  미국 주식 데이터 수집 (S&P 500) → Firebase /v1/us

import warnings, json, os, time
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db as firebase_db

warnings.filterwarnings('ignore')

# ── Firebase 초기화 ────────────────────────────────────────────────────────────
cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
try:
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'
    })
except ValueError:
    pass

HISTORY_DAYS = 400

# ── 1. S&P 500 종목 리스트 ──────────────────────────────────────────────────────
print('[US] S&P 500 종목 리스트 수집 중...')
t0 = time.time()

def parse_wiki_table(url, sym_candidates, name_candidates):
    """Wikipedia 테이블에서 {ticker: name} 딕셔너리 반환"""
    tables = pd.read_html(url)
    df = tables[0]
    sym_col  = next((c for c in sym_candidates if c in df.columns), df.columns[0])
    name_col = next((c for c in name_candidates if c in df.columns), df.columns[1])
    return {
        str(row[sym_col]).replace('.', '-'): str(row[name_col])
        for _, row in df.iterrows()
    }

tickers = {}
try:
    sp500 = parse_wiki_table(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        sym_candidates  = ['Symbol', 'Ticker'],
        name_candidates = ['Security', 'Company', 'Name']
    )
    tickers.update(sp500)
    print(f'  S&P 500: {len(sp500)}종목')
except Exception as e:
    print(f'  [WARN] S&P 500 파싱 실패: {e}')

try:
    ndx100 = parse_wiki_table(
        'https://en.wikipedia.org/wiki/Nasdaq-100',
        sym_candidates  = ['Ticker', 'Symbol'],
        name_candidates = ['Company', 'Security', 'Name']
    )
    new_ndx = {k: v for k, v in ndx100.items() if k not in tickers}
    tickers.update(new_ndx)
    print(f'  NASDAQ 100: {len(ndx100)}종목 (신규 {len(new_ndx)}개 추가)')
except Exception as e:
    print(f'  [WARN] NASDAQ 100 파싱 실패: {e}')

if not tickers:
    print('  [WARN] Wikipedia 파싱 전부 실패 → 백업 리스트 사용')
    tickers = {
        'AAPL':'Apple Inc.','MSFT':'Microsoft Corporation','NVDA':'NVIDIA Corporation',
        'AMZN':'Amazon.com Inc.','GOOGL':'Alphabet Inc.','META':'Meta Platforms Inc.',
        'TSLA':'Tesla Inc.','BRK-B':'Berkshire Hathaway Inc.','UNH':'UnitedHealth Group',
        'JPM':'JPMorgan Chase & Co.','XOM':'Exxon Mobil Corporation','JNJ':'Johnson & Johnson',
        'V':'Visa Inc.','MA':'Mastercard Incorporated','PG':'Procter & Gamble Co.',
    }

print(f'  합산 유니버스: {len(tickers)}종목')

ticker_list = list(tickers.keys())

# ── 2. 가격 데이터 (일괄 다운로드) ─────────────────────────────────────────────
print(f'\n[US] 가격 데이터 수집 중 ({len(ticker_list)}종목)...')
end_dt   = datetime.today()
start_dt = end_dt - timedelta(days=int(HISTORY_DAYS * 1.5))

raw = yf.download(
    ticker_list,
    start=start_dt.strftime('%Y-%m-%d'),
    end=(end_dt + timedelta(days=1)).strftime('%Y-%m-%d'),
    auto_adjust=True,
    progress=False,
    threads=True
)

if hasattr(raw, 'columns') and hasattr(raw.columns, 'levels'):
    close_prices = raw['Close']
else:
    close_prices = raw[['Close']].rename(columns={'Close': ticker_list[0]}) if 'Close' in raw.columns else raw

close_prices.columns = [str(c) for c in close_prices.columns]
print(f'  다운로드 완료 ({time.time()-t0:.0f}s)')

# ── 3. 시가총액 수집 (병렬) ────────────────────────────────────────────────────
print(f'\n[US] 시가총액 수집 중 ({len(ticker_list)}종목)...')

def get_mktcap(ticker):
    try:
        fi = yf.Ticker(ticker).fast_info
        mc = fi.market_cap
        if not mc:
            mc = yf.Ticker(ticker).info.get('marketCap', 0)
        return ticker, int(mc) if mc else 0
    except:
        return ticker, 0

mktcap_map = {}
done = 0
with ThreadPoolExecutor(max_workers=20) as ex:
    futures = {ex.submit(get_mktcap, t): t for t in ticker_list}
    for fut in as_completed(futures):
        t, mc = fut.result()
        mktcap_map[t] = mc
        done += 1
        if done % 50 == 0 or done == len(ticker_list):
            print(f'  {done}/{len(ticker_list)} ({time.time()-t0:.0f}s)')

# ── 4. 필터링 ($10B+ 시가총액, 가격 데이터 있는 종목) ────────────────────────────
USD_10B = 10_000_000_000
available_tickers = set(close_prices.columns)
all_stocks = [
    (t, tickers[t], mktcap_map.get(t, 0))
    for t in ticker_list
    if mktcap_map.get(t, 0) >= USD_10B and t in available_tickers
]
print(f'\n$10B 이상 & 데이터 있음: {len(all_stocks)}종목')

# ── 5. 유효 날짜 (80% 이상 종목에 데이터 있는 거래일) ────────────────────────────
tickers_filtered = [t for t, _, __ in all_stocks]
coverage = close_prices[tickers_filtered].notna().sum(axis=1)
threshold = len(tickers_filtered) * 0.8
valid_idx  = coverage[coverage >= threshold].index

valid_dates = sorted(
    [d.strftime('%Y-%m-%d') for d in valid_idx],
    reverse=True
)[:HISTORY_DAYS]
print(f'유효 날짜: {len(valid_dates)}일 ({valid_dates[-1]} ~ {valid_dates[0]})')

# ── 6. 가격 행렬 (cents 단위 정수 — 수익률 계산용, 절대가격 불필요) ──────────────
print('\n[US] 가격 행렬 구성 중...')
prices_data = []
for date in valid_dates:
    row = []
    for ticker, _, __ in all_stocks:
        try:
            p = close_prices.loc[date, ticker]
            row.append(round(float(p) * 100) if pd.notna(p) else 0)
        except:
            row.append(0)
    prices_data.append(row)

# ── 7. Firebase 업로드 ─────────────────────────────────────────────────────────
print('\n[US] Firebase 업로드 중...')
stocks_data = [{'c': t, 'n': name, 'm': int(mc)} for t, name, mc in all_stocks]

firebase_db.reference('/v1/us').set({
    'updated': valid_dates[0],
    'stocks': stocks_data,
    'dates': valid_dates,
    'prices': prices_data
})
print(f'[US] 완료! ({time.time()-t0:.0f}초)')
