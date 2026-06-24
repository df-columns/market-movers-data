# fetch_us.py  ─  미국 주식 데이터 수집 → Firebase /v1/us

import warnings, json, os, time
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
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
USD_20B = 20_000_000_000

print('[US] 종목 리스트 수집 중...')
t0 = time.time()

# ── 1. Nasdaq 공식 스크리너 API (한국 네이버 방식과 동일한 구조) ───────────────
import requests as _req

def get_tickers_via_nasdaq():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nasdaq.com/',
    }
    tickers, mktcaps = {}, {}
    for exchange in ['nasdaq', 'nyse', 'amex']:
        resp = _req.get(
            'https://api.nasdaq.com/api/screener/stocks',
            params={'tableonly': 'true', 'limit': 5000, 'download': 'true', 'exchange': exchange},
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        rows = resp.json().get('data', {}).get('rows', [])
        cnt = 0
        for row in rows:
            sym = (row.get('symbol') or '').strip()
            if not sym or not all(c.isalpha() or c == '-' for c in sym):
                continue  # 우선주·워런트·ETF 등 특수기호 제거
            mc_str = (row.get('marketCap') or '').replace(',', '').strip()
            try:
                mc = int(float(mc_str)) if mc_str else 0
            except:
                mc = 0
            if mc >= USD_20B:
                tickers[sym] = row.get('name', sym)
                mktcaps[sym] = mc
                cnt += 1
        print(f'  {exchange.upper()}: {len(rows)}종목 조회 → $20B+ {cnt}개')
    return tickers, mktcaps

tickers, mktcap_map = {}, {}
try:
    tickers, mktcap_map = get_tickers_via_nasdaq()
    print(f'  ✓ Nasdaq API: 총 {len(tickers)}종목 (시총 포함)')
except Exception as e:
    print(f'  [WARN] Nasdaq API 실패: {e}')

if not tickers:
    print('  [FALLBACK] 백업 리스트 사용')
    BACKUP = [
        'AAPL','MSFT','NVDA','AMZN','GOOGL','GOOG','META','TSLA','BRK-B','AVGO',
        'JPM','V','MA','UNH','XOM','LLY','JNJ','WMT','COST','HD','PG','ABBV',
        'BAC','NFLX','MRK','ORCL','CRM','AMD','CVX','TMO','KO','PEP','ACN',
        'MCD','ABT','GS','MS','IBM','CSCO','QCOM','TXN','INTU','ADBE','NOW',
        'AMGN','RTX','HON','CAT','DE','UPS','LMT','SCHW','BLK','GE','NEE',
        'ISRG','PANW','LRCX','AMAT','MU','KLAC','ADI','UBER','CRWD','MELI',
    ]
    tickers = {t: t for t in BACKUP}

ticker_list = list(tickers.keys())
print(f'  확정 유니버스: {len(ticker_list)}종목')

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

# ── 3. 시가총액 (Nasdaq API에서 받은 경우 생략) ────────────────────────────────
if not mktcap_map:
    print(f'\n[US] 시가총액 수집 중 ({len(ticker_list)}종목)...')

    def get_mktcap(ticker):
        try:
            mc = yf.Ticker(ticker).fast_info.market_cap
            return ticker, int(mc) if mc else 0
        except:
            return ticker, 0

    done = 0
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(get_mktcap, t): t for t in ticker_list}
        for fut in as_completed(futures):
            t, mc = fut.result()
            mktcap_map[t] = mc
            done += 1
            if done % 100 == 0 or done == len(ticker_list):
                print(f'  {done}/{len(ticker_list)} ({time.time()-t0:.0f}s)')
else:
    print(f'\n[US] 시가총액: Nasdaq API에서 수집됨 → fetch 생략')

# ── 4. 필터링 ($20B+, 가격 데이터 있는 종목) ──────────────────────────────────
available_tickers = set(close_prices.columns)
all_stocks_full = [
    (t, tickers.get(t, t), mktcap_map.get(t, 0))
    for t in ticker_list
    if mktcap_map.get(t, 0) >= USD_20B and t in available_tickers
]
print(f'\n$20B 이상 & 데이터 있음: {len(all_stocks_full)}종목')

# ── 4. 유효 날짜 ───────────────────────────────────────────────────────────────
tickers_filtered = [t for t, _, __ in all_stocks_full]
coverage  = close_prices[tickers_filtered].notna().sum(axis=1)
threshold = len(tickers_filtered) * 0.8
valid_idx = coverage[coverage >= threshold].index

valid_dates = sorted(
    [d.strftime('%Y-%m-%d') for d in valid_idx],
    reverse=True
)[:HISTORY_DAYS]
print(f'유효 날짜: {len(valid_dates)}일 ({valid_dates[-1]} ~ {valid_dates[0]})')

# ── 5. 가격 행렬 ───────────────────────────────────────────────────────────────
print('\n[US] 가격 행렬 구성 중...')
prices_data = []
for date in valid_dates:
    row = []
    for ticker, _, __ in all_stocks_full:
        try:
            p = close_prices.loc[date, ticker]
            row.append(round(float(p) * 100) if pd.notna(p) else 0)
        except:
            row.append(0)
    prices_data.append(row)

# ── 6. 지수 수집 (S&P 500, NASDAQ 100, Dow 30) ────────────────────────────────
print('\n[US] 시장 지수 수집 중...')
indices = {}
for sym, name, key in [('^GSPC', 'S&P 500', 'sp500'), ('^NDX', 'NASDAQ 100', 'ndx100'), ('^DJI', 'Dow 30', 'dji30')]:
    try:
        hist = yf.Ticker(sym).history(period='5d')
        if len(hist) >= 2:
            curr = float(hist['Close'].iloc[-1])
            prev = float(hist['Close'].iloc[-2])
            change = curr - prev
            changePct = (change / prev) * 100
            indices[key] = {'name': name, 'value': round(curr, 2),
                            'change': round(change, 2), 'changePct': round(changePct, 4)}
            print(f'  {name}: {curr:,.2f} ({change:+.2f}, {changePct:+.2f}%)')
        else:
            print(f'  [WARN] {sym}: 데이터 부족 ({len(hist)}일)')
    except Exception as e:
        print(f'  [WARN] {sym}: {e}')

# ── 7. Firebase 업로드 ─────────────────────────────────────────────────────────
print('\n[US] Firebase 업로드 중...')
stocks_data = [{'c': t, 'n': name, 'm': int(mc)} for t, name, mc in all_stocks_full]

KST = timezone(timedelta(hours=9))
collected_at = datetime.now(KST).strftime('%Y-%m-%d %H:%M')

firebase_db.reference('/v1/us').set({
    'updated': valid_dates[0], 'collected_at': collected_at,
    'stocks': stocks_data, 'dates': valid_dates, 'prices': prices_data,
    'indices': indices
})
print(f'[US] 완료! ({time.time()-t0:.0f}초)')
