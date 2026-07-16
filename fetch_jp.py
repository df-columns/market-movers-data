# fetch_jp.py  ─  일본 주식 데이터 수집 → Firebase /v1/jp
# Yahoo 스크리너(yfinance)로 시총 상위 유니버스 확보 → yfinance로 가격 수집

import warnings, json, os, time
import pandas as pd
import yfinance as yf
from yfinance import EquityQuery
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

MARKET       = 'jp'
REGIONS      = ['jp']          # Yahoo 지역 코드
TOP_N        = 200             # 시총 상위 N종목
HISTORY_DAYS = 400
# 지수: (후보 심볼 리스트, 표시명, 키) — 앞에서부터 데이터 있는 심볼 사용
INDEX_DEFS   = [
    (['^N225'], 'Nikkei 225', 'n225'),
    (['^TPX', '^TOPX', '998405.T', '1306.T'], 'TOPIX', 'topix'),
]

t0 = time.time()


# ── 1. 유니버스 (Yahoo 스크리너: 시총 상위, 코드+이름+시총+통화 한 번에) ─────────
def screen_universe(regions, top_n):
    if len(regions) == 1:
        region_q = EquityQuery('eq', ['region', regions[0]])
    else:
        region_q = EquityQuery('or', [EquityQuery('eq', ['region', r]) for r in regions])
    q = EquityQuery('and', [region_q, EquityQuery('gt', ['intradaymarketcap', 0])])
    quotes, size = [], 250
    for offset in range(0, top_n + size, size):
        if len(quotes) >= top_n:
            break
        try:
            res = yf.screen(q, offset=offset, size=size,
                            sortField='intradaymarketcap', sortAsc=False)
        except Exception as e:
            print(f'  [WARN] screen offset={offset}: {e}')
            break
        batch = res.get('quotes', []) if isinstance(res, dict) else []
        if not batch:
            break
        quotes.extend(batch)
        if len(batch) < size:
            break
    return quotes[:top_n]


print(f'[{MARKET.upper()}] 유니버스 수집 (Yahoo 스크리너, 상위 {TOP_N})...')
quotes = screen_universe(REGIONS, TOP_N)
stocks = []
for qd in quotes:
    sym = qd.get('symbol')
    mc = qd.get('marketCap')
    if not sym or not mc:
        continue
    name = qd.get('shortName') or qd.get('longName') or sym
    cur = qd.get('currency') or ''
    stocks.append((sym, name, int(mc), cur))
print(f'  유니버스: {len(stocks)}종목')
if not stocks:
    raise SystemExit(f'[{MARKET.upper()}] 유니버스가 비었습니다. 스크리너 응답 확인 필요.')

symbols = [s[0] for s in stocks]

# ── 2. 가격 데이터 (일괄 다운로드) ─────────────────────────────────────────────
print(f'\n[{MARKET.upper()}] 가격 수집 중 ({len(symbols)}종목)...')
end_dt   = datetime.today()
start_dt = end_dt - timedelta(days=int(HISTORY_DAYS * 1.5))
raw = yf.download(
    symbols,
    start=start_dt.strftime('%Y-%m-%d'),
    end=(end_dt + timedelta(days=1)).strftime('%Y-%m-%d'),
    auto_adjust=True, progress=False, threads=True
)
if hasattr(raw, 'columns') and hasattr(raw.columns, 'levels'):
    close_prices = raw['Close']
else:
    close_prices = raw[['Close']].rename(columns={'Close': symbols[0]}) if 'Close' in raw.columns else raw
close_prices.columns = [str(c) for c in close_prices.columns]
print(f'  다운로드 완료 ({time.time()-t0:.0f}s)')

# ── 3. 가격 데이터 있는 종목만 유지 ────────────────────────────────────────────
available = set(close_prices.columns)
all_stocks_full = [(sym, name, mc, cur) for sym, name, mc, cur in stocks if sym in available]
print(f'  가격 데이터 있음: {len(all_stocks_full)}종목')

# ── 4. 유효 날짜 (80% 이상 종목이 값 보유) ─────────────────────────────────────
tickers_filtered = [s[0] for s in all_stocks_full]
coverage  = close_prices[tickers_filtered].notna().sum(axis=1)
threshold = len(tickers_filtered) * 0.8
valid_idx = coverage[coverage >= threshold].index
valid_dates = sorted([d.strftime('%Y-%m-%d') for d in valid_idx], reverse=True)[:HISTORY_DAYS]
print(f'  유효 날짜: {len(valid_dates)}일 ({valid_dates[-1]} ~ {valid_dates[0]})')

# ── 5. 가격 행렬 ───────────────────────────────────────────────────────────────
prices_data = []
for date in valid_dates:
    row = []
    for sym, _, __, ___ in all_stocks_full:
        try:
            p = close_prices.loc[date, sym]
            row.append(round(float(p) * 100) if pd.notna(p) else 0)
        except Exception:
            row.append(0)
    prices_data.append(row)

# ── 6. 지수 수집 ───────────────────────────────────────────────────────────────
print(f'\n[{MARKET.upper()}] 지수 수집 중...')
indices = {}
for syms, name, key in INDEX_DEFS:
    got = False
    for sym in syms:
        try:
            hist = yf.Ticker(sym).history(period='5d')
            if len(hist) >= 2:
                curr = float(hist['Close'].iloc[-1])
                prev = float(hist['Close'].iloc[-2])
                chg = curr - prev
                pct = chg / prev * 100
                indices[key] = {'name': name, 'value': round(curr, 2),
                                'change': round(chg, 2), 'changePct': round(pct, 4)}
                print(f'  {name} ({sym}): {curr:,.2f} ({chg:+.2f}, {pct:+.2f}%)')
                got = True
                break
        except Exception as e:
            print(f'  [WARN] {sym}: {e}')
    if not got:
        print(f'  [WARN] {name}: 후보 심볼 모두 실패 {syms}')

# ── 7. Firebase 업로드 ─────────────────────────────────────────────────────────
print(f'\n[{MARKET.upper()}] Firebase 업로드 중...')
stocks_data = [{'c': sym, 'n': name, 'm': int(mc), 'cur': cur} for sym, name, mc, cur in all_stocks_full]
KST = timezone(timedelta(hours=9))
collected_at = datetime.now(KST).strftime('%Y-%m-%d %H:%M')

# 지수 히스토리 누적 (최근 400일)
import re as _re
existing_raw = firebase_db.reference(f'/v1/{MARKET}/indices').get() or {}
_date_re = _re.compile(r'^\d{4}-\d{2}-\d{2}$')
existing_indices = {k: v for k, v in existing_raw.items() if _date_re.match(k)}
existing_indices[valid_dates[0]] = indices
all_idx_dates = sorted(existing_indices.keys(), reverse=True)
indices_history = {d: existing_indices[d] for d in all_idx_dates[:400]}

firebase_db.reference(f'/v1/{MARKET}').set({
    'updated': valid_dates[0], 'collected_at': collected_at,
    'stocks': stocks_data, 'dates': valid_dates, 'prices': prices_data,
    'indices': indices_history
})
print(f'[{MARKET.upper()}] 완료! ({time.time()-t0:.0f}초)')
