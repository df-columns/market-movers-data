# fetch_us.py  ─  미국 주식 데이터 수집 → Firebase /v1/us

import warnings, json, os, time
import requests
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

# ── 1. Yahoo Finance 스크리너 API 직접 호출 ────────────────────────────────────
def get_tickers_via_yahoo():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://finance.yahoo.com/',
    })

    # 쿠키 + crumb 획득 (한국 네이버 방식과 동일한 구조)
    try:
        session.get('https://fc.yahoo.com', timeout=5)
    except Exception:
        pass
    session.get('https://finance.yahoo.com', timeout=10)
    crumb = session.get(
        'https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=10
    ).text.strip()

    tickers, mktcaps = {}, {}
    offset = 0

    while offset < 5000:
        payload = {
            'offset': offset,
            'size': 250,
            'sortField': 'intradaymarketcap',
            'sortType': 'DESC',
            'quoteType': 'EQUITY',
            'query': {
                'operator': 'AND',
                'operands': [
                    {'operator': 'GT', 'operands': ['intradaymarketcap', USD_20B]},
                    {'operator': 'EQ', 'operands': ['region', 'us']}
                ]
            },
            'userId': '',
            'userIdType': 'guid'
        }
        resp = session.post(
            f'https://query1.finance.yahoo.com/v1/finance/screener'
            f'?crumb={crumb}&lang=en-US&region=US&formatted=false',
            json=payload,
            timeout=15
        )
        resp.raise_for_status()
        result = resp.json().get('finance', {}).get('result', [])
        if not result:
            break
        quotes = result[0].get('quotes', [])
        if not quotes:
            break

        for q in quotes:
            sym = q.get('symbol', '').replace('.', '-')
            if not sym or q.get('quoteType') != 'EQUITY':
                continue
            tickers[sym] = q.get('longName') or q.get('shortName', sym)
            mktcaps[sym] = int(q.get('marketCap', 0) or 0)

        print(f'  {len(tickers)}종목 수집 중... (offset={offset})')
        if len(quotes) < 250:
            break
        offset += 250

    return tickers, mktcaps

# ── 백업 리스트 (API 완전 실패 시) ────────────────────────────────────────────
BACKUP_TICKERS = {
    'AAPL':'Apple','MSFT':'Microsoft','NVDA':'NVIDIA','AMZN':'Amazon',
    'GOOGL':'Alphabet A','GOOG':'Alphabet C','META':'Meta','TSLA':'Tesla',
    'AVGO':'Broadcom','BRK-B':'Berkshire','JPM':'JPMorgan','V':'Visa',
    'MA':'Mastercard','UNH':'UnitedHealth','XOM':'Exxon','LLY':'Eli Lilly',
    'JNJ':'J&J','WMT':'Walmart','COST':'Costco','HD':'Home Depot',
    'PG':'P&G','ABBV':'AbbVie','BAC':'BofA','NFLX':'Netflix',
    'CRM':'Salesforce','MRK':'Merck','ORCL':'Oracle','AMD':'AMD',
    'INTC':'Intel','QCOM':'Qualcomm','TXN':'TI','AMAT':'Applied Materials',
    'LRCX':'Lam Research','KLAC':'KLA','MU':'Micron','NOW':'ServiceNow',
    'ADBE':'Adobe','INTU':'Intuit','PANW':'Palo Alto','GS':'Goldman',
    'MS':'Morgan Stanley','WFC':'Wells Fargo','BLK':'BlackRock',
    'TMO':'Thermo Fisher','ABT':'Abbott','ISRG':'Intuitive Surgical',
    'CVX':'Chevron','COP':'ConocoPhillips','CAT':'Caterpillar',
    'DE':'Deere','HON':'Honeywell','RTX':'RTX','LMT':'Lockheed',
    'GE':'GE Aerospace','UPS':'UPS','DIS':'Disney','CMCSA':'Comcast',
    'T':'AT&T','VZ':'Verizon','TMUS':'T-Mobile','AMGN':'Amgen',
    'GILD':'Gilead','VRTX':'Vertex','REGN':'Regeneron','ACN':'Accenture',
    'IBM':'IBM','CSCO':'Cisco','UBER':'Uber','MELI':'MercadoLibre',
    'WDAY':'Workday','ADSK':'Autodesk','MRVL':'Marvell','FTNT':'Fortinet',
    'SNPS':'Synopsys','CDNS':'Cadence','SPGI':'S&P Global',
    'KO':'Coca-Cola','PEP':'PepsiCo','MCD':"McDonald's",
    'SBUX':'Starbucks','NKE':'Nike','LOW':"Lowe's",'TGT':'Target',
    'PLD':'Prologis','AMT':'American Tower','NEE':'NextEra Energy',
    'GS':'Goldman','AXP':'Amex','COF':'Capital One','SCHW':'Schwab',
    'ICE':'ICE','CME':'CME Group','MSCI':'MSCI','MCO':"Moody's",
    'FI':'Fiserv','PYPL':'PayPal','CRWD':'CrowdStrike','SNOW':'Snowflake',
    'NET':'Cloudflare','DDOG':'Datadog','ZS':'Zscaler',
    'DHR':'Danaher','SYK':'Stryker','MDT':'Medtronic',
    'LIN':'Linde','APD':'Air Products','ECL':'Ecolab',
    'UNP':'Union Pacific','CSX':'CSX','FDX':'FedEx',
}

# ── 소스 선택 ──────────────────────────────────────────────────────────────────
mktcap_from_api = {}
tickers = {}

try:
    tickers, mktcap_from_api = get_tickers_via_yahoo()
    print(f'  ✓ Yahoo API: {len(tickers)}종목 (시총 $20B+)')
except Exception as e:
    print(f'  [WARN] Yahoo API 실패: {e} → 백업 리스트 사용')
    tickers = dict(BACKUP_TICKERS)

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

# ── 3. 시가총액 (API에서 이미 받은 경우 생략) ─────────────────────────────────
if mktcap_from_api:
    print(f'\n[US] 시가총액: Yahoo API에서 수집됨 → fetch 생략')
    mktcap_map = mktcap_from_api
else:
    print(f'\n[US] 시가총액 수집 중 ({len(ticker_list)}종목)...')

    def get_mktcap(ticker):
        try:
            mc = yf.Ticker(ticker).fast_info.market_cap
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

# ── 4. 필터링 ($20B+, 가격 데이터 있는 종목) ──────────────────────────────────
available_tickers = set(close_prices.columns)
all_stocks = [
    (t, tickers[t], mktcap_map.get(t, 0))
    for t in ticker_list
    if mktcap_map.get(t, 0) >= USD_20B and t in available_tickers
]
print(f'\n$20B 이상 & 데이터 있음: {len(all_stocks)}종목')

# ── 5. 유효 날짜 ───────────────────────────────────────────────────────────────
tickers_filtered = [t for t, _, __ in all_stocks]
coverage  = close_prices[tickers_filtered].notna().sum(axis=1)
threshold = len(tickers_filtered) * 0.8
valid_idx = coverage[coverage >= threshold].index

valid_dates = sorted(
    [d.strftime('%Y-%m-%d') for d in valid_idx],
    reverse=True
)[:HISTORY_DAYS]
print(f'유효 날짜: {len(valid_dates)}일 ({valid_dates[-1]} ~ {valid_dates[0]})')

# ── 6. 가격 행렬 ───────────────────────────────────────────────────────────────
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

KST = timezone(timedelta(hours=9))
collected_at = datetime.now(KST).strftime('%Y-%m-%d %H:%M')

firebase_db.reference('/v1/us').set({
    'updated': valid_dates[0], 'collected_at': collected_at,
    'stocks': stocks_data, 'dates': valid_dates, 'prices': prices_data
})
print(f'[US] 완료! ({time.time()-t0:.0f}초)')
