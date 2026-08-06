# fetch_cn.py  ─  중국(상하이+선전+홍콩) 주식 데이터 수집 → Firebase /v1/cn
# Yahoo 스크리너(yfinance)로 시총 상위 유니버스 확보 → yfinance로 가격 수집
# ⚠️ 본토(CNY)+홍콩(HKD) 통화가 섞임 — 시총 정렬은 근사(환율 유사)로 처리, 표시는 통화별 기호 사용

import warnings, json, os, re, sys, time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import yfinance as yf
from yfinance import EquityQuery
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials, db as firebase_db

warnings.filterwarnings('ignore')

# Windows 콘솔(cp949)에서 한글·기호 print가 UnicodeEncodeError를 내면
# try/except 안의 수집 로직이 조용히 실패한다(실제로 지수 수집이 깨졌다).
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8')
    except Exception:
        pass

# ── Firebase 초기화 ────────────────────────────────────────────────────────────
cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
try:
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'
    })
except ValueError:
    pass

MARKET       = 'cn'
REGIONS      = ['cn', 'hk']     # 상하이(.SS)+선전(.SZ)=cn, 홍콩(.HK)=hk
TOP_N        = 200
HISTORY_DAYS = 400
KST          = timezone(timedelta(hours=9))

# ── A+H 자동 감지 설정 ─────────────────────────────────────────────────────────
IDENTITY_TTL_DAYS = 180   # 회사 identity(website) 캐시 유효기간
IDENTITY_WORKERS  = 8     # identity 조회 동시 실행 수
# website 가 같아도 병합하면 안 되는 (본토코드, 홍콩코드) 쌍 — 오탐이 나오면 여기 적는다
NEVER_MERGE = set()
# 지수 선정 근거 — Yahoo의 중국 지수 커버리지는 대부분 비어 있다(2026-07 확인).
#   정상: 000001.SS(상하이종합) 22행/1개월, ^HSI(항셍) 21행/1개월
#   불가: 000300.SS(CSI300)은 데이터 공백이 잦아 '전일 대비'가 며칠치가 되어 버림.
#         000688.SS(과창판50), 000016.SS(SSE50), 000905/000852(CSI500/1000),
#         399006/399673(창업판), 399330(선전100)은 1개월 조회 시 1행뿐이라 사용 불가.
#   → 데이터가 안정적인 2개만 쓴다. 과창판 종목은 개별 종목(688xxx)으로 이미 커버됨.
INDEX_DEFS   = [('000001.SS', '상하이종합', 'sse'),
                ('^HSI', 'Hang Seng', 'hsi')]
IDX_PERIOD        = '1mo'   # 5d는 데이터 공백에 취약해 지수가 조용히 누락됐다
IDX_GAP_WARN_DAYS = 4       # 이 이상 벌어지면 로그로 알린다
IDX_GAP_MAX_DAYS  = 5       # 이 이상이면 '전일 대비'가 아니므로 게시하지 않는다
# 임계값을 5로 둔 이유: 연휴로 인한 정상 공백과 데이터 누락은 캘린더 일수로
# 구분할 수 없다. 12일로 느슨하게 두면 CSI300처럼 11일 공백이 난 데이터의
# 11일치 변동률이 '전일 대비'로 게시된다(실측 확인). 춘절·국경절 직후
# 1거래일은 지수 카드가 비게 되지만(연 2회), 틀린 등락률을 싣는 것보다 낫다.

# A+H 이중상장 주요 종목 (본토코드, 홍콩코드) — 둘 다 잡히면 홍콩(H주) 제거, 본토(A주) 유지
DUAL_AH = [
    ('601398', '1398'), ('601939', '0939'), ('601288', '1288'), ('601988', '3988'),
    ('601328', '3328'), ('600036', '3968'), ('601658', '1658'), ('601998', '0998'),
    ('601818', '6818'), ('600016', '1988'), ('601318', '2318'), ('601628', '2628'),
    ('601601', '2601'), ('601319', '1339'), ('601336', '1336'), ('601857', '0857'),
    ('600028', '0386'), ('601088', '1088'), ('600941', '0941'), ('601728', '0728'),
    ('300750', '3750'), ('002594', '1211'), ('688981', '0981'), ('601899', '2899'),
    ('600030', '6030'), ('601898', '1898'), ('601633', '2333'), ('601238', '2238'),
    ('600585', '0914'), ('601111', '0753'), ('600029', '1055'), ('600115', '0670'),
    ('600837', '6837'), ('601881', '6881'), ('601688', '6886'), ('601390', '0390'),
    ('601186', '1186'), ('601800', '1800'), ('603259', '2359'), ('600600', '0168'),
    ('000338', '2338'), ('000333', '0300'), ('601766', '1766'), ('601066', '6066'),
]

# ── 신규상장 시드 ──────────────────────────────────────────────────────────────
# Yahoo 스크리너는 상장 직후 몇 주간 marketCap 을 채우지 않는다. 그 사이 그 종목은
# `intradaymarketcap > 0` 필터와 시총 정렬 양쪽에서 빠져 유니버스에 아예 못 든다.
#
#   2026-08-06 실측 — 688825.SS(CXMT/장신과기, 2026-07-27 과창판 상장):
#     · history('1mo')  → 8행 정상, 종가 54.30 CNY
#     · yf.download     → 정상 (2026-07-29 ~ 08-05 종가 확보)
#     · marketCap / sharesOutstanding / impliedSharesOutstanding → 전부 None
#     · screen(cn+hk, 시총순 상위 250) → 없음
#   시총 3.6조위안으로 중국 1위인데도 상장 열흘이 지나도록 리스트에 안 나왔다.
#
# 즉 모자란 건 '상장주식수' 하나뿐이다. 그 값만 여기 적어두고 시총은 매일
# 그날 종가로 다시 계산한다(가격은 Yahoo 로 정상 수집되므로 자동으로 최신화된다).
# Yahoo 가 marketCap 을 채우기 시작하면 스크리너가 종목을 잡게 되고,
# 그때는 Yahoo 값을 쓰면서 '[시드 은퇴 가능]' 로그를 남긴다 → 이 표에서 지우면 된다.
SEED_NEW_LISTINGS = [
    # (심볼, 표시명, 통화, 상장주식수, 근거)
    ('688825.SS', 'CXMT CORPORATION', 'CNY', 66_881_000_000,
     '2026-07-27 과창판 상장 · 발행 후 총주식수 668.81억주(초과배정 행사 전) · 공모가 8.66위안'),
]

t0 = time.time()


# ── 이중상장 자동 감지 (A주만 남기고 B주·H주 제거) ────────────────────────────
# DUAL_AH 표는 손으로 채우는 방식이라 계속 뒤처진다. 2026-08-06 전수 점검에서
# 표에 없는 이중상장 19쌍이 남아 있었다(CNOOC 600938/0883, 립쉰정밀 002475/2475,
# 헝루이 600276/1276, 하이얼스마트홈 600690/6690, VGT 300476/2476 …).
# 대부분 2026년에 몰린 A주 기업의 홍콩 2차 상장이다. CNOOC 는 시총 1위권에서
# 두 자리를 차지했고, 등락률은 두 시장이 따로 움직이니 무버 TOP10 에도 같은
# 회사가 각각 들어올 수 있었다.
# B주도 같은 문제다 — BOE(000725.SZ + 200725.SZ)처럼 둘 다 본토 상장이라
# '본토↔홍콩' 규칙만으로는 안 걸러졌다.
#
# 판정 키는 회사 website 다. 약칭(shortName)으로 맞추면 19쌍 중 5쌍만 잡힌다
# — Yahoo 가 A주·H주에 다른 약칭을 주는 경우가 많다('CNOOC LIMITED' vs 'CNOOC').
# website 는 get_info() 호출이 필요해 5분마다 도는 이 스크립트에서 매번 175회를
# 부를 수 없으므로, 회사 소개 캐시와 같은 방식으로 Firebase 에 캐싱한다.
# 정상 상태에서는 신규 상장 몇 건만 조회한다.
def safe_key(code):
    """Firebase 키에 쓸 수 없는 문자 치환 (300476.SZ → 300476_SZ)"""
    return re.sub(r'[.#$\[\]/]', '_', str(code))


# B주 코드 규칙 — 선전 B주는 200/201xxx(.SZ), 상하이 B주는 900xxx(.SS).
# BOE(京东方)처럼 A주와 B주가 같은 회사인데 둘 다 본토 상장이어서, '본토↔홍콩만
# 병합' 규칙으로는 걸러지지 않았다(2026-08-06 실측: 000725.SZ + 200725.SZ 보류).
B_SHARE_RE = re.compile(r'^(?:200|201)\d{3}\.SZ$|^900\d{3}\.SS$')


def listing_kind(sym):
    """상장 종류 — 'a'(본토 주력) / 'b'(본토 B주) / 'h'(홍콩).

    A주를 남기고 B주·H주를 제거한다. A주가 거래가 가장 활발하고 시총도 현지통화
    기준이라 정렬에 쓰기 적합하다.
    """
    if sym.endswith('.HK'):
        return 'h'
    if B_SHARE_RE.match(sym):
        return 'b'
    return 'a'


def identity_domain(url):
    """website URL → 비교용 호스트.

    서브도메인은 남긴다. 'smart-home.haier.com' 을 'haier.com' 으로 줄이면
    같은 그룹의 다른 상장사와 묶여 엉뚱한 종목이 사라진다.
    """
    s = (url or '').strip().lower()
    if not s:
        return ''
    s = re.sub(r'^[a-z][a-z0-9+.-]*://', '', s)
    s = s.split('/')[0].split('?')[0].split('#')[0]
    s = re.sub(r':\d+$', '', s)
    s = re.sub(r'^www\.', '', s)
    return s if '.' in s else ''


def load_identity(symbols):
    """캐시된 website 를 읽는다 (TTL 안쪽만). 빈 문자열도 유효한 값이다."""
    try:
        raw = firebase_db.reference(f'/identity/{MARKET}').get() or {}
    except Exception as e:
        print(f'  [WARN] identity 캐시 조회 실패: {e}')
        return {}
    today = datetime.now(KST).date()
    out = {}
    for sym in symbols:
        rec = raw.get(safe_key(sym))
        if not isinstance(rec, dict) or 'w' not in rec:
            continue
        try:
            age = (today - datetime.strptime(rec.get('u', '1970-01-01'), '%Y-%m-%d').date()).days
        except Exception:
            continue
        if age <= IDENTITY_TTL_DAYS:
            out[sym] = rec['w']
    return out


def fetch_identity(symbols):
    """캐시에 없는 종목의 website 를 Yahoo 에서 가져온다.

    조회 실패(None)는 캐시에 넣지 않아 다음 실행에서 다시 시도한다.
    website 가 없는 종목은 ''로 캐시해 매번 헛조회하지 않게 한다.
    """
    def one(sym):
        try:
            return sym, identity_domain((yf.Ticker(sym).get_info() or {}).get('website'))
        except Exception:
            return sym, None
    out = {}
    with ThreadPoolExecutor(max_workers=IDENTITY_WORKERS) as pool:
        for sym, dom in pool.map(one, symbols):
            if dom is not None:
                out[sym] = dom
    return out


def save_identity(fresh):
    if not fresh:
        return
    today = datetime.now(KST).strftime('%Y-%m-%d')
    try:
        firebase_db.reference(f'/identity/{MARKET}').update(
            {safe_key(sym): {'w': dom, 'u': today} for sym, dom in fresh.items()})
    except Exception as e:
        print(f'  [WARN] identity 캐시 저장 실패: {e}')


def dedupe_by_identity(stocks, ident):
    """website 가 같은 종목들에서 A주만 남기고 B주·H주를 제거한다.

    A주 1개 + (B주/H주) 1~2개일 때만 병합한다. A주가 없거나 2개 이상이면
    어느 쪽이 주력인지 알 수 없으므로 병합하지 않고 로그만 남긴다.
    같은 도메인에 관계없는 상장사가 여럿 걸릴 수 있으므로(모회사·자회사)
    조용히 종목을 지우기보다 보류하는 쪽을 택했다.

    반환: (남은 stocks, 병합한 쌍, 판단 보류한 그룹)
    """
    groups = {}
    for row in stocks:
        dom = ident.get(row[0])
        if dom:
            groups.setdefault(dom, []).append(row[0])

    drop, merged, skipped = set(), [], []
    for dom, syms in sorted(groups.items()):
        if len(syms) < 2:
            continue
        kinds = {}
        for s in syms:
            kinds.setdefault(listing_kind(s), []).append(s)
        primary   = kinds.get('a', [])
        secondary = kinds.get('b', []) + kinds.get('h', [])
        # A주 1개 + 부속 1~2개(B주·H주)만 자동 병합한다.
        if len(primary) != 1 or not secondary or len(secondary) > 2 \
                or len(primary) + len(secondary) != len(syms):
            skipped.append((dom, sorted(syms)))
            continue
        a = primary[0]
        for s in sorted(secondary):
            if (a.split('.')[0], s.split('.')[0]) in NEVER_MERGE:
                print(f'  [중복 예외] {dom}: NEVER_MERGE 지정 — {a} / {s} 둘 다 유지')
                continue
            drop.add(s)
            merged.append((a, s, dom))
    return [s for s in stocks if s[0] not in drop], merged, skipped


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
print(f'  유니버스(중복제거 전): {len(stocks)}종목')
if not stocks:
    raise SystemExit(f'[{MARKET.upper()}] 유니버스가 비었습니다. 스크리너 응답 확인 필요.')

# ── A+H 이중상장 중복 제거 (본토 A주 있으면 홍콩 H주 제거) ─────────────────────
present = {s[0].split('.')[0] for s in stocks}
drop_hk = {h for a, h in DUAL_AH if a in present and h in present}
if drop_hk:
    before = len(stocks)
    stocks = [s for s in stocks if s[0].split('.')[0] not in drop_hk]
    print(f'  A+H 중복 제거(표): 홍콩 {before - len(stocks)}종목 제외 → {len(stocks)}종목')

# ── A+H 자동 감지 (회사 website 기준) ──────────────────────────────────────────
print(f'\n[{MARKET.upper()}] A+H 자동 감지 (회사 website 기준)...')
_syms = [s[0] for s in stocks]
identity = load_identity(_syms)
print(f'  identity 캐시 적중 {len(identity)}/{len(_syms)}종목')
_todo = [s for s in _syms if s not in identity]
if _todo:
    print(f'  캐시 없는 {len(_todo)}종목 조회 중 (동시 {IDENTITY_WORKERS})...')
    _fresh = fetch_identity(_todo)
    identity.update(_fresh)
    save_identity(_fresh)
    print(f'  조회 완료 {len(_fresh)}/{len(_todo)}종목 '
          f'({time.time() - t0:.0f}s 경과)')
    if len(_fresh) < len(_todo):
        print(f'  [NOTE] {len(_todo) - len(_fresh)}종목 조회 실패 — '
              f'다음 실행에서 다시 시도합니다(그 사이 중복이 남을 수 있음).')

_KIND_LABEL = {'b': 'B주', 'h': 'H주'}
_before = len(stocks)
stocks, _merged, _skipped = dedupe_by_identity(stocks, identity)
for a, s, dom in _merged:
    print(f'  [중복 자동] {s}({_KIND_LABEL.get(listing_kind(s), "?")}) 제거 '
          f'← {a} 와 동일 회사 ({dom})')
for dom, syms in _skipped:
    print(f'  [중복 보류] {dom}: {len(syms)}종목이 묶여 판단 보류 — {", ".join(syms)}')
print(f'  자동 제거 {_before - len(stocks)}종목 → {len(stocks)}종목')

# ── 신규상장 시드 주입 ─────────────────────────────────────────────────────────
# 시총은 종가를 받은 뒤 계산하므로 여기서는 0 으로 넣어두고 심볼만 확보한다.
seed_shares = {}
_have = {s[0] for s in stocks}
for sym, name, cur, shares, note in SEED_NEW_LISTINGS:
    if sym in _have:
        print(f'  [시드 은퇴 가능] {sym}: 스크리너가 이제 잡습니다 '
              f'— SEED_NEW_LISTINGS 에서 지워도 됩니다.')
        continue
    seed_shares[sym] = shares
    stocks.append((sym, name, 0, cur))
    print(f'  [시드] {sym} {name} 추가 ({shares:,}주) — {note}')

symbols = [s[0] for s in stocks]

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

available = set(close_prices.columns)
all_stocks_full = [(sym, name, mc, cur) for sym, name, mc, cur in stocks if sym in available]
print(f'  가격 데이터 있음: {len(all_stocks_full)}종목')
for sym in seed_shares:
    if sym not in available:
        print(f'  [WARN] 시드 {sym}: 가격 컬럼이 없습니다 — 심볼이 맞는지 확인하세요.')

tickers_filtered = [s[0] for s in all_stocks_full]
coverage  = close_prices[tickers_filtered].notna().sum(axis=1)
threshold = len(tickers_filtered) * 0.8
valid_idx = coverage[coverage >= threshold].index
valid_dates = sorted([d.strftime('%Y-%m-%d') for d in valid_idx], reverse=True)[:HISTORY_DAYS]
print(f'  유효 날짜: {len(valid_dates)}일 ({valid_dates[-1]} ~ {valid_dates[0]})')

# ── 시드 종목 시총 계산 ────────────────────────────────────────────────────────
# 상장주식수 x 기준일 종가. 스크리너가 준 시총과 같은 단위(현지통화 절대금액)다.
# 시총 정렬은 이 뒤에서 하므로, 시드 종목도 제 순위에 들어간다.
if seed_shares:
    latest = valid_dates[0]
    rebuilt = []
    for sym, name, mc, cur in all_stocks_full:
        shares = seed_shares.get(sym)
        if shares:
            px = None
            try:
                v = close_prices.loc[latest, sym]
                px = float(v) if pd.notna(v) else None
            except Exception:
                px = None
            if px and px > 0:
                mc = int(shares * px)
                print(f'  [시드 시총] {sym}: {shares:,}주 x {px:,.2f} {cur} '
                      f'= {mc / 1e8:,.0f}억{cur}')
            else:
                print(f'  [WARN] 시드 {sym}: {latest} 종가가 없어 시총을 계산하지 못했습니다.')
        rebuilt.append((sym, name, mc, cur))
    all_stocks_full = rebuilt

# 시총 내림차순 정렬 후 상위 TOP_N 만 남긴다.
# 스크리너 결과는 이미 시총순이지만 시드가 끼어들었으므로 다시 세운다.
# prices 행렬은 아래에서 이 순서대로 만들기 때문에 정렬은 반드시 여기서 끝나야 한다.
all_stocks_full.sort(key=lambda s: s[2] or 0, reverse=True)
if len(all_stocks_full) > TOP_N:
    print(f'  시총 상위 {TOP_N}종목으로 절단 ({len(all_stocks_full)}종목 중)')
    all_stocks_full = all_stocks_full[:TOP_N]
print('  시총 상위 5: ' + ', '.join(
    f'{n}({s} {(m or 0) / 1e8:,.0f}억{c})' for s, n, m, c in all_stocks_full[:5]))

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

print(f'\n[{MARKET.upper()}] 지수 수집 중...')
indices = {}
for sym, name, key in INDEX_DEFS:
    try:
        # period='5d'는 Yahoo의 중국 지수 데이터 공백에 취약하다. 넉넉히 받아
        # 유효한 종가 바 2개를 쓰고, 두 바의 간격이 비정상이면 게시하지 않는다.
        hist = yf.Ticker(sym).history(period=IDX_PERIOD)
        hist = hist[hist['Close'].notna() & (hist['Close'] > 0)]
        if len(hist) >= 2:
            curr = float(hist['Close'].iloc[-1])
            prev = float(hist['Close'].iloc[-2])
            gap  = (hist.index[-1] - hist.index[-2]).days
            chg = curr - prev
            pct = chg / prev * 100
            if gap > IDX_GAP_MAX_DAYS:
                print(f'  [WARN] {name}({sym}): 최근 두 종가 간격 {gap}일 '
                      f'({hist.index[-2].date()} → {hist.index[-1].date()}) — '
                      f"'전일 대비'가 아니므로 제외")
                continue
            if gap > IDX_GAP_WARN_DAYS:
                print(f'  [NOTE] {name}({sym}): 간격 {gap}일 (연휴로 보임)')
            indices[key] = {'name': name, 'value': round(curr, 2),
                            'change': round(chg, 2), 'changePct': round(pct, 4)}
            print(f'  {name}: {curr:,.2f} ({chg:+.2f}, {pct:+.2f}%)')
        else:
            print(f'  [WARN] {name}({sym}): 유효 종가 {len(hist)}행 — 데이터 부족')
    except Exception as e:
        print(f'  [WARN] {sym}: {e}')

if not indices:
    print('  [WARN] 수집된 지수가 없습니다 — 지수 카드가 비게 됩니다.')

print(f'\n[{MARKET.upper()}] Firebase 업로드 중...')
stocks_data = [{'c': sym, 'n': name, 'm': int(mc), 'cur': cur} for sym, name, mc, cur in all_stocks_full]
collected_at = datetime.now(KST).strftime('%Y-%m-%d %H:%M')   # KST 는 상단에 정의

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
