# test_asia.py  ─  일본·중국 종목 유니버스 수집 가능성 테스트 (읽기 전용, Firebase 미사용)
# GitHub Actions에서 수동 실행 → 로그로 결과 확인용. 아직 실제 서비스에 연결하지 않음.

import io, sys, time, traceback
import requests
import pandas as pd

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def hr(title):
    print('\n' + '=' * 70)
    print(f'■ {title}')
    print('=' * 70)


def safe(fn):
    try:
        fn()
    except Exception as e:
        print(f'  [FAIL] {e}')
        traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# 1) 일본 — JPX 공식 상장종목 리스트 (전 종목 코드 + 이름 + 시장구분/업종)
# ─────────────────────────────────────────────────────────────────────────────
def test_jp_jpx():
    url = 'https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls'
    r = requests.get(url, headers=UA, timeout=60)
    print(f'  HTTP {r.status_code}, {len(r.content):,} bytes')
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content))
    print(f'  컬럼: {list(df.columns)}')
    print(f'  총 행수: {len(df):,}')
    # 보통 컬럼: 日付 / コード / 銘柄名 / 市場・商品区分 / 33業種コード ...
    code_col = next((c for c in df.columns if 'コード' in str(c) or 'code' in str(c).lower()), df.columns[1])
    name_col = next((c for c in df.columns if '銘柄名' in str(c) or 'name' in str(c).lower()), df.columns[2])
    print(f'  코드컬럼="{code_col}"  이름컬럼="{name_col}"')
    print('  --- 샘플 10개 ---')
    for _, row in df.head(10).iterrows():
        print(f'    {row[code_col]}  {row[name_col]}')
    # 시장구분 분포
    mkt_col = next((c for c in df.columns if '市場' in str(c)), None)
    if mkt_col:
        print('  --- 시장구분 분포 ---')
        print(df[mkt_col].value_counts().to_string())


# ─────────────────────────────────────────────────────────────────────────────
# 2) 일본 — yfinance로 가격/시총/이름 조회 되는지 (샘플)
# ─────────────────────────────────────────────────────────────────────────────
def test_jp_yfinance():
    import yfinance as yf
    for t in ['7203.T', '6758.T', '9984.T', '8306.T']:  # 도요타/소니/소프트뱅크/미쓰비시UFJ
        try:
            fi = yf.Ticker(t).fast_info
            mc = fi.market_cap
            px = fi.last_price
            cur = getattr(fi, 'currency', '?')
            print(f'    {t}: price={px} mktcap={mc:,} ({cur})' if mc else f'    {t}: price={px} mktcap=None')
        except Exception as e:
            print(f'    {t}: FAIL {e}')


# ─────────────────────────────────────────────────────────────────────────────
# 3) 중국 — akshare로 A주 전체 목록(코드+이름+시총) 되는지
# ─────────────────────────────────────────────────────────────────────────────
def test_cn_akshare():
    try:
        import akshare as ak
    except ImportError:
        print('  akshare 미설치 (requirements에 추가 필요)')
        return
    # 상하이+선전 실시간 스냅샷 (코드/이름/시총 포함)
    df = ak.stock_zh_a_spot_em()
    print(f'  컬럼: {list(df.columns)}')
    print(f'  총 종목수: {len(df):,}')
    print('  --- 샘플 10개 ---')
    show = [c for c in df.columns if c in ('代码', '名称', '总市值', '最新价')]
    print(df[show].head(10).to_string() if show else df.head(10).to_string())


# ─────────────────────────────────────────────────────────────────────────────
# 4) 중국 — yfinance로 개별 종목 조회 되는지 (본토 .SS/.SZ, 홍콩 .HK)
# ─────────────────────────────────────────────────────────────────────────────
def test_cn_yfinance():
    import yfinance as yf
    for t in ['600519.SS', '601398.SS', '000858.SZ', '300750.SZ', '0700.HK']:
        try:
            fi = yf.Ticker(t).fast_info
            mc = fi.market_cap
            px = fi.last_price
            cur = getattr(fi, 'currency', '?')
            print(f'    {t}: price={px} mktcap={mc:,} ({cur})' if mc else f'    {t}: price={px} mktcap=None')
        except Exception as e:
            print(f'    {t}: FAIL {e}')


if __name__ == '__main__':
    t0 = time.time()
    hr('1. 일본 — JPX 공식 상장종목 리스트 (코드+이름)')
    safe(test_jp_jpx)
    hr('2. 일본 — yfinance 가격/시총 샘플')
    safe(test_jp_yfinance)
    hr('3. 중국 — akshare A주 전체목록 (코드+이름+시총)')
    safe(test_cn_akshare)
    hr('4. 중국 — yfinance 개별종목 샘플')
    safe(test_cn_yfinance)
    print(f'\n완료 ({time.time()-t0:.0f}초)')
