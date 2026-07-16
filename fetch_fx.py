# fetch_fx.py  ─  환율 수집 → Firebase /v1/fx (대시보드 시총 통화 환산용)
# toKRW[통화] = 해당 통화 1단위의 원화 가치

import warnings, json, os
import yfinance as yf
from datetime import datetime, timezone, timedelta
import firebase_admin
from firebase_admin import credentials, db as firebase_db

warnings.filterwarnings('ignore')

cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
try:
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'
    })
except ValueError:
    pass

PAIRS = {'USD': 'USDKRW=X', 'JPY': 'JPYKRW=X', 'CNY': 'CNYKRW=X', 'HKD': 'HKDKRW=X'}
to_krw = {'KRW': 1.0}

print('[FX] 환율 수집 중...')
for ccy, sym in PAIRS.items():
    try:
        hist = yf.Ticker(sym).history(period='5d')
        if len(hist):
            to_krw[ccy] = round(float(hist['Close'].iloc[-1]), 4)
            print(f'  1 {ccy} = {to_krw[ccy]} KRW')
        else:
            print(f'  [WARN] {sym}: 데이터 없음')
    except Exception as e:
        print(f'  [WARN] {sym}: {e}')

KST = timezone(timedelta(hours=9))
firebase_db.reference('/v1/fx').set({
    'updated': datetime.now(KST).strftime('%Y-%m-%d %H:%M'),
    'toKRW': to_krw,
})
print(f'[FX] 완료! {to_krw}')
