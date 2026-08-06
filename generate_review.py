# generate_review.py  ─  GitHub Actions에서 실행
# Firebase(/v1/{kr,us,jp,cn}) → 상승/하락 TOP10 계산
#   1단계: 종목별 개별 리서치 (Claude + 웹검색, 병렬, 구조화 출력)
#   2단계: 총평 생성 (짧은 호출, 검색 없음)
#   3단계: HTML 리포트 렌더링 (파이썬 템플릿 — 모델이 HTML을 쓰지 않음)
# → Firebase(/reviews/{market}, /reviews_history/{market}/{date})에 게시
#
# 설계 노트:
#   기존 구조는 단일 호출이 "20종목 웹리서치"와 "A4 1장 HTML 작성"을 동시에 했다.
#   레이아웃 지시가 프롬프트의 대부분을 차지하고 A4 1장 제약이 등락배경을 100자
#   수준으로 압축시켜, 리서치 품질이 결과물에 반영되지 못했다.
#   → 리서치와 렌더링을 분리해 각 종목이 토큰 예산 전부를 쓰도록 하고,
#     HTML은 결정론적 템플릿으로 찍는다.
#     레이아웃은 A4 폭 고정 2섹션(개요+상승 / 하락+출처)이고, 등락 배경 분량에 따라
#     인쇄 시 2~4장으로 자연 분할된다(표 헤더 반복, 행 미분할).
#
# 필요 환경변수(GitHub Secrets):
#   FIREBASE_KEY        : Firebase 서비스 계정 JSON (기존 fetch 스크립트와 동일)
#   ANTHROPIC_API_KEY   : Claude API 키
#
# 사용법: python generate_review.py --market kr    (또는 us / jp / cn)
#         python generate_review.py --self-test    (오프라인 렌더링 검증)

import os, sys, re, json, html, argparse, random, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import anthropic

# Windows 콘솔(cp949)에서 한글·기호 출력이 깨지지 않도록
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8')
    except Exception:
        pass

DATABASE_URL = 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'
CLAUDE_MODEL = 'claude-opus-5'
KST = timezone(timedelta(hours=9))

RESEARCH_WORKERS  = 6      # 종목 리서치 동시 실행 수
SEARCH_MAX_USES   = 6      # 종목당 웹검색 최대 횟수
PROFILE_TTL_DAYS  = 180    # 회사 소개 캐시 유효기간

# ── 토큰 예산 ──────────────────────────────────────────────────────────────────
# max_tokens 는 '응답 텍스트'만이 아니라 adaptive thinking 까지 합친 상한이다.
# 예전 값 8000 은 웹검색 6~8회 + thinking 이 들어가면 그대로 소진돼 JSON 이
# 중간에서 잘렸고, 재시도 3번이 모두 같은 8000 을 써서 세 번 다 잘렸다.
# 실측(2026-08-06 게시본): 중국 18/20, 국내 6/20, 일본 5/20 종목이 이걸로 실패.
# 스트리밍으로 호출하므로 값을 키워도 HTTP 타임아웃 위험은 없다.
RESEARCH_MAX_TOKENS = 32000
RESEARCH_RETRIES    = 3    # 일시적 오류(429/5xx/네트워크) 재시도 횟수
PAUSE_RESUME_MAX    = 8    # 웹검색 pause_turn 재개 한도

# ── 분량 (1페이지 리포트) ──────────────────────────────────────────────────────
# 회사 소개와 등락 배경을 표의 같은 칸에 이어 쓴다. 두 열로 나누면 둘 중 긴 쪽이
# 행 높이를 정해 짧은 쪽 자리가 통째로 버려지는데, 한 칸에 흘려 쓰면 그 낭비가 없다.
#
# 캡은 브라우저 실측으로 잡았다(2026-08-06). 합친 칸 내부폭 473px, 6.9pt,
# 한글 한 글자 8.46px → 한 줄 55자, 두 줄 110자. 3줄이 되면 20행 x 13px = 260px
# 가 늘어 A4 한 장을 넘긴다(스트레스 테스트에서 실제로 초과했다).
#
# ★ 이 칸에 들어가는 건 소개+배경만이 아니다. 아래를 모두 더해야 한다:
#     회사 소개        PROFILE_CLIP
#     공백             1
#     '추정 ' 접두어    약 3.1  (신뢰도 low 인 행만)
#     등락 배경        REASON_CLIP
#   처음엔 '추정 ' 접두어와 (당시 있던) 각주 첨자를 빼먹고 40+60 이면 된다고
#   봤는데 합계가 109자가 되어 3줄로 넘어갔다. 출처를 없앤 뒤 첨자 몫이
#   빠져서: 40+1+3.1+62 = 106.1자 → 110자 예산 안에 4자 여유.
PROFILE_MIN_CHARS = 26     # 회사 소개 목표 하한(프롬프트용)
PROFILE_MAX_CHARS = 36     # 회사 소개 목표 상한(프롬프트용)
REASON_MIN_CHARS  = 44     # 등락 배경 목표 하한(프롬프트용)
REASON_MAX_CHARS  = 58     # 등락 배경 목표 상한(프롬프트용)
# 렌더 단계 하드 캡 — 프롬프트 한도를 모델이 넘겨도 2줄이 유지되게 한다.
# 프롬프트 한도보다 여유를 두었으므로 평소에는 걸리지 않는다.
PROFILE_CLIP = 40
REASON_CLIP  = 62
NAME_CLIP    = 40          # 종목명 열 하드 캡 (아래 NAME_NOISE 로 꼬리표를 떼고 나서)

# 시황 총평 줄 수. 개별 종목 배경보다 시장 전체 흐름을 두껍게 싣는다.
# 출처 목록(약 90px)을 뺀 자리를 여기에 돌렸다.
OVERVIEW_LINES     = 6
OVERVIEW_MIN_CHARS = 55    # 총평 한 줄 목표 하한
OVERVIEW_MAX_CHARS = 95    # 총평 한 줄 목표 상한 (7pt 전폭에서 약 1.4줄)
OVERVIEW_CLIP      = 110   # 총평 한 줄 하드 캡 (렌더에서 두 줄까지 허용)
# 총평은 매크로 원인('왜 금이 올랐나')을 확인해야 하므로 웹검색을 준다.
OVERVIEW_SEARCH_MAX_USES = 5

# 위 회계를 코드로 못박는다. 캡을 올리다 예산을 넘기면 import 단계에서 바로 터진다
# — 조용히 3줄이 되어 A4 두 장으로 새는 것보다 낫다.
MERGED_LINE_CHARS  = 55    # 실측: 합친 칸 내부폭 473px / 한글 8.46px
MERGED_MAX_LINES   = 2
_MERGED_OVERHEAD   = 1 + 3.1              # 공백 + '추정 ' (각주 첨자는 없앴다)
_MERGED_BUDGET     = MERGED_LINE_CHARS * MERGED_MAX_LINES
_MERGED_USED       = PROFILE_CLIP + REASON_CLIP + _MERGED_OVERHEAD
assert _MERGED_USED <= _MERGED_BUDGET, (
    f'합친 칸 예산 초과: {_MERGED_USED:.1f}자 > {_MERGED_BUDGET}자. '
    f'PROFILE_CLIP({PROFILE_CLIP}) 또는 REASON_CLIP({REASON_CLIP}) 를 줄이거나, '
    f'render_table() 의 열 폭을 넓히고 MERGED_LINE_CHARS 를 실측으로 다시 잡아라.')

# 미국 종목명은 Nasdaq 원문이라 주식 종류 꼬리표가 길게 붙는다.
# 표에서는 정보가 없는데 종목명 열을 4~5줄로 밀어 행 높이를 지배해 버린다.
#   실측(2026-08-06 미국편): 'Space Exploration Technologies Corp. Class A Common
#   Stock' 57자 → 행 62px. 합친 칸은 2줄(32px)이면 되는데도 행이 4~5줄이 됐고,
#   그렇게 12행이 겹쳐 A4 한 장을 147px 넘겼다.
# 꼬리표를 떼면 'Space Exploration Technologies Corp.' 이 남는다.
# '(NEW)' 는 이름 중간에도 오므로 \b 로는 잡히지 않는다(괄호 앞뒤가 모두 비단어라
# 단어 경계가 성립하지 않음). 별도 대안으로 두고 위치와 무관하게 지운다.
NAME_NOISE = re.compile(
    r'\s*\((?:NEW|OLD)\)'
    r'|\s*\b(?:'
    r'Class\s+[A-Z]\b.*'
    r'|(?:American|Global)\s+Depositary\s+(?:Shares?|Receipts?).*'
    r'|(?:Common|Ordinary|Registered|Subordinate\s+Voting)\s+(?:Stock|Shares?).*'
    r'|Depositary\s+Shares?.*'
    r')$', re.I)

# 리서치 성공률이 이 아래면 게시하지 않고 실패로 끝낸다.
# 반쪽짜리 리포트를 올려두면 멱등 가드가 그날의 재시도를 막아버려서,
# 빈칸이 가득한 리포트가 하루 종일 그대로 남는다(실제로 그랬다).
# 게시를 안 하면 30분 뒤 백업 cron 이 같은 기준일로 다시 시도한다.
PUBLISH_MIN_OK_RATIO = 0.7

# HTML의 시장별 지수 순서와 동일
IDX_ORDER = {
    'kr': ['kospi', 'kospi200', 'kosdaq'],
    'us': ['sp500', 'ndx100', 'dji30'],
    'jp': ['n225', 'topix'],
    # cn: 2026-07-28부터 상하이종합(sse)으로 교체. csi300은 그 이전 기록을
    #     계속 렌더링하기 위해 남겨둔다(없는 키는 필터링됨).
    'cn': ['sse', 'csi300', 'hsi'],
}
MARKET_NAME = {'kr': '한국', 'us': '미국', 'jp': '일본', 'cn': '중국'}
# 초과수익(alpha) 계산 기준 지수 (없으면 index_context가 사용 가능한 첫 지수로 대체)
BENCH_IDX   = {'kr': 'kospi', 'us': 'sp500', 'jp': 'n225', 'cn': 'sse'}
# 검색에 사용할 언어 — 현지어로 검색해야 개별 종목 뉴스가 잡힌다
SEARCH_LANG = {'kr': '한국어', 'us': '영어', 'jp': '일본어', 'cn': '중국어(간체)'}
DEFAULT_CUR = {'kr': 'KRW', 'us': 'USD', 'jp': 'JPY', 'cn': 'CNY'}

# 등락 사유 분류 — 리포트에는 표기하지 않는다(딱지 제거).
# 미확인 종목 집계와 총평 프롬프트 입력에만 쓰이는 내부 신호.
CATALYST_ENUM = [
    '실적', '공시', '수주·계약', '정책·규제',
    '섹터·테마', '지수·매크로', '수급', '확인된_뉴스_없음',
]

# 등락 배경 구조화 출력 스키마
#   sector / has_individual_issue / sector_issue 는 섹터 중복 축약에 쓰인다.
#   리서치 호출은 종목별로 독립·병렬이라 모델은 다른 종목이 뭘 찾았는지 모른다.
#   따라서 "섹터 이슈가 겹치는지"는 모델이 판단할 수 없고,
#   각 종목의 sector/sector_issue를 받아 collapse_sector_duplicates()에서
#   결정론적으로 묶어 축약한다.
RESEARCH_SCHEMA = {
    "type": "object",
    "properties": {
        "reason":               {"type": "string"},
        "catalyst_type":        {"type": "string", "enum": CATALYST_ENUM},
        "sector":               {"type": "string"},
        "has_individual_issue": {"type": "boolean"},
        "sector_issue":         {"type": "string"},
        "source_url":           {"type": "string"},
        "source_date":          {"type": "string"},
        "confidence":           {"type": "string", "enum": ["high", "medium", "low"]},
        "profile":              {"type": "string"},
    },
    "required": ["reason", "catalyst_type", "sector", "has_individual_issue",
                 "sector_issue", "source_url", "source_date", "confidence", "profile"],
    "additionalProperties": False,
}


# ── Firebase 초기화 ────────────────────────────────────────────────────────────
def init_firebase():
    import firebase_admin
    from firebase_admin import credentials
    cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
    try:
        firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
    except ValueError:
        pass  # 이미 초기화됨


def fb_ref(path):
    from firebase_admin import db as firebase_db
    return firebase_db.reference(path)


def safe_key(code):
    """Firebase 키에 쓸 수 없는 문자 치환 (005930.KS, BRK.B, 0700.HK 등)"""
    return re.sub(r'[.#$\[\]/]', '_', str(code))


# ── 계산 헬퍼 ──────────────────────────────────────────────────────────────────
def calc_ret(prices, si, off):
    """PRICES[0][si] / PRICES[off][si] - 1"""
    if len(prices) <= off:
        return None
    p1 = prices[0][si] if si < len(prices[0]) else 0
    p0 = prices[off][si] if si < len(prices[off]) else 0
    if not p1 or not p0:
        return None
    return p1 / p0 - 1


def get_top_bottom(stocks, prices, market, n=10):
    """상승/하락 상위 종목. 실제로 오른/내린 종목만 담는다.

    상승 종목이 n개 미만이면 있는 만큼만, 하락도 마찬가지.
    보합(0%)은 어느 쪽에도 넣지 않는다. 두 리스트는 부호로 갈리므로
    유효 종목이 2n개 미만이어도 겹치지 않는다.
    """
    default_cur = DEFAULT_CUR.get(market, '')
    lst = []
    for i, s in enumerate(stocks):
        sr = calc_ret(prices, i, 1)
        if sr is None:
            continue
        lst.append({
            'code': s.get('c', ''),
            'name': s.get('n', ''),
            'ret':  sr,
            'ret5': calc_ret(prices, i, 5),
            'mcap': s.get('m') or 0,
            'cur':  s.get('cur') or default_cur,
        })
    gainers = sorted([s for s in lst if s['ret'] > 0], key=lambda x: x['ret'], reverse=True)
    losers  = sorted([s for s in lst if s['ret'] < 0], key=lambda x: x['ret'])
    return gainers[:n], losers[:n]


def fmt_ret(ret):
    if ret is None:
        return '?'
    sign = '+' if ret >= 0 else ''
    return f'{sign}{ret * 100:.1f}%'


def fmt_idx_val(v):
    if v is None:
        return '—'
    if v >= 1000:
        return f'{v:,.2f}'
    return f'{v:.2f}'


def fmt_mcap(m, cur):
    """통화별 시가총액 축약 표기"""
    if not m:
        return '시총 미상'
    if cur == 'KRW':
        return f'시총 {m / 1e12:.1f}조원' if m >= 1e12 else f'시총 {m / 1e8:,.0f}억원'
    if cur == 'JPY':
        return f'시총 {m / 1e12:.1f}조엔' if m >= 1e12 else f'시총 {m / 1e8:,.0f}억엔'
    if cur == 'CNY':
        return f'시총 {m / 1e8:,.0f}억위안'
    if cur == 'HKD':
        return f'시총 {m / 1e8:,.0f}억HKD'
    return f'시총 ${m / 1e9:,.1f}B'


def get_idx_for_date(indices, date):
    """날짜별 지수 히스토리에서 date 이하 가장 가까운 날짜의 데이터 반환"""
    if not indices:
        return None
    date_re = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    avail = sorted([d for d in indices.keys() if date_re.match(d)], reverse=True)
    if not avail:
        return None
    use = next((d for d in avail if d <= date), avail[0])
    return indices.get(use)


def index_context(market, idx_data):
    """프롬프트에 넣을 지수 요약 문자열 + 벤치마크 등락률(%)"""
    if not idx_data:
        return '정보 없음', None
    parts, bench = [], None
    for k in IDX_ORDER.get(market, []):
        idx = idx_data.get(k)
        if not idx:
            continue
        pct = idx.get('changePct')
        if pct is None:
            continue
        parts.append(f"{idx.get('name', k)} {pct:+.2f}%")
        if k == BENCH_IDX.get(market) and bench is None:
            bench = pct
    if bench is None:
        for k in IDX_ORDER.get(market, []):
            idx = idx_data.get(k)
            if idx and idx.get('changePct') is not None:
                bench = idx['changePct']
                break
    return (', '.join(parts) if parts else '정보 없음'), bench


def enrich(stocks, bench_pct):
    """지수 대비 초과수익 등 리서치에 필요한 파생 지표 추가"""
    for s in stocks:
        s['ret_pct'] = s['ret'] * 100
        s['excess'] = (s['ret_pct'] - bench_pct) if bench_pct is not None else None
        s['mcap_str'] = fmt_mcap(s['mcap'], s['cur'])
    return stocks


# ── 1단계: 종목별 리서치 ────────────────────────────────────────────────────────
def build_research_prompt(market, date, stock, idx_ctx, need_profile):
    market_name = MARKET_NAME.get(market, market)
    lang = SEARCH_LANG.get(market, '현지어')

    excess_line = (
        f"지수 대비 초과수익 {stock['excess']:+.1f}%p"
        if stock.get('excess') is not None else "지수 대비 초과수익 계산 불가"
    )
    ret5_line = (
        f"최근 5거래일 누적 {stock['ret5'] * 100:+.1f}%"
        if stock.get('ret5') is not None else "5거래일 누적 데이터 없음"
    )
    profile_rule = (
        f"- profile: 이 회사의 핵심 사업을 {PROFILE_MIN_CHARS}~{PROFILE_MAX_CHARS}자로.\n"
        "  ★ 반드시 명사형으로 끝내라. 서술어('~한다', '~이다', '~하는 기업')를 쓰지 마라.\n"
        "  마침표도 찍지 마라. 무엇을 만들어 누구에게 파는지만 남긴다.\n"
        "  설립연도·지역·계열 관계·수사는 넣지 마라.\n"
        "  좋은 예: \"온라인 쇼핑몰 구축·결제 플랫폼을 전세계 판매자에 제공\"\n"
        "           \"메모리 반도체를 설계·생산해 글로벌 IT 제조사에 공급\"\n"
        "           \"금·은 광산을 운영해 정광과 지금 판매\"\n"
        "  나쁜 예: \"온라인 쇼핑몰 구축과 결제 플랫폼을 전세계 판매자에 제공한다.\"\n"
        "           \"세계적인 전자상거래 솔루션을 제공하는 글로벌 기업이다.\"\n"
        f"  ※ 리포트에서 이 구절 바로 뒤에 reason 이 이어 붙는다. "
        f"{PROFILE_MAX_CHARS}자를 넘기면 잘린다."
        if need_profile else
        '- profile: 빈 문자열("")로 두어라. 이미 확보돼 있다.'
    )

    return f"""{date} {market_name} 증시에서 {stock['name']}({stock['code']}) 주가가 {fmt_ret(stock['ret'])} 움직였다.
이 종목이 왜 그렇게 움직였는지 웹 검색으로 확인하라.

━━━ 종목 정보 (판단용 참고자료) ━━━
※ 아래 수치는 네가 개별 재료인지 섹터/매크로 요인인지 판단하는 데만 쓴다.
   리포트 표에 이미 등락률 컬럼이 있으므로, 코멘트에 이 수치들을 다시 쓰지 마라.
종목명/코드 : {stock['name']} / {stock['code']}
당일 등락률  : {fmt_ret(stock['ret'])}
시장 지수    : {idx_ctx}
{excess_line}
{ret5_line}
{stock['mcap_str']}

━━━ 검색 순서 (이 순서를 지켜라) ━━━
반드시 {lang}로 검색하라. 영어로 검색하면 현지 종목 뉴스가 잡히지 않는다.

1단계 — 개별 종목 재료를 먼저 찾는다
  "{stock['name']} 주가", "{stock['name']} 공시", "{stock['name']} {date}", "{stock['code']} 뉴스"
  → 이 종목만의 실적·공시·수주·경영 이슈가 있는지 확인한다.

2단계 — 1단계에서 뚜렷한 개별 재료가 안 나오면, 반드시 섹터/업종 이슈를 찾는다
  이 종목이 속한 업종을 먼저 판단하고, 업종 단위로 검색한다.
  "{{업종명}} 업종 주가 {date}", "{{업종명}} 관련주 급등락", "{{업종명}} 정책"
  → 같은 업종 종목이 함께 움직였는지, 그 원인이 무엇인지 확인한다.
  개별 재료가 없을 때 섹터 이슈를 건너뛰고 바로 "수급/미확인"으로 처리하지 마라.

3단계 — 섹터 이슈도 없으면 지수·매크로 요인을 본다 (금리, 환율, 외국인 수급 등)

4단계 — 위 어디에도 해당하지 않으면 "확인된_뉴스_없음"으로 분류한다

━━━ 작성 규칙 (엄수) ━━━
1. {date} 기준 ±3일 이내에 실제로 보도·공시된 내용만 근거로 삼아라.
2. ★★ reason에 주가 등락 수치를 다시 쓰지 마라. 표에 이미 있어서 중복이다.
   금지 표현 예: "최근 5거래일 8% 상승", "지수 대비 11%p 초과수익",
   "당일 12% 급등하며", "코스피가 0.8% 오른 가운데 이 종목은 …%"
   → 대신 원인만 서술하라. "왜 움직였는지"가 코멘트의 역할이다.
   ※ 단, 뉴스에서 확인한 숫자(영업이익 4.2조원, 계약 규모 1,200억원,
      목표주가 12% 상향 등)는 새 정보이므로 반드시 포함하라. 이건 금지 대상이 아니다.
3. ★ 근거를 찾지 못했으면 절대 지어내지 마라.
   catalyst_type을 "확인된_뉴스_없음"으로 하고, reason에는 관찰된 사실만 적어라.
   예: "개별 공시·뉴스 및 업종 이슈 모두 미확인. 수급 요인에 따른 변동으로 추정된다."
   근거 없는 그럴듯한 서술보다 "확인 안 됨"이 훨씬 가치 있다.
4. 지수가 이 종목과 같은 방향으로 크게 움직였고 초과수익이 작다면,
   개별 재료로 포장하지 말고 "지수·매크로"로 분류하라.
   (분류값은 내부 집계용이며 리포트에 표기되지 않는다. reason에도 쓰지 마라.)
5. ★ reason 분량 — 리포트가 A4 한 장이므로 분량이 곧 지면이다. 짧게 써라.
   • 개별 재료가 뚜렷하면(has_individual_issue=true): {REASON_MIN_CHARS}~{REASON_MAX_CHARS}자.
     숫자(실적 수치, 계약 규모, 목표주가 등)를 확인했다면 그 숫자를 우선 넣어라.
   • 섹터 이슈로 설명되는 경우(has_individual_issue=false): 22~36자로 더 짧게.
     섹터 공통 원인만 쓰고 개별 종목 서술을 늘리지 마라.
   {REASON_CLIP}자를 넘기면 뒤가 잘린다. 형용사·배경설명·전망은 버리고 원인 사실만 남겨라.
   한 문장이면 한 문장으로 끝내라. 도입부("~에 따르면", "시장에서는")를 쓰지 마라.
   좋은 예: "3분기 영업이익 4.2조원으로 컨센서스 18% 상회. HBM3E 엔비디아 퀄 통과."
   나쁜 예: "시장에서는 3분기 실적이 예상을 상회한 것으로 알려지면서 투자자들의
             관심이 집중되는 모습을 보였고, 이에 따라 매수세가 유입된 것으로 보인다."
6. sector — 이 종목의 업종을 짧은 명사로. 예: 반도체, 2차전지, 조선, 바이오, 방산, 증권, 자동차.
   다른 종목과 묶이도록 일반적으로 통용되는 업종명을 쓰고, 회사 고유 표현은 쓰지 마라.
7. has_individual_issue — 이 종목만의 뚜렷한 개별 재료(실적·공시·수주 등)가 확인되면 true,
   섹터/매크로/수급으로만 설명되면 false.
8. sector_issue — 오늘 이 업종을 움직인 공통 원인을 한 문장(20~32자)으로.
   업종 차원의 원인을 확인하지 못했으면 빈 문자열("").
   ※ 이 값은 같은 업종 종목이 여러 개일 때 코멘트를 한 줄로 묶는 데 쓰인다.
9. source_url은 실제로 검색 결과에서 확인한 기사·공시 URL. 없으면 빈 문자열("").
   source_date는 그 출처의 보도일(YYYY-MM-DD). 없으면 빈 문자열("").
10. confidence — high: 공시·IR 등 1차 출처 확인 / medium: 언론 보도 확인 / low: 추정
{profile_rule}

한국어로 작성하라."""


def _json_fallback_instruction():
    return (
        "\n\n━━━ 출력 형식 ━━━\n"
        "설명 없이 아래 키를 가진 JSON 객체 하나만 출력하라.\n"
        '{"reason": "...", "catalyst_type": "'
        + '|'.join(CATALYST_ENUM)
        + '", "sector": "...", "has_individual_issue": true|false, '
          '"sector_issue": "...", "source_url": "...", "source_date": "YYYY-MM-DD", '
          '"confidence": "high|medium|low", "profile": "..."}'
    )


def _extract_json(text):
    """구조화 출력이 적용되지 않은 경우까지 대비한 JSON 파서"""
    text = (text or '').strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r'```(?:json)?\s*([\s\S]*?)```', text, re.I)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except Exception:
            pass
    m = re.search(r'\{[\s\S]*\}', text)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return None


def _stream_final(client, **kwargs):
    """스트리밍 + pause_turn 재개 루프.

    웹검색은 서버측 루프에서 일시정지될 수 있다(stop_reason='pause_turn').
    문서가 정한 재개 방법대로 어시스턴트 응답을 그대로 붙여 다시 요청한다.
    재개 한도를 넘기면 stop_reason 이 'pause_turn' 인 응답이 그대로 돌아온다.
    호출자가 stop_reason 을 반드시 봐야 한다(예전 코드는 안 봐서, 끝나지 않은
    턴의 빈 텍스트를 'JSON 파싱 실패'로만 기록했다).
    """
    messages = list(kwargs.pop('messages'))
    final = None
    for _ in range(PAUSE_RESUME_MAX):
        with client.messages.stream(messages=messages, **kwargs) as stream:
            final = stream.get_final_message()
        if final.stop_reason != 'pause_turn':
            break
        messages = messages + [{'role': 'assistant', 'content': final.content}]
    return final


# 재시도할 가치가 있는 오류 — 429(rate limit), 5xx/529(overloaded), 네트워크.
# SDK 도 자체 재시도를 하지만(max_retries), 소진 후에는 여기서 백오프로 더 버틴다.
TRANSIENT_ERRORS = (
    anthropic.RateLimitError,
    anthropic.InternalServerError,
    anthropic.APIConnectionError,
    anthropic.APITimeoutError,
)


def _describe_stop(final, text):
    """실패 원인을 로그에 남길 수 있게 stop_reason 을 사람 말로 바꾼다."""
    sr = getattr(final, 'stop_reason', None)
    if sr == 'max_tokens':
        return (f'max_tokens({RESEARCH_MAX_TOKENS:,}) 소진 — 응답이 잘렸습니다 '
                f'(텍스트 {len(text)}자)')
    if sr == 'refusal':
        cat = getattr(getattr(final, 'stop_details', None), 'category', None)
        return f'모델이 응답을 거절했습니다 (category={cat})'
    if sr == 'pause_turn':
        return f'웹검색 재개 한도({PAUSE_RESUME_MAX}회) 초과 — 턴이 끝나지 않았습니다'
    return f'JSON 파싱 실패 (stop_reason={sr}, 텍스트 {len(text)}자)'


def _research_attempts(prompt):
    """토큰 압박을 단계적으로 낮추는 재시도 사다리.

    실패 원인 1순위는 예산 소진이었다(웹검색 결과 + thinking 이 응답 예산을
    같이 쓴다). 그래서 재시도마다 effort 와 검색 횟수를 줄여, 앞 단계가
    잘렸으면 뒤 단계는 더 적게 생각하고 더 적게 검색하도록 만든다.
    예전 사다리는 세 단계가 모두 같은 예산이라 한 번 잘리면 세 번 다 잘렸다.
    마지막 단계만 구조화 출력을 떼고 수동 JSON 파싱으로 내려간다.
    """
    def tools(n):
        return [{'type': 'web_search_20260209', 'name': 'web_search', 'max_uses': n}]

    schema_fmt = {'type': 'json_schema', 'schema': RESEARCH_SCHEMA}
    base = dict(model=CLAUDE_MODEL, max_tokens=RESEARCH_MAX_TOKENS,
                thinking={'type': 'adaptive'})
    return [
        dict(base, tools=tools(SEARCH_MAX_USES),
             output_config={'effort': 'medium', 'format': schema_fmt},
             messages=[{'role': 'user', 'content': prompt}]),
        dict(base, tools=tools(3),
             output_config={'effort': 'low', 'format': schema_fmt},
             messages=[{'role': 'user', 'content': prompt}]),
        dict(base, tools=tools(3),
             output_config={'effort': 'low'},
             messages=[{'role': 'user', 'content': prompt + _json_fallback_instruction()}]),
    ]


def call_research(client, prompt):
    """리서치 결과(JSON)를 받는다. 사다리 3단계 x 일시적 오류 재시도."""
    last_err = None
    for i, kwargs in enumerate(_research_attempts(prompt), 1):
        for retry in range(RESEARCH_RETRIES):
            try:
                final = _stream_final(client, **kwargs)
                text = ''.join(b.text for b in final.content if b.type == 'text')
                data = _extract_json(text)
                if data:
                    return data
                # 같은 요청을 그대로 다시 보내도 같은 결과다 → 다음 단계로.
                last_err = f'[{i}단계] ' + _describe_stop(final, text)
                break
            except anthropic.BadRequestError as e:
                last_err = f'[{i}단계] 400: {e}'
                break                       # 옵션 문제 — 옵션을 줄인 다음 단계로
            except TRANSIENT_ERRORS as e:
                last_err = f'[{i}단계] {type(e).__name__}: {e}'
                if retry < RESEARCH_RETRIES - 1:
                    wait = 2 ** retry + random.uniform(0, 1.5)
                    print(f'    [재시도] {type(e).__name__} — {wait:.1f}s 후 '
                          f'({retry + 2}/{RESEARCH_RETRIES})')
                    time.sleep(wait)
                    continue
                break
            except Exception as e:
                # 예전 코드는 여기서 break 로 남은 단계를 통째로 버렸다.
                last_err = f'[{i}단계] {type(e).__name__}: {e}'
                break
        print(f'    [단계 실패] {last_err}')
    raise RuntimeError(last_err or '리서치 실패')


def research_stock(client, market, date, stock, idx_ctx, cached_profile):
    need_profile = not cached_profile
    prompt = build_research_prompt(market, date, stock, idx_ctx, need_profile)
    try:
        data = call_research(client, prompt)
    except Exception as e:
        print(f'  [WARN] {stock["name"]}({stock["code"]}) 리서치 실패: {e}')
        # 표에 찍히는 문구는 짧고 담백하게 둔다. 사유 분류는 '확인된_뉴스_없음'
        # 이 아니라 research_failed 로 따로 센다 — 진짜로 뉴스가 없는 종목과
        # 호출이 실패한 종목을 섞으면 게시 판단이 흐려진다.
        return {**stock,
                'reason': '등락 배경을 확인하지 못했습니다.',
                'catalyst_type': '확인된_뉴스_없음',
                'sector': '', 'has_individual_issue': False, 'sector_issue': '',
                'source_url': '', 'source_date': '',
                'confidence': 'low',
                'profile': cached_profile or '',
                'profile_is_new': False,
                'research_failed': True}

    ctype = data.get('catalyst_type')
    if ctype not in CATALYST_ENUM:
        ctype = '확인된_뉴스_없음'
    profile = (data.get('profile') or '').strip()
    conf = data.get('confidence')
    out = {
        **stock,
        'reason':               (data.get('reason') or '').strip() or '내용 없음',
        'catalyst_type':        ctype,
        'sector':               (data.get('sector') or '').strip(),
        'has_individual_issue': bool(data.get('has_individual_issue')),
        'sector_issue':         (data.get('sector_issue') or '').strip(),
        'source_url':           (data.get('source_url') or '').strip(),
        'source_date':          (data.get('source_date') or '').strip(),
        'confidence':           conf if conf in ('high', 'medium', 'low') else 'low',
        'profile':              cached_profile or profile or '회사 정보 미확보',
        'profile_is_new':       bool(need_profile and profile),
        'research_failed':      False,
    }
    tag = '✓' if out['source_url'] else '·'
    mark = '개별' if out['has_individual_issue'] else '섹터/매크로'
    print(f'  {tag} {stock["name"]}({stock["code"]}) {fmt_ret(stock["ret"])} '
          f'[{ctype}/{out["confidence"]}/{mark}'
          + (f'/{out["sector"]}' if out['sector'] else '')
          + f'] {len(out["reason"])}자')
    return out


def collapse_sector_duplicates(rows):
    """개별 재료 없이 같은 섹터 이슈를 공유하는 종목들의 코멘트를 축약한다.

    리서치는 종목별 독립 호출이라 각 호출은 다른 종목 결과를 모른다.
    여기서 같은 방향(상승 표 / 하락 표) 안에서 sector가 겹치고
    has_individual_issue=False 인 종목이 2개 이상이면,
    첫 종목만 섹터 공통 이슈를 풀어 쓰고 나머지는 한 줄로 줄인다.
    → 중복 서술 제거 + 분량 컨트롤.

    반환: 축약된 종목 수
    """
    groups = {}
    for r in rows:
        if r.get('has_individual_issue'):
            continue                        # 개별 재료가 뚜렷하면 축약 대상 아님
        sector = (r.get('sector') or '').strip()
        issue  = (r.get('sector_issue') or '').strip()
        if not sector or not issue:
            continue                        # 섹터 공통 원인을 확인하지 못한 경우 원문 유지
        groups.setdefault(sector, []).append(r)

    collapsed = 0
    for sector, members in groups.items():
        if len(members) < 2:
            continue                        # 혼자면 축약할 이유가 없다
        lead = members[0]
        lead['reason'] = f"{sector} 섹터 공통 이슈 — {lead['sector_issue']}"
        lead['sector_role'] = 'lead'
        for m in members[1:]:
            m['reason'] = f"{sector} 섹터 동일 이슈 영향 ({clean_name(lead['name'])}과 동일)"
            m['sector_role'] = 'dup'
            m['source_url'] = ''            # 같은 출처를 각주로 중복 표기하지 않는다
            collapsed += 1
        print(f'  [섹터축약] {sector} {len(members)}종목 → 대표 1건 + 축약 {len(members) - 1}건')
    return collapsed


def _profile_is_current(p):
    """캐시된 회사 소개가 지금 규격(짧고, 명사형 종결)에 맞는지."""
    t = (p or '').strip()
    if not t or len(t) > PROFILE_CLIP:
        return False
    # 서술형 종결('~한다', '~이다', '~공급함') 과 마침표는 옛 형식이다.
    return not re.search(r'[다요음임]$|\.$', t)


def load_profiles(market, codes):
    """회사 소개 캐시 조회 — 매일 다시 쓰지 않고 재사용해 예산을 등락배경에 몰아준다"""
    try:
        raw = fb_ref(f'/profiles/{market}').get() or {}
    except Exception as e:
        print(f'  [WARN] 프로필 캐시 조회 실패: {e}')
        return {}
    today = datetime.now(KST).date()
    out = {}
    for code in codes:
        rec = raw.get(safe_key(code))
        if not isinstance(rec, dict) or not rec.get('p'):
            continue
        try:
            age = (today - datetime.strptime(rec.get('u', '1970-01-01'), '%Y-%m-%d').date()).days
        except Exception:
            age = 10 ** 6
        # 캐시에 남아 있는 옛 소개는 캐시 미스로 처리해 새 규격으로 다시 쓰게 한다.
        # 캐시 TTL 이 180일이라 그냥 두면 반년 동안 옛 형식이 표에 남는다.
        #   - 긴 것(60~90자): 1페이지 레이아웃이 짧은 소개를 전제로 한다.
        #   - 서술형('~한다.'): 이제 명사형으로 끝내기로 했다.
        if age <= PROFILE_TTL_DAYS and _profile_is_current(rec['p']):
            out[code] = rec['p']
    return out


def save_profiles(market, results):
    updates = {
        safe_key(r['code']): {'p': r['profile'], 'u': datetime.now(KST).strftime('%Y-%m-%d')}
        for r in results if r.get('profile_is_new') and r.get('profile')
    }
    if not updates:
        return 0
    try:
        fb_ref(f'/profiles/{market}').update(updates)
        return len(updates)
    except Exception as e:
        print(f'  [WARN] 프로필 캐시 저장 실패: {e}')
        return 0


# ── 2단계: 시황 총평 ───────────────────────────────────────────────────────────
# 개별 종목 배경은 표에서 두 줄로 끊기니, 시장 전체를 읽는 몫은 여기가 진다.
# 그래서 총평만은 웹검색을 허용한다 — 표에 있는 개별 재료를 합쳐도 '왜 금 현물이
# 급등했나' 같은 매크로 원인은 나오지 않기 때문이다. 검색은 그 원인을 확인하는
# 용도로만 쓰고, 없으면 없다고 쓰게 한다.
def build_overview(client, market, date, idx_ctx, top, bot):
    def brief(rows):
        if not rows:
            return '- (해당 종목 없음)'
        return '\n'.join(
            f"- {r['name']} {fmt_ret(r['ret'])} [{r['catalyst_type']}"
            + (f"/{r['sector']}" if r.get('sector') else '')
            + ('/개별' if r.get('has_individual_issue') else '/섹터·매크로')
            + f"] {r['reason'][:110]}"
            for r in rows)

    lang = SEARCH_LANG.get(market, '현지어')
    prompt = f"""{date} {MARKET_NAME.get(market, market)} 증시 데일리 브리핑의 '시황 총평'을 작성하라.

시장 지수: {idx_ctx}

[상승 {len(top)}종목]
{brief(top)}

[하락 {len(bot)}종목]
{brief(bot)}

각 항목의 대괄호는 [사유분류/업종/개별재료 유무]다.

━━━ 이 총평의 역할 ━━━
리포트 표에는 종목별 배경이 두 줄씩 들어간다. 그건 이미 있으니 여기서 반복하지 마라.
여기서 필요한 건 개별 종목 나열이 아니라 **시장 전체를 관통하는 흐름과 그 원인**이다.
"무엇이 올랐다"가 아니라 "왜 그것이 올랐고 그래서 무엇이 갈렸는가"를 써라.

━━━ 인과관계를 반드시 드러내라 (가장 중요) ━━━
오늘 시장을 움직인 축이 있으면 그 축의 **원인**을 한 줄 배정해 설명하라.
예: 금·은 관련주가 몰려 올랐다면 → "금 현물이 올랐다"에서 멈추지 말고,
    왜 올랐는지(실질금리 하락 / 중앙은행 매수 / 지정학 리스크 / 달러 약세 등)를 적어라.
위 자료의 개별 재료만으로 매크로 원인을 알 수 없으면 웹검색으로 확인하라.
{lang} 또는 영어로 "{date} 금 가격 급등 이유" 같은 식으로 원인을 직접 검색하라.
검색해도 원인이 확인되지 않으면 "원인은 확인되지 않았다"고 쓰고 지어내지 마라.

━━━ 구성 ({OVERVIEW_LINES}줄) ━━━
1) 오늘 시장을 지배한 축 하나 — 무엇이 움직였는지
2) 그 축이 왜 생겼는지 — 원인·배경 (매크로 지표, 정책, 수급, 상품가격 등)
3) 그 축이 업종별로 어떻게 갈라졌는지 — 수혜와 피해
4) 지수와 개별 종목의 괴리 또는 두 번째 테마
5) 상승과 하락을 가른 기준 — 실적인지 테마인지 수급인지
6) 투자자 관점 시사점 — 무엇을 확인해야 하는지

━━━ 작성 규칙 ━━━
- 각 줄은 한 문장, {OVERVIEW_MIN_CHARS}~{OVERVIEW_MAX_CHARS}자.
- 확인한 숫자(금 온스당 가격, 금리, 환율, 지표치)는 넣어라. 새 정보라서 값이 있다.
- 개별 종목명은 흐름을 설명하는 데 필요한 경우에만 쓰고, 나열하지 마라.
- 자료에 없고 검색으로도 확인 못 한 사실을 추가하지 마라.
- 여러 종목이 "확인된_뉴스_없음"이면 그 사실 자체가 시황이다(수급 장세).
- 상승 또는 하락 종목이 없으면 그 사실을 그대로 반영하라.
- 불릿 기호·번호 없이 각 줄에 문장만 출력하라({OVERVIEW_LINES}줄).
- 한국어로 작성하라."""

    # 종목 리서치와 같은 이유로 예산을 넉넉히 준다(웹검색 결과 + thinking 이
    # 응답 예산을 같이 쓴다). 매크로 원인 확인에는 몇 번의 검색으로 충분하다.
    try:
        final = _stream_final(
            client,
            model=CLAUDE_MODEL,
            max_tokens=RESEARCH_MAX_TOKENS,
            thinking={'type': 'adaptive'},
            output_config={'effort': 'medium'},
            tools=[{'type': 'web_search_20260209', 'name': 'web_search',
                    'max_uses': OVERVIEW_SEARCH_MAX_USES}],
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = ''.join(b.text for b in final.content if b.type == 'text')
        lines = [re.sub(r'^\s*[-•*]\s*|^\s*\d+[.)]\s*', '', ln).strip()
                 for ln in text.strip().splitlines() if ln.strip()]
        lines = [clip(ln, OVERVIEW_CLIP) for ln in lines if ln][:OVERVIEW_LINES]
        if not lines:
            print(f'  [WARN] 총평 비어 있음 — {_describe_stop(final, text)}')
            return ['총평을 생성하지 못했습니다.']
        if len(lines) < OVERVIEW_LINES:
            print(f'  [WARN] 총평 {len(lines)}줄 (기대 {OVERVIEW_LINES}줄)')
        return lines
    except Exception as e:
        print(f'  [WARN] 총평 생성 실패: {e}')
        return ['총평을 생성하지 못했습니다.']


# ── 3단계: HTML 렌더링 (결정론적 템플릿) ────────────────────────────────────────
E = html.escape


def clip(text, limit):
    """렌더 단계 하드 캡.

    분량은 프롬프트로 지시하지만 모델이 넘길 수 있다. A4 1장 보장은 레이아웃
    쪽에서 결정론적으로 끊어야 하므로 여기서 한 번 더 자른다. 가능하면 마지막
    공백에서 끊고, 그럴 자리가 없으면 그냥 자른다.
    """
    t = re.sub(r'\s+', ' ', text or '').strip()
    if len(t) <= limit:
        return t
    cut = t[:limit]
    sp = cut.rfind(' ')
    if sp >= limit * 0.7:
        cut = cut[:sp]
    return cut.rstrip(' ,·') + '…'


def clean_name(name):
    """종목명에서 주식 종류 꼬리표를 떼고 길이를 캡한다.

    표에 필요한 건 회사 이름이다. 'Class A Common Stock' 류는 정보를 주지 않고
    종목명 열만 4~5줄로 밀어 행 높이를 지배한다(NAME_NOISE 주석 참고).
    """
    t = NAME_NOISE.sub('', str(name or '').strip()).strip(' ,.')
    return clip(t or str(name or ''), NAME_CLIP)


def render_idx_cards(market, idx_data):
    if not idx_data:
        return '<div style="font-size:8pt;color:#94a3b8;margin-bottom:8px">지수 데이터 없음</div>'
    cards = []
    for k in IDX_ORDER.get(market, []):
        idx = idx_data.get(k)
        if not idx:
            continue
        chg = idx.get('change') or 0
        pct = idx.get('changePct') or 0
        up = chg >= 0
        color  = '#16a34a' if up else '#dc2626'
        bg     = '#f0fdf4' if up else '#fff1f2'
        border = '#86efac' if up else '#fca5a5'
        arrow  = '▲' if chg > 0 else ('▼' if chg < 0 else '━')
        sign   = '+' if chg >= 0 else ''
        cards.append(
            f'<div style="padding:3px 9px;border-radius:6px;border:1.2px solid {border};'
            f'background:{bg};flex:1;min-width:0;display:flex;align-items:baseline;'
            f'gap:6px;flex-wrap:nowrap">'
            f'<span style="font-size:7.2pt;color:#64748b;font-weight:600;white-space:nowrap">'
            f'{E(str(idx.get("name", k)))}</span>'
            f'<span style="font-size:10pt;font-weight:800;color:#1e293b;white-space:nowrap">'
            f'{fmt_idx_val(idx.get("value"))}</span>'
            f'<span style="font-size:7pt;font-weight:800;color:{color};white-space:nowrap">'
            f'{arrow} {sign}{pct:.2f}%</span>'
            f'</div>'
        )
    if not cards:
        return '<div style="font-size:7pt;color:#94a3b8;margin-bottom:6px">지수 데이터 없음</div>'
    return ('<div style="display:flex;gap:6px;flex-wrap:nowrap;margin-bottom:6px">'
            + ''.join(cards) + '</div>')


def render_reason_prefix(confidence):
    """사유 분류·업종 딱지는 표기하지 않는다.

    남기는 것은 근거 신뢰도 표시 하나뿐이다. 이건 분류 라벨이 아니라
    "이 서술은 확인된 사실이 아니라 추정"이라는 경고여서, 다른 칸의
    데이터로 대체할 수 없다. 색 배경 없는 회색 소자로만 표시한다.
    """
    if confidence == 'low':
        return '<span style="color:#94a3b8;font-size:6.5pt;font-weight:700">추정 </span>'
    return ''


def render_table(rows, kind, code_header, n_max=10):
    """kind: 'up' | 'down'. rows 개수가 n_max보다 적으면 제목이 실제 개수를 반영한다.

    회사 소개와 등락 배경은 한 칸에 이어 쓴다. 두 열로 나누면 둘 중 긴 쪽이 행
    높이를 정해 짧은 쪽 자리가 통째로 버려지는데, 한 칸에 흘려 쓰면 그 낭비가
    없어져 20종목이 A4 한 장에 들어간다. 소개는 회색, 배경은 진한 색으로 두어
    글자색이 구분자 역할을 한다(구분 기호를 넣지 않아 줄바꿈 낭비도 없다).

    행 높이는 MERGED_MAX_LINES 줄로 고정한다. 내용 길이에 따라 행이 들쭉날쭉하면
    표가 흐트러져 보이므로, 짧은 행도 같은 높이를 차지하게 min-height 를 준다.
    (height + overflow:hidden 이 아니라 min-height 인 이유: 캡을 넘긴 내용이
     조용히 잘려 사라지는 것보다 행이 늘어나 눈에 보이는 편이 낫다. 캡이
     제대로 걸려 있으면 늘어나는 일은 없다 — 모듈 상단 예산 assert 참고.)

    종목코드·종목명·등락률은 세로 중앙에 둔다. 고정 높이 행에서 위로 붙으면
    두 줄짜리 배경 옆에서 어긋나 보인다.
    """
    up = kind == 'up'
    accent = '#16a34a' if up else '#dc2626'
    arrow = '▲' if up else '▼'
    word  = '상승' if up else '하락'
    bar = (f'background:#1e3a5f;color:#ffffff;font-size:8pt;font-weight:700;'
           f'padding:3px 8px;border-left:4px solid {accent}')

    if not rows:
        return (
            '<div style="border:1px solid #e2e8f0;border-radius:6px;overflow:hidden;margin-bottom:7px">'
            f'<div style="{bar}">{arrow} {word} 종목</div>'
            '<div style="padding:7px;font-size:7pt;color:#64748b">'
            f'해당 거래일에 {word}한 종목이 없습니다.</div></div>'
        )

    title = (f'{arrow} {word} TOP {n_max}' if len(rows) >= n_max
             else f'{arrow} {word} 종목 {len(rows)}개 (전 종목)')

    head = ['종목코드' if code_header == 'code' else 'Ticker', '종목명', '등락률',
            '회사 개요 · 등락 배경']
    # 종목명 열이 좁으면 영문 회사명이 여러 줄로 벌어져 행 높이를 지배한다.
    # 종목코드는 최장이 중국의 '688825.SS'(9자, 6.4pt 모노 ≈ 39px) 이므로 7% 로 충분.
    widths = ['7%', '19%', '7%', '67%']

    # thead 에 아래 경계를 준다. border-collapse 는 맞닿은 경계를 반씩 나누므로,
    # 헤더에 경계가 없으면 첫 행만 위쪽 0.5px 을 못 받아 다른 행보다 0.5px 낮아진다
    # (실측 31.63px vs 32.13px). 색은 헤더 배경과 같게 두어 보이지 않는다.
    ths = ''.join(
        f'<th style="width:{w};background:#334155;color:#ffffff;font-size:6.6pt;'
        f'font-weight:700;padding:2.5px 4px;text-align:center;border:0;'
        f'border-bottom:1px solid #334155">{E(h)}</th>'
        for h, w in zip(head, widths))

    # 합친 칸 고정 높이 = MERGED_MAX_LINES 줄. line-height 가 1.42 이므로 em 단위로
    # 정확히 떨어진다(2줄 = 2.84em).
    cell_h = f'{MERGED_MAX_LINES * 1.42:.2f}em'

    trs = []
    for i, r in enumerate(rows):
        bg = '#f8fafc' if i % 2 else '#ffffff'
        dim = 'color:#64748b;font-style:italic;' if r.get('confidence') == 'low' else ''
        td = ('padding:2.5px 4px;border-bottom:1px solid #e2e8f0;font-size:6.9pt;'
              f'background:{bg};')
        mid = td + 'vertical-align:middle;'
        profile = clip(r.get('profile', ''), PROFILE_CLIP)
        reason  = clip(r.get('reason', ''), REASON_CLIP)
        prof_html = (f'<span style="color:#8592a3">{E(profile)}</span> '
                     if profile else '')
        trs.append(
            '<tr>'
            f'<td style="{mid}text-align:center;font-family:Consolas,monospace;font-size:6.4pt;'
            f'color:#475569">{E(str(r["code"]))}</td>'
            f'<td style="{mid}font-weight:700;color:#1e293b;line-height:1.3">'
            f'{E(clean_name(r["name"]))}</td>'
            f'<td style="{mid}text-align:right;font-weight:800;color:{accent};white-space:nowrap">'
            f'{fmt_ret(r["ret"])}</td>'
            # overflow-wrap:anywhere — 긴 영문 토큰(티커·제품명)이 줄 앞에서
            # 통째로 넘어가면 2줄 예산이 3줄로 튄다. 어디서든 끊게 둔다.
            f'<td style="{td}vertical-align:top">'
            f'<div style="min-height:{cell_h};line-height:1.42;overflow-wrap:anywhere">'
            f'{prof_html}'
            f'<span style="{dim}">'
            f'{render_reason_prefix(r.get("confidence", ""))}'
            f'{E(reason)}</span></div></td>'
            '</tr>')

    return (
        '<div style="border:1px solid #e2e8f0;border-radius:6px;overflow:hidden;margin-bottom:7px">'
        f'<div style="{bar}">{title}</div>'
        '<table style="width:100%;border-collapse:collapse;table-layout:fixed">'
        f'<thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table></div>'
    )


# 출처 목록은 리포트에 싣지 않는다(2026-08-06).
#   종이에서 URL 은 클릭할 수 없어 쓸모가 없는데 자리는 크게 먹었다(20건이면
#   A4 세로 예산의 90px). 그 자리를 시황 코멘트와 등락 배경 분량으로 돌렸다.
#   source_url / source_date 는 계속 수집한다 — 근거를 찾았는지 확인하는
#   장치이고, confidence 판정과 실행 로그의 '✓' 표시가 여기에 걸려 있다.


def render_html(market, date, idx_data, overview, top, bot):
    market_name = MARKET_NAME.get(market, market)
    code_header = 'code' if market == 'kr' else 'ticker'
    tbl_up   = render_table(top, 'up', code_header)
    tbl_down = render_table(bot, 'down', code_header)

    ov = ''.join(
        '<div style="display:flex;gap:4px;margin-bottom:2px">'
        '<span style="color:#94a3b8;flex-shrink:0">•</span>'
        f'<span>{E(line)}</span></div>'
        for line in overview)

    rows_all = top + bot
    total = len(rows_all)
    unknown = sum(1 for r in rows_all if r.get('catalyst_type') == '확인된_뉴스_없음')
    indiv   = sum(1 for r in rows_all if r.get('has_individual_issue'))
    nofetch = sum(1 for r in rows_all if r.get('research_failed'))
    quality = (f'<span style="color:#94a3b8;font-size:6pt">'
               f'대상 {total}종목 · 개별 재료 {indiv}건 · 섹터/매크로 {total - unknown - indiv}건 · '
               f'미확인 {unknown}건'
               + (f' · <b style="color:#dc2626">확인 실패 {nofetch}건</b>' if nofetch else '')
               + '</span>') if total else ''

    head_block = (
        f'<div style="display:flex;justify-content:space-between;align-items:baseline;'
        f'margin-bottom:5px">'
        f'<span style="font-size:10pt;font-weight:800;color:#1e293b">'
        f'데일리 마켓 브리핑 · {E(market_name)} 증시</span>'
        f'<span style="font-size:6.6pt;color:#94a3b8">🕒 __GEN_TIME__ KST</span></div>'
        f'<div style="font-size:7pt;color:#64748b;margin-bottom:6px">'
        f'기준일 {E(date)} · 기준 기간 1D · 시장 {E(market_name)}</div>'
    )

    # 레이아웃: A4 세로 한 장 고정.
    #   여백 10mm → 가용 190 x 277mm. 표 20행이 여기 들어가야 하므로 본문은
    #   6.9pt / 행 padding 2.5px 로 잡았고, 회사 소개와 등락 배경을 한 칸에
    #   합쳐 행당 2줄 안에 끝나게 했다. 분량은 clip() 이 결정론적으로 끊는다.
    #   (예전 구조는 .page 2개 + page-break-after 라서 인쇄 시 2~4장이 됐다)
    return f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="utf-8">
<title>데일리 마켓 브리핑 · {E(market_name)} 증시 · {E(date)}</title>
<style>
  @page {{ size: A4 portrait; margin: 10mm; }}
  body {{ margin:0; padding:16px 0; background:#e2e8f0;
          font-family:'Noto Sans KR','Malgun Gothic',sans-serif; color:#1e293b; }}
  .page {{ width:210mm; height:297mm; margin:0 auto; padding:10mm;
           background:#fff; box-shadow:0 2px 12px rgba(0,0,0,.15);
           box-sizing:border-box; overflow:hidden; }}
  table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
  thead {{ display:table-header-group; }}
  tr    {{ page-break-inside:avoid; break-inside:avoid; }}
  @media print {{
    body {{ -webkit-print-color-adjust:exact; print-color-adjust:exact;
            background:#fff; padding:0; }}
    /* 인쇄에서는 @page 여백이 자리를 잡으므로 .page 자체 여백을 없앤다.
       height:auto 로 두어야 마지막 빈 페이지가 딸려 나오지 않는다. */
    .page {{ box-shadow:none; margin:0; padding:0; height:auto; overflow:visible; }}
  }}
</style></head>
<body>
<div class="page">
  {head_block}
  <div style="border:1px solid #e2e8f0;border-radius:6px;padding:5px 8px;margin-bottom:7px">
    {render_idx_cards(market, idx_data)}
    <div style="background:#f8fafc;border-radius:5px;padding:6px 8px;font-size:7pt;
                line-height:1.48;color:#334155">{ov}</div>
  </div>
  {tbl_up}
  {tbl_down}
  <div style="display:flex;justify-content:flex-end">{quality}</div>
</div>
</body></html>"""


def inject_timestamp(html_doc, ts):
    if '__GEN_TIME__' in html_doc:
        return html_doc.replace('__GEN_TIME__', ts)
    fallback = (
        '<div style="position:fixed;top:6px;right:12px;z-index:50;font-size:8pt;'
        f'color:#64748b;font-family:sans-serif">🕒 생성 {ts} KST</div>'
    )
    m = re.search(r'<body[^>]*>', html_doc, re.I)
    return (html_doc[:m.end()] + fallback + html_doc[m.end():]) if m else (fallback + html_doc)


# ── 오프라인 렌더링 검증 ────────────────────────────────────────────────────────
def self_test(out_path):
    """API/Firebase 없이 템플릿만 검증한다."""
    idx_data = {
        'kospi':    {'name': 'KOSPI',     'value': 3421.55, 'change': 28.4, 'changePct': 0.84},
        'kospi200': {'name': 'KOSPI 200', 'value': 465.12,  'change': 4.1,  'changePct': 0.89},
        'kosdaq':   {'name': 'KOSDAQ',    'value': 812.33,  'change': -3.2, 'changePct': -0.39},
    }
    long_reason = ('3분기 영업이익이 시장 컨센서스를 18% 상회한 4.2조원으로 발표되며 장중 급등했다. '
                   'HBM3E 12단 제품의 엔비디아 퀄 테스트 통과 소식이 함께 전해졌고, '
                   '증권사 4곳이 목표주가를 평균 12% 상향 조정했다.')
    no_news = ('개별 공시·뉴스 및 업종 이슈 모두 미확인. 수급 요인에 따른 변동으로 추정된다.')
    sector_issue = '미국 상무부 대중 반도체 장비 수출 규제 완화 발표로 업종 전반이 반등했다.'
    top = []
    for i in range(10):
        indiv = (i % 3 == 0)
        top.append({
            'code': f'00593{i}', 'name': f'테스트종목{i}', 'ret': 0.12 - i * 0.008,
            'ret5': 0.2, 'mcap': 5e12, 'cur': 'KRW',
            # 명사형 종결(서술어·마침표 없음)이 현재 규격이다
            'profile': '메모리 반도체를 설계·생산해 글로벌 IT 제조사에 공급',
            'reason': long_reason if indiv else no_news,
            'catalyst_type': ['실적', '섹터·테마', '확인된_뉴스_없음'][i % 3],
            # i%3==1 → 반도체 섹터 공통(축약 대상 4종목), i%3==2 → 섹터 미확인(원문 유지)
            'sector': '반도체' if i % 3 == 1 else ('2차전지' if indiv else ''),
            'has_individual_issue': indiv,
            'sector_issue': sector_issue if i % 3 == 1 else '',
            'source_url': f'https://example.com/news/article-{i}-long-path' if indiv else '',
            'source_date': '2026-07-27',
            'confidence': ['high', 'medium', 'low'][i % 3],
        })
    bot = [dict(s, ret=-abs(s['ret']), code=f'01234{i}', name=f'하락종목{i}')
           for i, s in enumerate(top)]

    # 섹터 축약은 상승/하락 표 안에서 각각 독립적으로 적용된다
    # 반도체 그룹 = i%3==1 → i 1,4,7 의 3종목 → 대표 1 + 축약 2
    c_up, c_down = collapse_sector_duplicates(top), collapse_sector_duplicates(bot)
    assert (c_up, c_down) == (2, 2), f'섹터 축약 결과 이상: {c_up}, {c_down}'
    leads = [r for r in top if r.get('sector_role') == 'lead']
    dups  = [r for r in top if r.get('sector_role') == 'dup']
    assert len(leads) == 1 and len(dups) == 2, '섹터 대표/축약 분류 이상'
    assert leads[0]['reason'].startswith('반도체 섹터 공통 이슈 — '), '대표 코멘트 형식 이상'
    assert dups[0]['reason'] == '반도체 섹터 동일 이슈 영향 (테스트종목1과 동일)', '축약 코멘트 형식 이상'
    assert all(not d['source_url'] for d in dups), '축약 종목의 각주가 남아 있음'
    # 개별 재료가 있는 종목은 축약되지 않아야 한다
    assert all(r.get('sector_role') is None for r in top if r['has_individual_issue']), \
        '개별 재료 종목이 축약됨'

    overview = [
        '반도체가 오늘 지수를 끌어올린 유일한 축으로, 상승 10종목 중 6개가 이 업종이다.',
        '미 상무부가 대중 장비 수출 규제를 완화하며 장비 발주 재개 기대가 살아난 결과다.',
        '장비·소재는 동반 강세였지만 중국 매출 비중이 낮은 후공정은 상승에서 빠졌다.',
        '지수는 0.84% 올랐는데 코스닥은 밀려, 대형주로만 자금이 몰린 하루였다.',
        '실적이 확인된 종목이 평균 8%대, 재료 미확인 종목은 3%대로 격차가 벌어졌다.',
        '규제 완화가 실제 발주로 이어지는지 3분기 장비 수주 공시를 확인해야 한다.',
    ]
    assert len(overview) == OVERVIEW_LINES, '샘플 총평 줄 수가 설정과 다르다'
    doc = render_html('kr', '2026-07-28', idx_data, overview, top, bot)
    doc = inject_timestamp(doc, '2026-07-28 16:30')

    assert doc.startswith('<!DOCTYPE html>'), 'DOCTYPE 누락'
    assert doc.rstrip().endswith('</html>'), '</html> 누락'
    assert '__GEN_TIME__' not in doc, '타임스탬프 미치환'
    assert doc.count('<div class="page">') == 1, 'A4 1장이어야 한다'
    assert 'page-break-after' not in doc, '페이지 강제분할 규칙이 남아 있음'
    assert '테스트종목0' in doc and '하락종목9' in doc, '행 누락'
    # 회사 소개는 별도 열이 아니라 등락 배경과 같은 칸에 있어야 한다
    # '<th' 로 세면 <thead> 까지 잡히므로 속성까지 붙여 센다
    assert doc.count('<th style') == 8, \
        f'열 개수 이상(표 2개 x 4열 기대): {doc.count("<th style")}'
    assert '회사 개요 · 등락 배경' in doc, '합친 열 제목 누락'
    assert '>회사 소개<' not in doc, '회사 소개 열이 아직 분리돼 있음'
    assert doc.count('▲ 상승 TOP 10') == 1 and doc.count('▼ 하락 TOP 10') == 1, '표 제목 이상'

    # ── 출처는 리포트에 싣지 않는다 ────────────────────────────────────────────
    assert doc.count('<sup') == 0, '각주 첨자가 남아 있음'
    assert '>출처<' not in doc and 'example.com' not in doc, '출처 목록이 남아 있음'
    assert 'render_footnotes' not in globals(), 'render_footnotes 가 아직 살아 있음'

    # ── 행 높이 균일 + 앞 3열 세로 중앙 ────────────────────────────────────────
    # 합친 칸에 고정 높이 컨테이너가 들어가야 짧은 행도 같은 높이를 차지한다
    assert doc.count(f'min-height:{MERGED_MAX_LINES * 1.42:.2f}em') == 20, \
        '합친 칸 고정 높이 컨테이너가 20행에 안 붙었다'
    # 종목코드/종목명/등락률 = 3열 x 20행
    assert doc.count('vertical-align:middle') == 60, \
        f'세로 중앙 정렬 칸 수 이상: {doc.count("vertical-align:middle")}'
    # 합친 칸만 위 정렬 (20행)
    assert doc.count('vertical-align:top') == 20, \
        f'합친 칸 위 정렬 수 이상: {doc.count("vertical-align:top")}'
    # thead 아래 경계 — 없으면 첫 행이 다른 행보다 0.5px 낮아진다
    assert doc.count('border-bottom:1px solid #334155') == 8, \
        'thead 아래 경계 누락 — 첫 행 높이가 어긋난다'

    # ── 총평 ──────────────────────────────────────────────────────────────────
    for line in overview:
        assert line in doc, f'총평 줄 누락: {line[:20]}'
    # 사유 분류·업종 딱지가 렌더링되지 않아야 한다.
    # (총평 산문에 '실적' 같은 단어가 정상적으로 등장하므로 단어가 아니라
    #  딱지 마크업 자체를 검사한다)
    assert 'display:inline-block;padding:0 5px;border-radius:4px' not in doc, '딱지 마크업이 남아 있음'
    for css in ('#f0fdfa', '#eff6ff', '#f5f3ff', '#fdf2f8', '#fffbeb'):
        assert css not in doc, f'딱지 배경색이 남아 있음: {css}'
    # 업종명은 딱지로는 안 나오고, 축약 문구 안에서만 나온다
    assert '2차전지' not in doc, '업종 딱지가 남아 있음'
    # 축약 코멘트 본문의 업종명은 남아야 한다(딱지가 아니라 문장)
    assert '반도체 섹터 동일 이슈 영향' in doc, '섹터 축약 문구 누락'
    # 등락 수치를 코멘트에서 재인용하지 않는지 (샘플 기준)
    assert '초과수익' not in doc and '5거래일' not in doc, '코멘트에 등락 수치 재인용'
    # 근거 신뢰도 표시는 유지
    assert '>추정 <' in doc, '추정 표시 누락'
    # 인쇄 시 표가 여러 장으로 넘어가도 깨지지 않아야 한다
    assert 'display:table-header-group' in doc, '표 헤더 반복 규칙 누락'
    assert 'page-break-inside:avoid' in doc, '행 미분할 규칙 누락'
    # HTML 이스케이프 동작 확인
    assert '<script>' not in render_table(
        [{'code': 'X', 'name': '<script>alert(1)</script>', 'ret': 0.1,
          'profile': '<b>x</b>', 'reason': '<img onerror=1>',
          'catalyst_type': '실적', 'confidence': 'high', 'source_url': ''}],
        'up', 'code'), 'HTML 이스케이프 실패'

    # ── clip(): 렌더 단계 하드 캡 ──────────────────────────────────────────────
    assert clip('짧다', 20) == '짧다', '짧은 문장이 변형됨'
    assert clip('  공백   정리   확인  ', 40) == '공백 정리 확인', '공백 정규화 실패'
    long_ko = '가' * 200
    assert len(clip(long_ko, REASON_CLIP)) == REASON_CLIP + 1, '하드 캡 길이 이상(…포함)'
    assert clip(long_ko, REASON_CLIP).endswith('…'), '생략 표시 누락'
    spaced = '앞부분 문장입니다 ' * 20
    c = clip(spaced, 50)
    assert len(c) <= 51 and c.endswith('…') and not c.endswith(' …'), f'공백 절단 이상: {c!r}'

    # ── clean_name(): 미국 종목명 꼬리표 제거 ──────────────────────────────────
    # 실제 Nasdaq 원문(2026-08-06 미국편에서 뽑음)
    for raw, want in [
        ('Space Exploration Technologies Corp. Class A Common Stock',
         'Space Exploration Technologies Corp'),
        ('Advanced Micro Devices Inc. Common Stock', 'Advanced Micro Devices Inc'),
        ('Shopify Inc. Class A Subordinate Voting Shares', 'Shopify Inc'),
        ('Gold Fields Limited American Depositary Shares', 'Gold Fields Limited'),
        ('United Microelectronics Corporation (NEW) Common Stock',
         'United Microelectronics Corporation'),
        ('Thomson Reuters Corporation Common Shares', 'Thomson Reuters Corporation'),
        ('삼성전자', '삼성전자'),                    # 한글명은 건드리지 않는다
        ('CXMT CORPORATION', 'CXMT CORPORATION'),
    ]:
        got = clean_name(raw)
        assert got == want, f'clean_name({raw!r}) -> {got!r}, 기대 {want!r}'
    assert len(clean_name('X' * 80)) == NAME_CLIP + 1, '종목명 하드 캡 미적용'

    # 프롬프트 한도를 넘긴 입력도 렌더에서 잘려야 한다
    over = render_table(
        [{'code': 'Z', 'name': '초과종목', 'ret': 0.05,
          'profile': '나' * 300, 'reason': '다' * 300,
          'catalyst_type': '실적', 'confidence': 'high', 'source_url': ''}],
        'up', 'code')
    assert '나' * (PROFILE_CLIP + 1) not in over, '회사 소개가 캡을 넘겨 렌더됨'
    assert '다' * (REASON_CLIP + 1) not in over, '등락 배경이 캡을 넘겨 렌더됨'
    assert over.count('…') == 2, f'생략 표시 개수 이상: {over.count("…")}'

    # ── _profile_is_current(): 옛 캐시 무효화 ──────────────────────────────────
    assert _profile_is_current('메모리 반도체를 설계·생산해 글로벌 IT 제조사에 공급')
    assert _profile_is_current('온라인 쇼핑몰 구축·결제 플랫폼을 전세계 판매자에 제공')
    assert not _profile_is_current('메모리 반도체를 생산해 IT 제조사에 공급한다'), '서술형이 통과됨'
    assert not _profile_is_current('반도체를 만드는 기업이다.'), '마침표·서술형이 통과됨'
    assert not _profile_is_current('가' * (PROFILE_CLIP + 1)), '긴 소개가 통과됨'
    assert not _profile_is_current(''), '빈 소개가 통과됨'

    # ── 리서치 실패 종목 표기 ──────────────────────────────────────────────────
    failed_rows = [dict(top[0], research_failed=True,
                        reason='등락 배경을 확인하지 못했습니다.', source_url='')]
    docf = render_html('kr', '2026-07-28', idx_data, ['총평.'], failed_rows, [])
    assert '확인 실패 1건' in docf, '리서치 실패 건수가 품질 요약에 안 보임'
    assert '리서치 실패 — 데이터를 가져오지 못했습니다' not in docf, '옛 실패 문구가 남아 있음'
    assert '확인 실패' not in doc, '실패가 없는데 실패 건수가 표기됨'

    # ── 가변 종목 수: 상승 3개 / 하락 0개 ──────────────────────────────────────
    doc2 = render_html('kr', '2026-07-28', idx_data, ['전 종목이 상승했다.'], top[:3], [])
    assert '▲ 상승 종목 3개 (전 종목)' in doc2, '상승 개수 표기 이상'
    assert '하락한 종목이 없습니다' in doc2, '빈 하락 표 처리 이상'
    assert doc2.count('<div class="page">') == 1, '빈 표 페이지 이상'
    # 반대 방향
    doc3 = render_html('kr', '2026-07-28', idx_data, ['전 종목이 하락했다.'], [], bot[:5])
    assert '▼ 하락 종목 5개 (전 종목)' in doc3, '하락 개수 표기 이상'
    assert '상승한 종목이 없습니다' in doc3, '빈 상승 표 처리 이상'
    # 양쪽 모두 비어도 죽지 않아야 한다
    render_html('kr', '2026-07-28', idx_data, ['해당 없음.'], [], [])

    # ── get_top_bottom: 부호 기준 분리 + 가변 개수 ────────────────────────────
    st = [{'c': 'A', 'n': 'a', 'm': 1}, {'c': 'B', 'n': 'b', 'm': 1},
          {'c': 'C', 'n': 'c', 'm': 1}, {'c': 'D', 'n': 'd', 'm': 1}]
    pr = [[110, 90, 100, 130], [100, 100, 100, 100]]   # +10%, -10%, 0%, +30%
    g_, l_ = get_top_bottom(st, pr, 'kr')
    assert [x['code'] for x in g_] == ['D', 'A'], f'상승 추출 이상: {g_}'
    assert [x['code'] for x in l_] == ['B'], f'하락 추출 이상: {l_}'
    assert not ({x['code'] for x in g_} & {x['code'] for x in l_}), '상승/하락 중복'

    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(doc)
    print(f'[self-test] OK — {len(doc):,} bytes, A4 1장, 총평 {OVERVIEW_LINES}줄, '
          f'행 높이 {MERGED_MAX_LINES}줄 고정, 섹터축약 상승 {c_up}건/하락 {c_down}건 '
          f'→ {out_path}')
    print('  ※ 실제 1페이지 여부는 브라우저 인쇄 높이로 확인해야 한다 '
          '(자리 예산: 190 x 277mm).')
    return doc


# ── 메인 ────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--market', choices=['kr', 'us', 'jp', 'cn'])
    ap.add_argument('--self-test', action='store_true', help='API 없이 HTML 렌더링만 검증')
    ap.add_argument('--out', default='review_selftest.html')
    ap.add_argument('--force', action='store_true',
                    help='해당 기준일 리뷰가 이미 있어도 다시 생성한다')
    args = ap.parse_args()

    if args.self_test:
        self_test(args.out)
        return
    if not args.market:
        ap.error('--market 또는 --self-test 중 하나가 필요합니다.')
    market = args.market

    init_firebase()

    print(f'[{market}] Firebase에서 데이터 읽는 중...')
    data = fb_ref(f'/v1/{market}').get()
    if not data:
        print(f'[ERROR] /v1/{market} 데이터가 없습니다.')
        sys.exit(1)

    stocks  = data.get('stocks') or []
    prices  = data.get('prices') or []
    dates   = data.get('dates') or []
    indices = data.get('indices') or {}

    if not stocks or len(prices) < 2 or not dates:
        print('[ERROR] 무버 계산에 필요한 데이터(stocks/prices/dates)가 부족합니다.')
        sys.exit(1)

    date = dates[0]

    # ── 멱등 가드: 이 기준일 리뷰가 이미 있으면 아무것도 하지 않는다 ───────────
    # 리뷰 트리거가 두 경로(항상 켜둔 PC 루프 + GitHub cron 백업)에서 오므로
    # 먼저 도착한 쪽만 실제로 생성하고 나머지는 몇 초 만에 no-op 으로 끝나게 한다.
    # 덕분에 cron 을 백업으로 켜도 Claude API 를 두 번 쓰지 않는다.
    #
    # 키를 '기준일'(= /v1/{market}/updated, 마지막 거래일)로 잡는 게 핵심이다.
    # 휴장일에는 기준일이 안 넘어가므로 이미 있는 리뷰를 다시 만들지 않고,
    # 수집이 아직 새 거래일을 못 올렸으면 리뷰할 게 없으니 그것도 건너뛴다.
    # 둘 다 올바른 동작이다.
    if not args.force:
        existing = fb_ref(f'/reviews_history/{market}/{date}/updated_at').get()
        if existing:
            print(f'[{market}] 기준일 {date} 리뷰가 이미 있습니다 (게시: {existing}).')
            print(f'[{market}] 건너뜀 — 다시 만들려면 --force. (에러 아님)')
            return

    top10, bot10 = get_top_bottom(stocks, prices, market)
    idx_data = get_idx_for_date(indices, date)
    idx_ctx, bench = index_context(market, idx_data)

    if not top10 and not bot10:
        print('[ERROR] 유효한 등락 종목이 없습니다.')
        sys.exit(1)

    enrich(top10, bench)
    enrich(bot10, bench)
    targets = top10 + bot10
    # 상승/하락 종목이 10개 미만이면 있는 만큼만 다룬다
    print(f'[{market}] 기준일 {date}  |  상승 {len(top10)}종목 / 하락 {len(bot10)}종목'
          f'{"  (10개 미만 — 해당 종목만 처리)" if min(len(top10), len(bot10)) < 10 else ""}'
          f'  |  지수 {idx_ctx}')

    profiles = load_profiles(market, [s['code'] for s in targets])
    print(f'[{market}] 회사 소개 캐시 적중 {len(profiles)}/{len(targets)}종목')

    # SDK 기본 재시도는 2회다. 20종목을 동시 6개로 돌리면 429 가 뭉쳐서 오므로
    # 넉넉히 잡는다 (call_research 의 백오프 재시도는 이게 소진된 뒤에 붙는다).
    client = anthropic.Anthropic(max_retries=4)

    print(f'[{market}] 1단계 — 종목별 리서치 (동시 {RESEARCH_WORKERS}개, 웹검색, '
          f'max_tokens {RESEARCH_MAX_TOKENS:,})...')
    with ThreadPoolExecutor(max_workers=RESEARCH_WORKERS) as pool:
        results = list(pool.map(
            lambda s: research_stock(client, market, date, s, idx_ctx, profiles.get(s['code'])),
            targets))

    top_r = results[:len(top10)]
    bot_r = results[len(top10):]

    indiv   = sum(1 for r in results if r['has_individual_issue'])
    unknown = sum(1 for r in results if r['catalyst_type'] == '확인된_뉴스_없음')
    nofetch = sum(1 for r in results if r.get('research_failed'))
    print(f'[{market}] 리서치 완료 — 개별재료 {indiv} / 섹터·매크로 {len(results) - unknown - indiv} '
          f'/ 미확인 {unknown} / 확인실패 {nofetch} (총 {len(results)}종목)')

    # ── 게시 게이트 ────────────────────────────────────────────────────────────
    # 빈칸이 가득한 리포트를 올리면 멱등 가드가 그날의 재시도를 막아버려서
    # 하루 종일 그대로 남는다(2026-08-05 중국편이 18/20 실패로 그랬다).
    # 게시를 안 하면 /reviews_history/{market}/{date} 가 비어 있으므로
    # 30분 뒤 백업 cron 이 같은 기준일로 다시 시도한다.
    ok_ratio = 1 - nofetch / len(results)
    if ok_ratio < PUBLISH_MIN_OK_RATIO:
        print(f'[{market}] [ERROR] 리서치 성공률 {ok_ratio:.0%} '
              f'({len(results) - nofetch}/{len(results)}) — 게시 기준 '
              f'{PUBLISH_MIN_OK_RATIO:.0%} 미달.')
        print(f'[{market}] 반쪽 리포트를 올리지 않고 실패로 끝냅니다.')
        print(f'[{market}] 이 기준일 리포트가 아직 없으면 백업 cron 이 다시 시도합니다. '
              f'이미 올라간 리포트가 있으면 그대로 남으니 --force 로 재생성하세요.')
        sys.exit(1)
    if nofetch:
        print(f'[{market}] [WARN] {nofetch}종목 확인 실패 — 게시는 진행합니다 '
              f'(성공률 {ok_ratio:.0%}).')

    # 개별 재료 없이 같은 섹터 이슈를 공유하는 종목은 코멘트를 축약한다 (상승/하락 각각)
    print(f'[{market}] 섹터 중복 코멘트 축약 중...')
    collapsed = collapse_sector_duplicates(top_r) + collapse_sector_duplicates(bot_r)
    print(f'[{market}] 섹터 축약 {collapsed}건')

    saved = save_profiles(market, results)
    if saved:
        print(f'[{market}] 회사 소개 캐시 {saved}건 갱신')

    print(f'[{market}] 2단계 — 총평 생성...')
    overview = build_overview(client, market, date, idx_ctx, top_r, bot_r)
    for line in overview:
        print(f'  • {line}')

    print(f'[{market}] 3단계 — HTML 렌더링...')
    ts = datetime.now(KST).strftime('%Y-%m-%d %H:%M')
    html_doc = inject_timestamp(render_html(market, date, idx_data, overview, top_r, bot_r), ts)

    payload = {'html': html_doc, 'updated_at': ts, 'base_date': date}

    print(f'[{market}] Firebase /reviews/{market} 에 게시 중...')
    fb_ref(f'/reviews/{market}').set(payload)                # 최신본 (index.html 호환)
    fb_ref(f'/reviews_history/{market}/{date}').set(payload)  # 날짜별 아카이브
    print(f'[{market}] 완료! (게시: {ts}  ·  기준일 {date}  ·  '
          f'재료확인 {len(results) - unknown}/{len(results)}  ·  {len(html_doc):,} bytes)')


if __name__ == '__main__':
    main()
