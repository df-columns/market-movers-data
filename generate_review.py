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

import os, sys, re, json, html, argparse
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
SEARCH_MAX_USES   = 8      # 종목당 웹검색 최대 횟수
PROFILE_TTL_DAYS  = 180    # 회사 소개 캐시 유효기간
REASON_MIN_CHARS  = 150    # 등락 배경 목표 길이 하한(프롬프트용)
REASON_MAX_CHARS  = 260    # 등락 배경 목표 길이 상한(프롬프트용, 인쇄 분량과 트레이드오프)

# HTML의 시장별 지수 순서와 동일
IDX_ORDER = {
    'kr': ['kospi', 'kospi200', 'kosdaq'],
    'us': ['sp500', 'ndx100', 'dji30'],
    'jp': ['n225', 'topix'],
    'cn': ['csi300', 'hsi'],
}
MARKET_NAME = {'kr': '한국', 'us': '미국', 'jp': '일본', 'cn': '중국'}
# 초과수익(alpha) 계산 기준 지수
BENCH_IDX   = {'kr': 'kospi', 'us': 'sp500', 'jp': 'n225', 'cn': 'csi300'}
# 검색에 사용할 언어 — 현지어로 검색해야 개별 종목 뉴스가 잡힌다
SEARCH_LANG = {'kr': '한국어', 'us': '영어', 'jp': '일본어', 'cn': '중국어(간체)'}
DEFAULT_CUR = {'kr': 'KRW', 'us': 'USD', 'jp': 'JPY', 'cn': 'CNY'}

# 등락 사유 분류 → (라벨색, 배경색)
CATALYST_STYLE = {
    '실적':             ('#1d4ed8', '#eff6ff'),
    '공시':             ('#7c3aed', '#f5f3ff'),
    '수주·계약':        ('#0369a1', '#f0f9ff'),
    '정책·규제':        ('#b45309', '#fffbeb'),
    '섹터·테마':        ('#0f766e', '#f0fdfa'),
    '지수·매크로':      ('#475569', '#f8fafc'),
    '수급':             ('#be185d', '#fdf2f8'),
    '확인된_뉴스_없음': ('#64748b', '#f1f5f9'),
}
CATALYST_ENUM = list(CATALYST_STYLE.keys())

# 등락 배경 구조화 출력 스키마
RESEARCH_SCHEMA = {
    "type": "object",
    "properties": {
        "reason":        {"type": "string"},
        "catalyst_type": {"type": "string", "enum": CATALYST_ENUM},
        "source_url":    {"type": "string"},
        "source_date":   {"type": "string"},
        "confidence":    {"type": "string", "enum": ["high", "medium", "low"]},
        "profile":       {"type": "string"},
    },
    "required": ["reason", "catalyst_type", "source_url", "source_date", "confidence", "profile"],
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


def get_top_bottom(stocks, prices, market):
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
    lst.sort(key=lambda x: x['ret'], reverse=True)
    return lst[:10], list(reversed(lst[-10:]))


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
        "- profile: 이 회사의 핵심 사업을 한 문장(60~90자)으로. 무엇을 만들어 어디에 파는지 구체적으로."
        if need_profile else
        '- profile: 빈 문자열("")로 두어라. 이미 확보돼 있다.'
    )

    return f"""{date} {market_name} 증시에서 {stock['name']}({stock['code']}) 주가가 {fmt_ret(stock['ret'])} 움직였다.
이 종목이 왜 그렇게 움직였는지 웹 검색으로 확인하라.

━━━ 종목 정보 ━━━
종목명/코드 : {stock['name']} / {stock['code']}
당일 등락률  : {fmt_ret(stock['ret'])}
시장 지수    : {idx_ctx}
{excess_line}
{ret5_line}
{stock['mcap_str']}

━━━ 검색 방법 ━━━
반드시 {lang}로 검색하라. 영어로 검색하면 현지 종목 뉴스가 잡히지 않는다.
검색어 예시: "{stock['name']} 주가", "{stock['name']} 공시", "{stock['name']} {date}", "{stock['code']} 뉴스"

━━━ 작성 규칙 (엄수) ━━━
1. {date} 기준 ±3일 이내에 실제로 보도·공시된 내용만 근거로 삼아라.
2. ★ 개별 재료를 찾지 못했으면 절대 지어내지 마라.
   catalyst_type을 "확인된_뉴스_없음"으로 하고, reason에는 관찰된 사실만 적어라.
   예: "개별 공시·뉴스 미확인. 지수가 {idx_ctx}인 가운데 {excess_line}로,
        업종 순환매 또는 수급 요인에 따른 변동으로 추정된다."
   근거 없는 그럴듯한 서술보다 "확인 안 됨"이 훨씬 가치 있다.
3. 지수가 이 종목과 같은 방향으로 크게 움직였고 초과수익이 작다면,
   개별 재료로 포장하지 말고 "지수·매크로"로 분류하라.
4. reason은 {REASON_MIN_CHARS}~{REASON_MAX_CHARS}자. 숫자(실적 수치, 계약 규모, 목표주가 등)를
   확인했다면 반드시 포함하라. 형용사보다 사실을 써라.
5. source_url은 실제로 검색 결과에서 확인한 기사·공시 URL. 없으면 빈 문자열("").
   source_date는 그 출처의 보도일(YYYY-MM-DD). 없으면 빈 문자열("").
6. confidence — high: 공시·IR 등 1차 출처 확인 / medium: 언론 보도 확인 / low: 추정
{profile_rule}

한국어로 작성하라."""


def _json_fallback_instruction():
    return (
        "\n\n━━━ 출력 형식 ━━━\n"
        "설명 없이 아래 키를 가진 JSON 객체 하나만 출력하라.\n"
        '{"reason": "...", "catalyst_type": "'
        + '|'.join(CATALYST_ENUM)
        + '", "source_url": "...", "source_date": "YYYY-MM-DD", '
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
    """스트리밍 + pause_turn 재개 루프 (웹검색은 서버측 루프에서 일시정지될 수 있음)"""
    messages = list(kwargs.pop('messages'))
    final = None
    for _ in range(6):
        with client.messages.stream(messages=messages, **kwargs) as stream:
            final = stream.get_final_message()
        if final.stop_reason == 'pause_turn':
            messages = messages + [{'role': 'assistant', 'content': final.content}]
            continue
        break
    return final


def call_research(client, prompt):
    """구조화 출력으로 리서치 결과를 받는다.

    output_config.format(구조화 출력)을 우선 시도하고, 서버가 거부하면
    JSON 지시문 + 수동 파싱으로 단계적으로 낮춰 재시도한다.
    (로컬에서 실호출 검증이 불가능한 환경이라 방어적으로 작성)
    """
    tools = [{'type': 'web_search_20260209', 'name': 'web_search', 'max_uses': SEARCH_MAX_USES}]
    base = dict(model=CLAUDE_MODEL, max_tokens=8000,
                thinking={'type': 'adaptive'}, tools=tools)

    attempts = [
        dict(base,
             output_config={'effort': 'medium',
                            'format': {'type': 'json_schema', 'schema': RESEARCH_SCHEMA}},
             messages=[{'role': 'user', 'content': prompt}]),
        dict(base,
             output_config={'effort': 'medium'},
             messages=[{'role': 'user', 'content': prompt + _json_fallback_instruction()}]),
        dict(base,
             messages=[{'role': 'user', 'content': prompt + _json_fallback_instruction()}]),
    ]

    last_err = None
    for i, kwargs in enumerate(attempts):
        try:
            final = _stream_final(client, **kwargs)
            text = ''.join(b.text for b in final.content if b.type == 'text')
            data = _extract_json(text)
            if data:
                return data
            last_err = f'JSON 파싱 실패 (응답 {len(text)}자)'
        except anthropic.BadRequestError as e:
            last_err = f'400: {e}'
            if i < len(attempts) - 1:
                print(f'    [degrade] 옵션 축소 후 재시도 — {e}')
                continue
            raise
        except Exception as e:
            last_err = f'{type(e).__name__}: {e}'
            break
    raise RuntimeError(last_err or '리서치 실패')


def research_stock(client, market, date, stock, idx_ctx, cached_profile):
    need_profile = not cached_profile
    prompt = build_research_prompt(market, date, stock, idx_ctx, need_profile)
    try:
        data = call_research(client, prompt)
    except Exception as e:
        print(f'  [WARN] {stock["name"]}({stock["code"]}) 리서치 실패: {e}')
        return {**stock,
                'reason': '리서치 실패 — 데이터를 가져오지 못했습니다.',
                'catalyst_type': '확인된_뉴스_없음',
                'source_url': '', 'source_date': '',
                'confidence': 'low',
                'profile': cached_profile or '',
                'profile_is_new': False}

    ctype = data.get('catalyst_type')
    if ctype not in CATALYST_STYLE:
        ctype = '확인된_뉴스_없음'
    profile = (data.get('profile') or '').strip()
    conf = data.get('confidence')
    out = {
        **stock,
        'reason':         (data.get('reason') or '').strip() or '내용 없음',
        'catalyst_type':  ctype,
        'source_url':     (data.get('source_url') or '').strip(),
        'source_date':    (data.get('source_date') or '').strip(),
        'confidence':     conf if conf in ('high', 'medium', 'low') else 'low',
        'profile':        cached_profile or profile or '회사 정보 미확보',
        'profile_is_new': bool(need_profile and profile),
    }
    tag = '✓' if out['source_url'] else '·'
    print(f'  {tag} {stock["name"]}({stock["code"]}) {fmt_ret(stock["ret"])} '
          f'[{ctype}/{out["confidence"]}] {len(out["reason"])}자')
    return out


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
        if age <= PROFILE_TTL_DAYS:
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


# ── 2단계: 총평 ────────────────────────────────────────────────────────────────
def build_overview(client, market, date, idx_ctx, top, bot):
    def brief(rows):
        return '\n'.join(
            f"- {r['name']} {fmt_ret(r['ret'])} [{r['catalyst_type']}] {r['reason'][:110]}"
            for r in rows)

    prompt = f"""{date} {MARKET_NAME.get(market, market)} 증시 데일리 브리핑의 '총평'을 작성하라.

시장 지수: {idx_ctx}

[상승 TOP 10]
{brief(top)}

[하락 TOP 10]
{brief(bot)}

위 자료만 근거로, 오늘 시장을 관통하는 흐름을 불릿 3개로 정리하라.
- 각 불릿은 한 문장, 60~100자.
- 반복되는 섹터·테마 / 상승과 하락을 가른 축 / 투자자 관점 시사점 순서로.
- 자료에 없는 사실을 추가하지 마라. 여러 종목이 "확인된_뉴스_없음"이면 그 사실 자체를 언급하라.
- 불릿 기호 없이 각 줄에 문장만 출력하라(3줄)."""

    try:
        final = _stream_final(
            client,
            model=CLAUDE_MODEL,
            max_tokens=4000,
            thinking={'type': 'adaptive'},
            output_config={'effort': 'low'},
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = ''.join(b.text for b in final.content if b.type == 'text')
        lines = [re.sub(r'^\s*[-•*]\s*|^\s*\d+[.)]\s*', '', ln).strip()
                 for ln in text.strip().splitlines() if ln.strip()]
        return [ln for ln in lines if ln][:3] or ['총평을 생성하지 못했습니다.']
    except Exception as e:
        print(f'  [WARN] 총평 생성 실패: {e}')
        return ['총평을 생성하지 못했습니다.']


# ── 3단계: HTML 렌더링 (결정론적 템플릿) ────────────────────────────────────────
E = html.escape


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
            f'<div style="padding:6px 12px;border-radius:8px;border:1.5px solid {border};'
            f'background:{bg};flex:1;min-width:0;display:flex;flex-direction:column;gap:2px">'
            f'<span style="font-size:9pt;color:#64748b;font-weight:600">{E(str(idx.get("name", k)))}</span>'
            f'<div style="display:flex;align-items:baseline;gap:6px;flex-wrap:nowrap">'
            f'<span style="font-size:13pt;font-weight:800;color:#1e293b;white-space:nowrap">'
            f'{fmt_idx_val(idx.get("value"))}</span>'
            f'<span style="font-size:9pt;font-weight:800;color:{color};white-space:nowrap">'
            f'{arrow} {sign}{chg:.2f} ({sign}{pct:.2f}%)</span>'
            f'</div></div>'
        )
    if not cards:
        return '<div style="font-size:8pt;color:#94a3b8;margin-bottom:8px">지수 데이터 없음</div>'
    return ('<div style="display:flex;gap:8px;flex-wrap:nowrap;margin-bottom:10px">'
            + ''.join(cards) + '</div>')


def render_badge(ctype, confidence):
    fg, bg = CATALYST_STYLE.get(ctype, CATALYST_STYLE['확인된_뉴스_없음'])
    label = '미확인' if ctype == '확인된_뉴스_없음' else ctype
    chip = (f'<span style="display:inline-block;padding:0 5px;border-radius:4px;'
            f'background:{bg};color:{fg};font-size:6.5pt;font-weight:800;'
            f'white-space:nowrap;vertical-align:1px">{E(label)}</span>')
    if confidence == 'low':
        chip += ('<span style="display:inline-block;margin-left:3px;padding:0 5px;'
                 'border-radius:4px;background:#f1f5f9;color:#64748b;font-size:6.5pt;'
                 'font-weight:700;vertical-align:1px">추정</span>')
    return chip


def render_table(rows, kind, footnotes, code_header):
    """kind: 'up' | 'down'"""
    up = kind == 'up'
    accent = '#16a34a' if up else '#dc2626'
    head = ['종목코드' if code_header == 'code' else 'Ticker', '종목명', '등락률', '회사 소개', '등락 배경']
    widths = ['8%', '13%', '7%', '30%', '42%']

    ths = ''.join(
        f'<th style="width:{w};background:#334155;color:#ffffff;font-size:7.5pt;'
        f'font-weight:700;padding:4px 5px;text-align:center;border:0">{E(h)}</th>'
        for h, w in zip(head, widths))

    trs = []
    for i, r in enumerate(rows):
        bg = '#f8fafc' if i % 2 else '#ffffff'
        note = ''
        if r.get('source_url'):
            footnotes.append((r['name'], r['source_url'], r.get('source_date', '')))
            note = (f'<sup style="color:#2563eb;font-weight:700;font-size:6pt">'
                    f'[{len(footnotes)}]</sup>')
        dim = ' color:#64748b;font-style:italic;' if r.get('confidence') == 'low' else ''
        td = (f'padding:4px 5px;border-bottom:1px solid #e2e8f0;font-size:7.5pt;'
              f'vertical-align:top;background:{bg};')
        trs.append(
            '<tr>'
            f'<td style="{td}text-align:center;font-family:Consolas,monospace;font-size:7pt;'
            f'color:#475569">{E(str(r["code"]))}</td>'
            f'<td style="{td}font-weight:700;color:#1e293b">{E(str(r["name"]))}</td>'
            f'<td style="{td}text-align:right;font-weight:800;color:{accent};white-space:nowrap">'
            f'{fmt_ret(r["ret"])}</td>'
            f'<td style="{td}color:#334155;line-height:1.45">{E(r.get("profile", ""))}</td>'
            f'<td style="{td}line-height:1.45;{dim}">'
            f'{render_badge(r.get("catalyst_type", ""), r.get("confidence", ""))} '
            f'{E(r.get("reason", ""))}{note}</td>'
            '</tr>')

    title = '▲ 상승 TOP 10' if up else '▼ 하락 TOP 10'
    return (
        '<div style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-bottom:12px">'
        f'<div style="background:#1e3a5f;color:#ffffff;font-size:9pt;font-weight:700;'
        f'padding:5px 10px;border-left:5px solid {accent}">{title}</div>'
        '<table style="width:100%;border-collapse:collapse;table-layout:fixed">'
        f'<thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table></div>'
    )


def render_footnotes(footnotes):
    if not footnotes:
        return ''
    items = ''.join(
        f'<div style="margin-bottom:1px">[{i}] {E(str(name))}'
        + (f' · {E(str(date))}' if date else '')
        + f' — <span style="color:#2563eb;word-break:break-all">{E(str(url)[:110])}</span></div>'
        for i, (name, url, date) in enumerate(footnotes, 1))
    return ('<div style="border-top:1px solid #e2e8f0;padding-top:6px;margin-top:6px;'
            'font-size:5.8pt;color:#94a3b8;line-height:1.5">'
            '<div style="font-weight:700;color:#64748b;margin-bottom:3px">출처</div>'
            f'{items}</div>')


def render_html(market, date, idx_data, overview, top, bot):
    market_name = MARKET_NAME.get(market, market)
    code_header = 'code' if market == 'kr' else 'ticker'
    footnotes = []
    tbl_up   = render_table(top, 'up', footnotes, code_header)
    tbl_down = render_table(bot, 'down', footnotes, code_header)

    ov = ''.join(f'<div style="margin-bottom:3px">• {E(line)}</div>' for line in overview)

    total = len(top) + len(bot)
    unknown = sum(1 for r in top + bot if r.get('catalyst_type') == '확인된_뉴스_없음')
    quality = (f'<span style="color:#94a3b8;font-size:7pt">'
               f'개별 재료 확인 {total - unknown}/{total}종목</span>')

    head_block = (
        f'<div style="font-size:11pt;font-weight:800;color:#1e293b;margin-bottom:3px">'
        f'데일리 마켓 브리핑 · {E(market_name)} 증시</div>'
        f'<div style="display:flex;justify-content:space-between;align-items:baseline;'
        f'font-size:8pt;color:#64748b;margin-bottom:10px">'
        f'<span>기준일 {E(date)} · 기준 기간 1D · 시장 {E(market_name)}</span>'
        f'<span>🕒 생성 __GEN_TIME__ KST</span></div>'
    )

    return f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="utf-8">
<title>데일리 마켓 브리핑 · {E(market_name)} 증시 · {E(date)}</title>
<style>
  @page {{ size: A4 portrait; margin: 10mm; }}
  body {{ margin:0; padding:16px 0; background:#e2e8f0;
          font-family:'Noto Sans KR','Malgun Gothic',sans-serif; color:#1e293b; }}
  .page {{ width:210mm; min-height:297mm; margin:0 auto 8mm; padding:10mm;
           background:#fff; box-shadow:0 2px 12px rgba(0,0,0,.15); box-sizing:border-box; }}
  table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
  /* 등락 배경이 길어지면 표가 한 장을 넘길 수 있다. 페이지가 넘어가도
     헤더는 반복되고 행은 중간에 잘리지 않게 한다. */
  thead {{ display:table-header-group; }}
  tr    {{ page-break-inside:avoid; break-inside:avoid; }}
  @media print {{
    body {{ -webkit-print-color-adjust:exact; print-color-adjust:exact;
            background:#fff; padding:0; }}
    .page {{ box-shadow:none; margin:0; min-height:auto; padding:0;
             page-break-after:always; }}
    .page:last-child {{ page-break-after:auto; }}
  }}
</style></head>
<body>
<div class="page">
  {head_block}
  <div style="border:1px solid #e2e8f0;border-radius:8px;padding:8px 10px;margin-bottom:12px">
    <div style="font-size:9pt;font-weight:700;color:#1e3a5f;margin-bottom:6px">시장 개요</div>
    {render_idx_cards(market, idx_data)}
    <div style="background:#f8fafc;border-radius:6px;padding:7px 9px;font-size:8pt;
                line-height:1.6;color:#334155">{ov}</div>
  </div>
  {tbl_up}
</div>
<div class="page">
  {head_block}
  {tbl_down}
  <div style="display:flex;justify-content:flex-end;margin-bottom:4px">{quality}</div>
  {render_footnotes(footnotes)}
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
    no_news = ('개별 공시·뉴스 미확인. 지수가 KOSPI +0.84%인 가운데 초과수익 +11.2%p로 '
               '나타나, 업종 순환매 또는 수급 요인에 따른 변동으로 추정된다.')
    top = []
    for i in range(10):
        top.append({
            'code': f'00593{i}', 'name': f'테스트종목{i}', 'ret': 0.12 - i * 0.008,
            'ret5': 0.2, 'mcap': 5e12, 'cur': 'KRW',
            'profile': '메모리 반도체와 파운드리를 생산해 글로벌 IT 제조사에 공급하는 종합 반도체 기업이다.',
            'reason': no_news if i % 3 == 1 else long_reason,
            'catalyst_type': ['실적', '확인된_뉴스_없음', '수주·계약'][i % 3],
            'source_url': '' if i % 3 == 1 else f'https://example.com/news/article-{i}-long-path',
            'source_date': '2026-07-27',
            'confidence': ['high', 'low', 'medium'][i % 3],
        })
    bot = [dict(s, ret=-abs(s['ret']), code=f'01234{i}', name=f'하락종목{i}')
           for i, s in enumerate(top)]

    doc = render_html('kr', '2026-07-28', idx_data,
                      ['반도체 업종이 실적 서프라이즈를 주도하며 지수 상승을 이끌었다.',
                       '중소형 개별주는 뚜렷한 재료 없이 수급으로 움직인 사례가 다수였다.',
                       '실적이 확인된 종목과 미확인 종목의 수익률 격차가 벌어지는 국면이다.'],
                      top, bot)
    doc = inject_timestamp(doc, '2026-07-28 16:30')

    assert doc.startswith('<!DOCTYPE html>'), 'DOCTYPE 누락'
    assert doc.rstrip().endswith('</html>'), '</html> 누락'
    assert '__GEN_TIME__' not in doc, '타임스탬프 미치환'
    assert doc.count('<div class="page">') == 2, '페이지 수 불일치'
    assert '테스트종목0' in doc and '하락종목9' in doc, '행 누락'
    assert doc.count('<sup') == 14, f'각주 개수 이상: {doc.count("<sup")}'
    # 인쇄 시 표가 여러 장으로 넘어가도 깨지지 않아야 한다
    assert 'display:table-header-group' in doc, '표 헤더 반복 규칙 누락'
    assert 'page-break-inside:avoid' in doc, '행 미분할 규칙 누락'
    # HTML 이스케이프 동작 확인
    assert '<script>' not in render_table(
        [{'code': 'X', 'name': '<script>alert(1)</script>', 'ret': 0.1,
          'profile': '<b>x</b>', 'reason': '<img onerror=1>',
          'catalyst_type': '실적', 'confidence': 'high', 'source_url': ''}],
        'up', [], 'code'), 'HTML 이스케이프 실패'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(doc)
    print(f'[self-test] OK — {len(doc):,} bytes, 페이지 2장, 각주 {doc.count("<sup")}건 → {out_path}')
    return doc


# ── 메인 ────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--market', choices=['kr', 'us', 'jp', 'cn'])
    ap.add_argument('--self-test', action='store_true', help='API 없이 HTML 렌더링만 검증')
    ap.add_argument('--out', default='review_selftest.html')
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
    top10, bot10 = get_top_bottom(stocks, prices, market)
    idx_data = get_idx_for_date(indices, date)
    idx_ctx, bench = index_context(market, idx_data)

    if not top10 and not bot10:
        print('[ERROR] 유효한 등락 종목이 없습니다.')
        sys.exit(1)

    enrich(top10, bench)
    enrich(bot10, bench)
    targets = top10 + bot10
    print(f'[{market}] 기준일 {date}  |  상승 {len(top10)} / 하락 {len(bot10)}  |  지수 {idx_ctx}')

    profiles = load_profiles(market, [s['code'] for s in targets])
    print(f'[{market}] 회사 소개 캐시 적중 {len(profiles)}/{len(targets)}종목')

    client = anthropic.Anthropic()

    print(f'[{market}] 1단계 — 종목별 리서치 (동시 {RESEARCH_WORKERS}개, 웹검색)...')
    with ThreadPoolExecutor(max_workers=RESEARCH_WORKERS) as pool:
        results = list(pool.map(
            lambda s: research_stock(client, market, date, s, idx_ctx, profiles.get(s['code'])),
            targets))

    top_r = results[:len(top10)]
    bot_r = results[len(top10):]

    found = sum(1 for r in results if r['catalyst_type'] != '확인된_뉴스_없음')
    print(f'[{market}] 리서치 완료 — 개별 재료 확인 {found}/{len(results)}종목')

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
          f'재료확인 {found}/{len(results)}  ·  {len(html_doc):,} bytes)')


if __name__ == '__main__':
    main()
