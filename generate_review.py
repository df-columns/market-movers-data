# generate_review.py  ─  GitHub Actions에서 실행
# Firebase(/v1/{kr,us}) → 상승/하락 TOP10 계산 → Claude API(웹검색) 분석
# → HTML 리포트 생성 → Firebase(/reviews/{kr,us})에 게시/수정
#
# 필요 환경변수(GitHub Secrets):
#   FIREBASE_KEY        : Firebase 서비스 계정 JSON (기존 fetch 스크립트와 동일)
#   ANTHROPIC_API_KEY   : Claude API 키
#
# 사용법: python generate_review.py --market kr    (또는 us)

import os, sys, re, json, argparse
from datetime import datetime, timezone, timedelta

import firebase_admin
from firebase_admin import credentials, db as firebase_db
import anthropic

DATABASE_URL = 'https://market-movers-75461-default-rtdb.asia-southeast1.firebasedatabase.app/'
CLAUDE_MODEL = 'claude-opus-4-8'
KST = timezone(timedelta(hours=9))

# HTML의 시장별 지수 순서와 동일
IDX_ORDER = {
    'kr': ['kospi', 'kospi200', 'kosdaq'],
    'us': ['sp500', 'ndx100', 'dji30'],
    'jp': ['n225', 'topix'],
    'cn': ['csi300', 'hsi'],
}
MARKET_NAME = {'kr': '한국', 'us': '미국', 'jp': '일본', 'cn': '중국'}


# ── Firebase 초기화 ────────────────────────────────────────────────────────────
def init_firebase():
    cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY']))
    try:
        firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
    except ValueError:
        pass  # 이미 초기화됨


# ── 계산 헬퍼 (HTML의 calcRet / getTopBottom 1D 포팅) ───────────────────────────
def calc_ret_1d(prices, si):
    """di=0, off=1 : PRICES[0][si] / PRICES[1][si] - 1"""
    if len(prices) < 2:
        return None
    p1 = prices[0][si] if si < len(prices[0]) else 0
    p0 = prices[1][si] if si < len(prices[1]) else 0
    if not p1 or not p0:
        return None
    return p1 / p0 - 1


def get_top_bottom(stocks, prices):
    lst = []
    for i, s in enumerate(stocks):
        sr = calc_ret_1d(prices, i)
        if sr is None:
            continue
        lst.append({'code': s.get('c', ''), 'name': s.get('n', ''), 'ret': sr})
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


def get_idx_for_date(indices, date):
    """날짜별 지수 히스토리에서 date 이하 가장 가까운 날짜의 데이터 반환 (HTML getIdxForDate 포팅)"""
    if not indices:
        return None
    date_re = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    avail = sorted([d for d in indices.keys() if date_re.match(d)], reverse=True)
    if not avail:
        return None
    use = next((d for d in avail if d <= date), avail[0])
    return indices.get(use)


# ── 프롬프트 생성 (HTML의 generateClaudePrompt, 기준기간 1D 고정) ────────────────
def build_prompt(market, date, idx_data, top10, bot10):
    market_name = MARKET_NAME.get(market, market)

    index_section = ''
    idx_cards_instruction = ''

    if idx_data and len(idx_data):
        ordered_keys = [k for k in IDX_ORDER[market] if idx_data.get(k)]
        lines = []
        for k in ordered_keys:
            idx = idx_data[k]
            chg = idx['change']
            sign = '+' if chg >= 0 else ''
            arrow = '▲' if chg > 0 else '▼' if chg < 0 else '━'
            lines.append(
                f"  • {idx['name']}: {fmt_idx_val(idx['value'])}  {arrow} "
                f"{sign}{chg:.2f} ({sign}{idx['changePct']:.2f}%)"
            )
        index_section = f"\n시장 지수 ({date} 기준)\n" + "\n".join(lines) + "\n"

        card_parts = []
        for k in ordered_keys:
            idx = idx_data[k]
            chg = idx['change']
            sign = '+' if chg >= 0 else ''
            arrow = '▲' if chg > 0 else '▼'
            color = '#16a34a' if chg >= 0 else '#dc2626'
            bg = '#f0fdf4' if chg >= 0 else '#fff1f2'
            border = '#86efac' if chg >= 0 else '#fca5a5'
            card_parts.append(
                f'name="{idx["name"]}" value="{fmt_idx_val(idx["value"])}" '
                f'chg="{arrow} {sign}{chg:.2f} ({sign}{idx["changePct"]:.2f}%)" '
                f'color="{color}" bg="{bg}" border="{border}"'
            )
        cards_line = " | ".join(card_parts)

        idx_cards_instruction = (
            "• 아래 제공된 실제 지수 수치를 가로 한 줄 카드로 HTML에 직접 넣어줘 (별도 조회 불필요)\n"
            f"• 카드 정보: {cards_line}\n"
            "\n"
            "• [카드 HTML 구조 — 반드시 이 구조로, 카드 3개를 한 줄에 나란히]\n"
            '  <div style="display:flex;gap:8px;flex-wrap:nowrap;margin-bottom:10px">\n'
            "    카드마다:\n"
            '    <div style="padding:6px 12px;border-radius:8px;border:1.5px solid {border};background:{bg};flex:1;min-width:0;display:flex;flex-direction:column;gap:2px">\n'
            '      <span style="font-size:9pt;color:#64748b;font-weight:600">{name}</span>\n'
            '      <div style="display:flex;align-items:baseline;gap:6px;flex-wrap:nowrap">\n'
            '        <span style="font-size:13pt;font-weight:800;color:#1e293b;white-space:nowrap">{value}</span>\n'
            '        <span style="font-size:9pt;font-weight:800;color:{color};white-space:nowrap">{chg}</span>\n'
            "      </div>\n"
            "    </div>\n"
            "  </div>\n"
            "\n"
            "• 카드 3개는 반드시 한 줄(flex nowrap)에 배치, 줄바꿈 금지\n"
            "• 지수값은 13pt, 변동률은 9pt 볼드로 같은 줄에 나란히\n"
            "• 카드 아래 시장 흐름 설명 2~3줄 (8pt)"
        )
    else:
        idx_hint = {
            'us': 'S&P500, NASDAQ100, Dow30',
            'kr': '코스피, KOSPI 200, 코스닥',
            'jp': 'Nikkei 225, TOPIX',
            'cn': 'CSI 300, 항셍(Hang Seng)',
        }.get(market, '해당 시장 주요 지수')
        idx_cards_instruction = (
            f"• {idx_hint} 일간 등락률을 직접 조회해서 수치와 함께 가로 한 줄 카드로 표시\n"
            "• 카드: padding 6px 14px, 지수값 13pt, 변동률 9.5pt 볼드, 같은 줄에 나란히\n"
            "• 상승 bg #f0fdf4 / border #86efac / 글씨 #16a34a, 하락 bg #fff1f2 / border #fca5a5 / 글씨 #dc2626\n"
            "• 카드 아래 시장 흐름 설명 2~3줄"
        )

    col_header = (
        "| 종목코드 | 종목명 | 등락률 | 회사 소개 (2문장: ①핵심사업 ②주요고객·경쟁우위) | 등락 배경 |"
        if market == 'kr'
        else "| Ticker | 종목명 | 등락률 | 회사 소개 (2문장: ①핵심사업 ②주요고객·경쟁우위) | 등락 배경 |"
    )

    top_lines = "\n".join(f"{i + 1}. {s['code']} / {s['name']} / {fmt_ret(s['ret'])}" for i, s in enumerate(top10))
    bot_lines = "\n".join(f"{i + 1}. {s['code']} / {s['name']} / {fmt_ret(s['ret'])}" for i, s in enumerate(bot10))

    return f"""아래 데이터를 바탕으로 데일리 마켓 브리핑 HTML 리포트를 만들어줘.

━━━ 입력 데이터 ━━━
기준일: {date}  |  기준 기간: 1D  |  시장: {market_name}
{index_section}
▲ 상승 TOP 10 (1D 기준)
{top_lines}

▼ 하락 TOP 10 (1D 기준)
{bot_lines}

━━━ 출력 지시 ━━━

각 종목의 "회사 소개"와 "등락 배경"은 웹 검색으로 최신 정보를 확인해서 정확하게 작성해줘.
특히 "등락 배경"은 {date} 전후의 실제 뉴스·공시·이슈를 근거로 작성해줘.

반드시 아래 조건을 모두 지켜줘:
1. ★★★ 가장 중요 ★★★ 너는 파일을 만들 수 없고, 아티팩트도 만들 수 없어. 웹 검색이 모두 끝나면, 너의 응답 본문(텍스트)에 완성된 HTML 문서 전체를 직접 써야 해.
   • 응답은 반드시 `<!DOCTYPE html>` 로 시작해서 `</html>` 로 끝나야 해.
   • "파일을 만들었다", "생성했다", "완성했습니다" 같은 설명 문장을 절대 쓰지 마. 인사말·머리말·꼬리말·요약 없이 오직 HTML 코드만 출력해.
   • 코드블록(```)으로 감싸도 되고 안 감싸도 되지만, HTML 문서 전체가 응답 안에 실제로 들어 있어야 해.
   • 외부 CDN·외부 폰트 링크 없이 인라인 스타일만 사용. 그 자체로 더블클릭하면 바로 열리는 완성된 파일이어야 해.
2. 화면에서도, 인쇄할 때도 A4 세로(portrait) 1장 비율로 보이게 설계해줘.
   • 화면: 회색(#e2e8f0 등) 배경 가운데에, 가로 210mm · 세로 297mm 의 흰색 A4 용지 한 장이 놓인 것처럼 보이게 해줘. (페이지 컨테이너 width:210mm; min-height:297mm; margin:0 auto; padding:10mm; background:#fff; box-shadow 로 종이 느낌)
   • CSS에 반드시 아래 포함:
     @page {{ size: A4 portrait; margin: 10mm; }}
     @media print {{ body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; background:#fff; }} .page {{ box-shadow:none; margin:0; }} }}
3. 브라우저에서 Ctrl+P → "PDF로 저장"을 누르면 A4 세로 1장으로 깔끔하게 나와야 해.
   모든 내용(지수 카드·총평·상승/하락 표)이 이 A4 세로 한 장(210×297mm) 안에 반드시 들어가야 하고, 넘치면 font-size와 padding을 줄여서 1페이지 이내로 맞춰줘.

━━━ 섹션 순서 ━━━
① 시장 개요 (지수 카드 최상단)  ② 총평  ③ 상승 TOP 10  ④ 하락 TOP 10

━━━ 디자인 가이드 ━━━

전체 톤: 금융 리포트 스타일. 단정하고 읽기 쉽게. 색깔 과하지 않게.

[제목·메타 줄]
• 제목("데일리 마켓 브리핑 · {market_name} 증시") 바로 아래 메타 줄을 좌우 양끝 배치(display:flex; justify-content:space-between; align-items:baseline)
• 왼쪽: 기준일 {date} · 기준 기간 1D · 시장 {market_name} (기존 스타일 그대로, 회색 #64748b)
• 오른쪽: 왼쪽과 완전히 동일한 글씨크기·동일한 회색(#64748b)으로 "🕒 생성 __GEN_TIME__ KST" 텍스트만 표시. 배경상자·테두리·그림자 절대 금지. __GEN_TIME__ 은 그대로 두면 됨(자동 치환됨).

[레이아웃]
• 각 섹션은 흰 배경 + 연한 테두리(#e2e8f0)의 카드 박스로 구분
• 섹션 헤더는 네이비(#1e3a5f) 배경에 흰 글씨, 좌측 굵은 바(accent line) 포함
• 섹션 간 여백 12px 이상으로 답답하지 않게

[시장 개요 — 제목 바로 아래 최상단에 위치]
{idx_cards_instruction}

[총평]
• 연회색(#f8fafc) 박스 배경
• 핵심 테마·섹터 로테이션·시사점을 불릿(•) 2~3개로

[상승/하락 TOP 10 표]
• 표 헤더 행: 짙은 회색(#334155) 배경, 모든 헤더 셀 글씨 흰색(#ffffff), 가운데 정렬
• 짝수 행: 연회색(#f8fafc) 배경
• 상승 등락률: 초록 볼드, 하락 등락률: 빨강 볼드
• 회사 소개와 등락 배경 컬럼: 줄바꿈 허용, 글씨 7~8pt, 일반 검정 텍스트
• ★ 종목명 열에는 회사 이름만 써라. 앞에 순위 숫자(1, 2, 3...)나 순번을 절대 붙이지 마라 (입력 데이터의 번호는 순위 참고용일 뿐, 표에는 넣지 않는다)
• 종목코드(Ticker) 열: 가운데 정렬
• 컬럼 너비: 종목코드 8%, 종목명 12%, 등락률 7%, 회사소개 35%, 등락배경 38%

[폰트 & 크기]
• 한국어: 'Noto Sans KR', sans-serif
• 제목 11pt, 섹션헤더 9pt, 본문·표 7.5pt
• 인쇄 시 A4 1장에 딱 맞게 font-size와 padding 조정 (margin: 10mm)

[표 컬럼]
{col_header}"""


# ── HTML 추출 (HTML의 extractHtml + publishReview 폴백 포팅) ────────────────────
def extract_html(text):
    t = text.strip()
    m = re.search(r'```(?:html)?\s*([\s\S]*?)```', t, re.I)
    if m:
        inner = m.group(1).strip()
        if '<html' in inner or '<!DOCTYPE' in inner:
            return inner
    if t.startswith('<!DOCTYPE') or t.startswith('<html'):
        return t
    # 코드블록 없이 순수 HTML만 온 경우
    if '<' in t and ('<html' in t or '<!DOCTYPE' in t):
        idx = t.find('<!DOCTYPE')
        if idx < 0:
            idx = t.find('<html')
        return t[idx:].strip()
    return None


# ── Claude API 호출 (웹 검색 도구 사용) ─────────────────────────────────────────
SYSTEM_PROMPT = (
    "너는 HTML 마켓 리포트 생성기다. 웹 검색으로 사실을 확인한 뒤, "
    "너의 응답은 오직 하나의 완전한 HTML 문서여야 한다. "
    "너에게는 파일 시스템도 아티팩트 기능도 없다 — 파일을 만들 수 없다. "
    "'파일을 생성했다/완성했습니다' 같은 말은 절대 하지 말고, 어떤 설명 문장도 없이 "
    "응답 본문에 `<!DOCTYPE html>`로 시작해 `</html>`로 끝나는 HTML 코드 자체를 직접 출력하라."
)


def call_claude(prompt):
    client = anthropic.Anthropic()  # ANTHROPIC_API_KEY 환경변수 사용
    tools = [{"type": "web_search_20260209", "name": "web_search"}]
    messages = [{"role": "user", "content": prompt}]

    final = None
    for _ in range(6):  # pause_turn 대비 재개 루프
        with client.messages.stream(
            model=CLAUDE_MODEL,
            max_tokens=32000,
            system=SYSTEM_PROMPT,
            thinking={"type": "adaptive"},
            tools=tools,
            messages=messages,
        ) as stream:
            final = stream.get_final_message()

        if final.stop_reason == "pause_turn":
            messages.append({"role": "assistant", "content": final.content})
            continue
        break

    text = "".join(b.text for b in final.content if b.type == "text")
    return text


# ── 메인 ────────────────────────────────────────────────────────────────────────
def inject_timestamp(html, ts):
    """기준일 메타 줄 오른쪽 끝에 생성 완료 시각을 같은 텍스트 스타일로 표시"""
    # 1순위: 프롬프트가 심어둔 플레이스홀더를 실제 완료 시각으로 치환
    if '__GEN_TIME__' in html:
        return html.replace('__GEN_TIME__', ts)
    # 폴백: 플레이스홀더가 없으면 화면 우측 상단에 소박한 텍스트로
    fallback = (
        '<div style="position:fixed;top:6px;right:12px;z-index:50;font-size:8pt;'
        f'color:#64748b;font-family:sans-serif">🕒 생성 {ts} KST</div>'
    )
    m = re.search(r'<body[^>]*>', html, re.I)
    return (html[:m.end()] + fallback + html[m.end():]) if m else (fallback + html)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--market', required=True, choices=['kr', 'us', 'jp', 'cn'])
    args = ap.parse_args()
    market = args.market

    init_firebase()

    print(f'[{market}] Firebase에서 데이터 읽는 중...')
    data = firebase_db.reference(f'/v1/{market}').get()
    if not data:
        print(f'[ERROR] /v1/{market} 데이터가 없습니다.')
        sys.exit(1)

    stocks = data.get('stocks') or []
    prices = data.get('prices') or []
    dates = data.get('dates') or []
    indices = data.get('indices') or {}

    if not stocks or len(prices) < 2 or not dates:
        print('[ERROR] 무버 계산에 필요한 데이터(stocks/prices/dates)가 부족합니다.')
        sys.exit(1)

    date = dates[0]
    top10, bot10 = get_top_bottom(stocks, prices)
    idx_data = get_idx_for_date(indices, date)

    if not top10 and not bot10:
        print('[ERROR] 유효한 등락 종목이 없습니다.')
        sys.exit(1)

    print(f'[{market}] 기준일 {date}  |  상승 {len(top10)} / 하락 {len(bot10)}')
    prompt = build_prompt(market, date, idx_data, top10, bot10)

    print(f'[{market}] Claude 분석 중 (웹 검색 사용)...')
    raw = call_claude(prompt)

    html = extract_html(raw)
    if not html:
        print('[ERROR] Claude 응답에서 HTML을 추출하지 못했습니다.')
        print(f'--- 응답 길이: {len(raw):,}자 ---')
        print('--- 응답 앞 1000자 ---')
        print(raw[:1000])
        print('--- 응답 뒤 500자 ---')
        print(raw[-500:])
        sys.exit(1)

    ts = datetime.now(KST).strftime('%Y-%m-%d %H:%M')
    html = inject_timestamp(html, ts)   # 리포트 우측 상단에 생성 시각 표시
    payload = {
        'html': html,
        'updated_at': ts,
        'base_date': date,
    }

    print(f'[{market}] Firebase /reviews/{market} 에 게시 중...')
    firebase_db.reference(f'/reviews/{market}').set(payload)
    print(f'[{market}] 완료! (게시: {payload["updated_at"]}  ·  기준일 {date}  ·  {len(html):,} bytes)')


if __name__ == '__main__':
    main()
