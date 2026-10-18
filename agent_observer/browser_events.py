"""Browser metadata summaries. Never evaluate scripts or retain page contents."""
import json
import re
from urllib.parse import urlsplit


# Classify the browser API operation, independently of the visited service.
OPERATIONS = {
    'createBrowserTab': '페이지 열기', 'goto': '페이지 이동',
    'getTab': '탭 연결', 'reload': '새로고침', 'back': '뒤로 이동',
    'forward': '앞으로 이동', 'getAXState': '화면 읽기',
    'getAXStateAndScreenshot': '화면 읽기', 'domSnapshot': '화면 읽기',
    'getScreenshot': '화면 캡처', 'screenshot': '화면 캡처',
    'click': '클릭', 'dblclick': '클릭', 'scroll': '스크롤',
    'fill': '입력', 'setValue': '입력', 'typeText': '입력',
    'pressKey': '키 입력', 'getState': '브라우저 목록 확인',
    'listTabs': '탭 목록 확인', 'listBrowsers': '브라우저 목록 확인',
}


def browser_actions(code):
    if not isinstance(code, str):
        return []
    # Ignore literals/comments so page text and strings cannot invent operations.
    pattern = r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|`(?:\\.|[^`\\])*`|/\*[\s\S]*?\*/|//[^\n]*"
    code = re.sub(pattern, ' ', code)
    methods = re.findall(r'\.\s*([A-Za-z]+)\s*\(', code)
    return list(dict.fromkeys(OPERATIONS[m] for m in methods if m in OPERATIONS))


def browser_texts(result, depth=0):
    if depth > 3 or not isinstance(result, dict):
        return
    for block in result.get('content', []):
        if not isinstance(block, dict) or block.get('type') != 'text':
            continue
        text = block.get('text', '')
        if not isinstance(text, str):
            continue
        try:
            wrapped = json.loads(text)
        except (ValueError, TypeError):
            wrapped = None
        if isinstance(wrapped, dict) and isinstance(wrapped.get('content'), list):
            yield from browser_texts(wrapped, depth+1)
        else:
            yield text


def observed_sites(result):
    """Only use the browser tool's tab-header lines, never URLs in page bodies."""
    if not isinstance(result, dict):
        return []
    sites = []
    for text in browser_texts(result):
        # CUA returns a header followed by an accessibility tree. Read each header.
        for url in re.findall(r'^Browser tab: [^\n]+?, Title: [^\n]*?, URL: "([^"\n]+)"\.?$', text, re.M):
            try:
                parsed = urlsplit(url)
                host = parsed.hostname
            except ValueError:
                continue
            if parsed.scheme in ('http', 'https') and host and re.fullmatch(r'[A-Za-z0-9.-]{1,253}', host):
                if host not in sites:
                    sites.append(host)
    return sites[:3]


def browser_metadata(item):
    if item.get('server') != 'cua_repl' or item.get('tool') != 'js':
        return None
    args = item.get('arguments')
    actions = browser_actions(args.get('code')) if isinstance(args, dict) else []
    sites = observed_sites(item.get('result'))
    action_label = ' · '.join(actions[:3]) or '브라우저 작업'
    label = (' / '.join(sites) + ' · ' if sites else '') + action_label
    return {
        'label': label, 'toolName': 'cua_repl.js',
        'browserActions': actions, 'browserSites': sites,
        'evidence': '브라우저 호출 메서드 및 반환된 탭 헤더 기반 분류',
        'scope': '행동 분류는 추정이며 읽기·클릭의 성공이나 문서 이해를 뜻하지 않습니다. '
                 '페이지 제목·본문·URL 경로·입력값은 저장하지 않습니다.',
    }
