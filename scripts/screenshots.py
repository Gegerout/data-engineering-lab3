"""Снимки для отчета, выводы команд без заголовков и консоль RustFS с бакетом hh-lake."""
import html
import sys

from playwright.sync_api import sync_playwright

from common import LAB, env

CHROMIUM = '/opt/homebrew/bin/chromium'
CONSOLE = 'http://127.0.0.1:9011'
LOGS = ['01_deploy', '02_upload', '03_mapreduce', '04_duckdb', '05_verify', '06_map_failure',
        '09_combinatorics', '12_s3ls_final']
PROFILES = ['08_profile_mr_base', '08_profile_mr_seq_opt', '08_profile_mr_par', '08_profile_mr_opt',
            '08_profile_duckdb', '08_profile_s3_read']
SHOTS = LAB / 'screenshots'


def log_shot(page, name, text, width=760):
    doc = ('<!doctype html><html><meta charset="utf-8"><style>body{margin:0;background:#fff}'
           f'pre{{margin:0;padding:14px;width:{width}px;box-sizing:border-box;color:#111;'
           'font:13px/1.45 Menlo,monospace;white-space:pre-wrap;overflow-wrap:anywhere}</style>'
           f'<pre>{html.escape(text)}</pre></html>')
    page.set_content(doc)
    shown = page.inner_text('pre')
    if shown.strip() != text.strip():
        raise SystemExit(f'text mismatch: {name}')
    page.locator('pre').screenshot(path=str(SHOTS / f'{name}.png'))
    print(f'{name}.png saved, text equals log')


def console_shots(page):
    """Область снимка без шапки, где консоль показывает ключ доступа."""
    e = env()
    page.goto(CONSOLE, wait_until='networkidle')
    page.fill('#accessKey', e['RUSTFS_ACCESS_KEY'])
    page.fill('#secretKey', e['RUSTFS_SECRET_KEY'])
    page.click('button[type=submit]')
    page.wait_for_url('**/browser/**', timeout=15000)
    for name, path, height in [('console_hh_lake', [], 410), ('console_raw', ['raw/'], 340),
                               ('console_results_mapreduce', ['results/', 'mapreduce/'], 490)]:
        page.goto(CONSOLE + '/rustfs/console/browser/?bucket=hh-lake', wait_until='networkidle')
        for folder in path:
            page.get_by_text(folder, exact=True).click()
            page.wait_for_load_state('networkidle')
            page.wait_for_timeout(800)
        page.wait_for_timeout(1200)
        page.screenshot(path=str(SHOTS / f'{name}.png'), clip={'x': 262, 'y': 66, 'width': 1010, 'height': height})
        print(f'{name}.png saved')


def main():
    SHOTS.mkdir(exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=CHROMIUM, headless=True)
        page = b.new_page(viewport={'width': 1280, 'height': 900}, device_scale_factor=2, locale='ru-RU')
        if 'logs' in sys.argv[1:] or len(sys.argv) == 1:
            for name in LOGS:
                log_shot(page, name, (LAB / 'logs' / f'{name}.txt').read_text())
            text = '\n'.join((LAB / 'logs' / f'{n}.txt').read_text().rstrip('\n') for n in PROFILES) + '\n'
            log_shot(page, '08_profile', text, width=860)
        if 'console' in sys.argv[1:] or len(sys.argv) == 1:
            console_shots(page)
        b.close()


if __name__ == '__main__':
    main()
