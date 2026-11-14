#!/usr/bin/env python3
"""Real browser against Client API; upstream auth/aggregate fixtures are explicit."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
from playwright.sync_api import sync_playwright, expect
import re

ROOT=Path(__file__).resolve().parent.parent
def main():
    run=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dt%H%M%S')+'-'+uuid.uuid4().hex[:8]
    evidence=ROOT/'evidence'/'role1-audit-analytics'/run
    evidence.mkdir(parents=True)
    env=os.environ.copy();env['ANALYTICS_BROWSER_EVIDENCE']=str(evidence)
    log=(evidence/'server.log').open('wb')
    process=subprocess.Popen(['go','test','-mod=readonly','-count=1','-timeout=6m','-run','^TestAnalyticsBrowserHarness$','-v','.'],cwd=ROOT/'client-api',env=env,stdout=log,stderr=subprocess.STDOUT)
    result={'scope':'Actual Chromium/Client API; deterministic test auth and aggregate upstreams, not deployed auth/storage acceptance','checks':[]}
    def check(name,condition):
        result['checks'].append({'name':name,'status':'passed' if condition else 'failed'})
        if not condition:raise AssertionError(name)
    def mode(value):
        (evidence/'mode.txt').write_text(value)
        until=time.monotonic()+5
        while time.monotonic()<until:
            ack=evidence/'mode-ack.txt'
            if ack.exists() and ack.read_text()==value:return
            time.sleep(.05)
        raise RuntimeError('fixture mode not acknowledged: '+value)
    try:
        until=time.monotonic()+90
        url_file=evidence/'browser-url.txt'
        while not url_file.exists():
            if process.poll() is not None or time.monotonic()>until:raise RuntimeError('browser harness did not start')
            time.sleep(.1)
        with sync_playwright() as p:
            executable=os.environ.get('ANALYTICS_BROWSER_EXECUTABLE')
            if not executable:
                candidates=[Path('C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'),Path('C:/Program Files/Google/Chrome/Application/chrome.exe')]
                executable=next((str(v) for v in candidates if v.exists()),None)
            browser=p.chromium.launch(headless=True,**({'executable_path':executable} if executable else {}))
            result['browser_executable']=executable or 'Playwright bundled Chromium'
            result['browser_version']=browser.version
            page=browser.new_page(viewport={'width':1280,'height':1000})
            errors=[];requests=[]
            page.on('pageerror',lambda e:errors.append(str(e)))
            page.on('request',lambda r:requests.append(r.url) if '/analytics/summary?' in r.url else None)
            page.goto(url_file.read_text(),wait_until='networkidle')
            page.locator('#client-id').fill('fixture-client');page.locator('#client-secret').fill('fixture-secret')
            page.locator('#login button').click();expect(page.locator('#total')).to_have_text('5')
            check('existing client login and aggregates',page.locator('#results').is_visible())
            check('warning overlay and area table',page.locator('#tsunami').inner_text()=='1' and page.locator('#areas tbody tr').count()==3)
            page.locator('#from').fill('2026-01-01T00:00');page.locator('#to').fill('2026-01-02T00:00');page.locator('#interval').select_option('hour')
            page.locator('#refresh').click();expect(page.locator('#refresh')).to_be_enabled()
            check('UTC filter forwarding',any('interval=hour' in r and '2026-01-01' in r for r in requests))
            page.screenshot(path=str(evidence/'analytics-complete.png'),full_page=True)
            mode('partial')
            page.locator('#refresh').click();expect(page.locator('#total')).to_have_text('3')
            check('visible incomplete worker status','INCOMPLETE DATA' in page.locator('#availability').inner_text() and 'worker2' in page.locator('#availability').inner_text())
            page.screenshot(path=str(evidence/'analytics-partial.png'),full_page=True)
            mode('unavailable')
            page.locator('#refresh').click();expect(page.locator('#status')).to_have_text(re.compile('.*stale.*'))
            check('failed refresh retains labelled prior values',page.locator('#total').inner_text()=='3')
            page.set_viewport_size({'width':390,'height':844});page.screenshot(path=str(evidence/'analytics-mobile.png'),full_page=True)
            check('mobile layout fits viewport',page.evaluate('document.documentElement.scrollWidth<=window.innerWidth'))
            check('no persistent browser credentials',page.evaluate('localStorage.length===0 && sessionStorage.length===0'))
            check('no script errors',not errors)
            page.locator('#logout').click();expect(page.locator('#login-panel')).to_be_visible()
            check('logout removes displayed data',not page.locator('#results').is_visible())
            browser.close()
        result['browser']='Chromium';result['status']='passed'
    except Exception as exc:
        result['status']='failed';result['error']=str(exc)
        raise
    finally:
        (evidence/'mode.txt').write_text('done')
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.terminate();process.wait(timeout=10)
        log.close();(evidence/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
        print('Evidence: '+str(evidence),flush=True)
        print(json.dumps(result),flush=True)
if __name__=='__main__':main()
