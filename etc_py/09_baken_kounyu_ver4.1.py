# -*- coding: utf-8 -*-
"""
JRA-IPAT 自動投票スクリプト  ver 3.9k  (Playwright)
────────────────────────────────────────────────────
【今回のピンポイント修正：最終確認の OK ボタンだけ確実に押す】
- 「購入する」クリック後に出る確認ダイアログ内の
  <button class="btn btn-default btn-lg btn-ok ng-binding" ng-click="vm.dismiss()">OK</button>
  を確実に押す専用関数 `click_final_ok_strict()` を新設・使用。
- クリック手順（強い順）:
  1) root(最前面ダイアログ)特定 → .btn-ok を wait_for(visible)
  2) .click() → ダメなら click(force=True)
  3) focus → Enter
  4) JS で直接 e.click()
  5) それでも閉じなければネイティブ dialog を accept
- 併せて NameError 対策で read_purchase_limit / wait_limit_decreased を確実に定義。

※ ログイン〜購入ボタン押下までの流れは既存方針を維持。
依存: pip install playwright python-dotenv pandas openpyxl
初回: python -m playwright install
"""
import asyncio, os, sys, pathlib, time, re
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import (
    async_playwright, TimeoutError, Page, Browser
)
import sys

    # 標準出力・標準エラーを UTF-8 にする（Python3.7+）
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
# ========= .env 読込 =========
load_dotenv()
INET_ID  = os.getenv("INET_ID",  "")
USER_ID  = os.getenv("USER_ID",  "")
PASSWORD = os.getenv("PASSWORD", "")
P_ARS    = os.getenv("P_ARS",    "")

# ========= 定数 =========
LOGIN_URL  = "https://www.ipat.jra.go.jp/"
SLOW_MO_MS = 40
RETRY_MAX  = 1                  # 失敗後の再挑戦回数
RACEDAY   = "251206"            # ←適宜変更（YYMMDD）
SRC_EXCEL = "購入対象.xlsx"
SHEET     = RACEDAY
TOTAL_AMOUNT = 1000             # 合計金額（円）

ERRSHOT = pathlib.Path("errshot_img"); ERRSHOT.mkdir(exist_ok=True)
ERRDOM  = pathlib.Path("errshot_dom"); ERRDOM.mkdir(exist_ok=True)

KEIBAJO = {
    "01":"札幌","02":"函館","03":"福島","04":"新潟","05":"東京",
    "06":"中山","07":"中京","08":"京都","09":"阪神","10":"小倉"
}

# ========== 画面ズーム 50 % ==========
async def set_zoom_50(page: Page):
    try:
        await page.evaluate("document.body.style.zoom='50%'")
    except Exception:
        pass

# ========== ユーティリティ ==========
def jp_week(date:str)->str:
    return "月火水木金土日"[datetime.strptime(date,"%Y%m%d").weekday()]

async def js_fill(page:Page, sel:str, val:str):
    """Angularに伝わるよう input/change を発火させて値を入れる"""
    await page.evaluate("""([s,v])=>{
        const e=document.querySelector(s);
        if(!e)return;
        e.value=v;
        e.dispatchEvent(new Event('input',{bubbles:true}));
        e.dispatchEvent(new Event('change',{bubbles:true}));
        e.blur && e.blur();
    }""",[sel,val])

async def shot(page:Page, lbl:str):
    fn=ERRSHOT/f"{lbl}_{datetime.now():%H%M%S}.png"
    try:
        await page.screenshot(path=str(fn))
    except Exception:
        pass
    return fn

async def dump_dom(page:Page, lbl:str):
    fn=ERRDOM/f"{lbl}_{datetime.now():%H%M%S}.html"
    try:
        fn.write_text(await page.content(),encoding="utf-8")
    except Exception:
        pass
    return fn

async def safe_shot(page:Page, lbl:str):
    try:
        if page.is_closed(): return None
        return await shot(page,lbl)
    except Exception:
        return None

async def safe_dump_dom(page:Page, lbl:str):
    try:
        if page.is_closed(): return None
        return await dump_dom(page,lbl)
    except Exception:
        return None

# ========== 合計金額入力 → 行金額チェック（必要時のみ補正）→ 購入 ==========
async def ensure_total_and_rows_then_buy(page: Page, total: int = TOTAL_AMOUNT, timeout_ms: int = 8000) -> bool:
    """
    1) 下部「合計金額入力」に total を確実に入れる
    2) 行金額(item.unit)が0円行を残さない（必要時のみ一括/予算セット→直書き補正）
    3) 「購入する」enabled待ち → クリック
    """
    modal = page.locator('.ipat-vote-list').first
    try:
        await modal.wait_for(state="visible", timeout=timeout_ms)
    except Exception:
        print("  └─モーダル(.ipat-vote-list)が見つかりません")
        return False

    # 1) 合計金額入力
    total_input = modal.locator(
        'input[ng-model="vm.cAmountTotal"]:not([disabled]), input[name="amountTotal"]:not([disabled])'
    )
    try:
        await total_input.first.wait_for(state="visible", timeout=timeout_ms)
        try:
            await total_input.first.fill(str(total))
        except Exception:
            await js_fill(page,
                          '.ipat-vote-list input[ng-model="vm.cAmountTotal"]:not([disabled]), '
                          '.ipat-vote-list input[name="amountTotal"]:not([disabled])',
                          str(total))
        await page.keyboard.press("Tab")
        await page.wait_for_timeout(150)
    except Exception:
        print("  └─合計金額入力の操作に失敗")
        return False

    # 書けているか検証（Angular差し替え対策で再試行あり）
    v = await page.evaluate(
        """()=>{const el=document.querySelector('.ipat-vote-list input[ng-model="vm.cAmountTotal"], .ipat-vote-list input[name="amountTotal"]');
                 if(!el) return null;
                 return Number(String(el.value||'').replace(/[^\\d]/g,''));}"""
    )
    if not v:
        try:
            await total_input.first.fill(str(total))
            await page.keyboard.press("Tab")
            await page.wait_for_timeout(150)
        except Exception:
            await js_fill(page,
                          '.ipat-vote-list input[ng-model="vm.cAmountTotal"], .ipat-vote-list input[name="amountTotal"]',
                          str(total))
        v = await page.evaluate("""()=>Number(String((document.querySelector('.ipat-vote-list input[ng-model="vm.cAmountTotal"], .ipat-vote-list input[name="amountTotal"]')||{}).value||'').replace(/[^\\d]/g,''))""")
        if not v:
            print("  └─合計金額入力へのセット失敗（値が空）")
            return False

    # 2) 行金額（必要時のみ補正）
    async def read_row_state():
        return await page.evaluate("""
        () => {
          const inputs = Array.from(document.querySelectorAll('.ipat-vote-list input[ng-model="item.unit"]'));
          const vals   = inputs.map(e => Number(String(e.value||'').replace(/[^\\d]/g,''))||0);
          return {n: inputs.length, vals, all_pos: (inputs.length>0 && vals.every(v=>v>0))};
        }
        """)
    state = await read_row_state()
    if not state["all_pos"]:
        # 一括セット → 予算セット
        async def try_click(label:str):
            btn = modal.locator(f'button:has-text("{label}"):not([disabled])').first
            if await btn.count():
                try:
                    await btn.wait_for(state="visible", timeout=1200)
                    await btn.click()
                    await page.wait_for_timeout(150)
                    return True
                except Exception:
                    pass
            return False
        if not await try_click("一括セット"):
            await try_click("予算セット")

        state = await read_row_state()
        # まだダメなら 100 円を直書き
        if not state["all_pos"]:
            await page.evaluate("""
            ()=> {
              const inputs = Array.from(document.querySelectorAll('.ipat-vote-list input[ng-model="item.unit"]'));
              for (const e of inputs) {
                e.value = '100';
                e.dispatchEvent(new Event('input',{bubbles:true}));
                e.dispatchEvent(new Event('change',{bubbles:true}));
              }
            }
            """)
            await page.wait_for_timeout(150)
            state = await read_row_state()
        if not state["all_pos"]:
            print(f"  └─行金額の補正に失敗 vals={state['vals']}")
            await safe_dump_dom(page,"row_units_not_applied")
            return False

    # 3) 購入する
    buy = modal.get_by_role("button", name="購入する").first
    try:
        await buy.wait_for(state="visible", timeout=timeout_ms)
        for _ in range(50):  # ～10秒
            if await buy.is_enabled():
                break
            await page.wait_for_timeout(200)
        await buy.click(force=True)
        return True
    except Exception:
        print("  └─購入ボタン検出/クリックに失敗")
        await safe_dump_dom(page,"buy_click_failed")
        return False

# ========== 購入限度額（判定補助：定義抜け対策で明示残置） ==========
async def read_purchase_limit(page: Page) -> Optional[int]:
    script = r"""
    () => {
      const rows = Array.from(document.querySelectorAll('tr'));
      for (const tr of rows) {
        const txt = (tr.textContent || '').replace(/\s+/g,'');
        if (/購入限度額/.test(txt)) {
          const td = tr.querySelector('td.text-right') || tr.querySelector('td:last-child');
          if (!td) return null;
          const v = (td.textContent || '').replace(/[^\d]/g, '');
          return v ? parseInt(v, 10) : null;
        }
      }
      return null;
    }
    """
    try:
        val = await page.evaluate(script); return int(val) if val is not None else None
    except Exception:
        return None

async def wait_limit_decreased(page: Page, before: int, delta: int, timeout_ms: int = 12000) -> bool:
    func = r"""
    ([before, delta]) => {
      const rows = Array.from(document.querySelectorAll('tr'));
      let current = null;
      for (const tr of rows) {
        const txt = (tr.textContent || '').replace(/\s+/g,'');
        if (/購入限度額/.test(txt)) {
          const td = tr.querySelector('td.text-right') || tr.querySelector('td:last-child');
          if (!td) return false;
          const v = (td.textContent || '').replace(/[^\d]/g, '');
          current = v ? parseInt(v, 10) : null;
          break;
        }
      }
      return (current !== null) && (current <= before - delta);
    }
    """
    try:
        await page.wait_for_function(func, arg=[before, delta], timeout=timeout_ms)
        return True
    except Exception:
        return False

# ========== ★最終確認(HTML)の OK を“確実に”押す（今回の修正の核） ==========
async def click_final_ok_strict(page: Page, timeout_ms: int = 5000) -> bool:
    """
    直近の可視ダイアログから .btn-ok(OK) を押して閉じるまで待つ。
    スクショ/提供HTMLの構造：
      <div class="dialog ..."><div class="dialog-footer"> <button class="btn ... btn-ok">OK</button> ...
    """
    # 直近(最前面)の可視ダイアログを取得
    root = page.locator(
        '.ipat-error-window[aria-hidden="false"] .dialog[role="dialog"], '
        '.dialog[role="dialog"]:visible, .dialog:visible, [role="dialog"]:visible'
    ).last
    try:
        await root.wait_for(state="visible", timeout=timeout_ms)
    except Exception:
        # 文言フォロー（ご確認 / 投票内容と金額を送信…）
        for p in ("ご確認", "送信してよろしいですか", "投票内容", "OK"):
            try:
                node = page.locator(f"text={p}").last
                await node.wait_for(state="visible", timeout=800)
                root = node.locator("xpath=ancestor::*[self::div or self::section or self::*[@role='dialog']][1]")
                break
            except Exception:
                continue
        else:
            return False

    # OK ボタン候補（優先順）
    selectors = [
        '.dialog-footer button.btn-ok:not([disabled])',
        'button.btn.btn-ok:not([disabled])',
        'button:has-text("OK"):not([disabled])',
        'button:has-text("ＯＫ"):not([disabled])',
        'button:has-text("はい"):not([disabled])',
    ]

    for sel in selectors:
        btn = root.locator(sel).first
        try:
            if not await btn.count():
                continue
            await btn.wait_for(state="visible", timeout=1500)

            # 有効化待ち（ng-disabled解除）
            for _ in range(30):
                if await btn.is_enabled():
                    break
                await page.wait_for_timeout(100)

            # 1) click → 2) force → 3) Enter → 4) JS click の順で試行
            try:
                await btn.click()
            except Exception:
                try:
                    await btn.click(force=True)
                except Exception:
                    try:
                        await btn.focus()
                        await page.keyboard.press("Enter")
                    except Exception:
                        try:
                            el = await btn.element_handle()
                            if el:
                                await page.evaluate("(e)=>e.click()", el)
                        except Exception:
                            pass

            # ダイアログが閉じれば成功
            try:
                await root.wait_for(state="hidden", timeout=4000)
            except Exception:
                pass
            if not await root.is_visible():
                print("  └─確認モーダル(HTML): OKクリック確定")
                return True
        except Exception:
            continue

    # 最終フォールバック：画面全体で Enter
    try:
        await page.keyboard.press("Enter")
        print("  └─確認モーダル(HTML): Enter 送信（最終フォールバック）")
        return True
    except Exception:
        return False

# ========== Excel 読込 ==========
def load_bets()->List[dict]:
    df=pd.read_excel(SRC_EXCEL, sheet_name=SHEET, engine="openpyxl")
    rid_col=5
    horse_cols=list(range(11,16))
    df=df.dropna(subset=[df.columns[rid_col]])
    df["rid_str"]=df.iloc[:,rid_col].apply(lambda x:str(int(float(x))).zfill(12))
    bets=[]
    for _,r in df.iterrows():
        horses=[int(x) for x in r.iloc[horse_cols].dropna()]
        bets.append({"rid":r["rid_str"],"horses":horses})
    return bets

# ========== ログイン ==========
async def ipat_login(page:Page):
    await set_zoom_50(page)
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=20000)
    await set_zoom_50(page)
    await js_fill(page,'input[name="inetid"]', INET_ID)
    await page.locator('a[onclick*="send()"]').click()
    await page.wait_for_url("**/pw_080_i.cgi",timeout=15000)
    await js_fill(page,'input[name="i"]', USER_ID)
    await js_fill(page,'input[name="p"]', PASSWORD)
    await js_fill(page,'input[name="r"]', P_ARS)
    await page.locator('a[onclick*="ToModernMenu()"]').click()
    await page.wait_for_url("**/pw_890_i.cgi",timeout=15000)
    await page.wait_for_selector('button[ui-sref="bet.basic"]',timeout=15000)
    print("✅ ログイン成功")

# ========== 馬番チェック ==========
async def ensure_check(page:Page, num:int)->bool:
    in_sel=f'input#no{num}'
    lb_sel=f'label[for="no{num}"]'
    tgt=page.locator(lb_sel) if await page.locator(lb_sel).count() else page.locator(in_sel)
    for i in range(3):
        try:
            await tgt.scroll_into_view_if_needed(timeout=1500)
            await tgt.click(force=True,timeout=2500)
            await page.wait_for_function(f'document.querySelector("{in_sel}")?.checked===true',timeout=1200)
            return True
        except Exception:
            if i==2:
                try:
                    ok = await page.evaluate("""(sel)=>{
                        const e=document.querySelector(sel);
                        if(!e) return false;
                        e.checked=true;
                        e.dispatchEvent(new Event('change',{bubbles:true}));
                        return e.checked===true;
                    }""", in_sel)
                    if ok: return True
                except Exception:
                    pass
                print(f"   馬番 {num} クリック失敗")
                await safe_shot(page,f"uma{num}")
                return False
    return False

# ========== 結果モーダル ==========
SUCCESS_KEYWORDS = ("投票を受け付けました", "購入を受け付けました", "受付番号")
ERROR_KEYWORDS   = ("購入できません", "エラー", "時間外", "締切", "有効期限切れ", "ログイン")

async def wait_result_modal(page: Page, timeout_ms: int = 10000) -> Tuple[str, str]:
    modal = page.locator('.modal, .modal-dialog, .ipat-modal, .ngdialog, .ui-dialog, [role="dialog"]')
    body  = modal.locator('.modal-body, .ngdialog-content, .body, .content, .ui-dialog-content, [role="document"]')
    try:
        await modal.first.wait_for(state="visible", timeout=timeout_ms)
        try:
            txt = (await body.first.inner_text()).strip()
        except Exception:
            txt = (await modal.first.inner_text()).strip()
        tnorm = txt.replace("\n"," ").replace("\r"," ")
        if any(k in tnorm for k in SUCCESS_KEYWORDS): return ("success", tnorm)
        if any(k in tnorm for k in ERROR_KEYWORDS):   return ("error", tnorm)
        return ("unknown", tnorm)
    except TimeoutError:
        return ("timeout", "")

# ========== 1レース購入 ==========
async def vote_one(page:Page,rid:str,horses:List[int])->bool:
    if not horses:
        print("  └─SKIP: 馬番なし（購入処理なし）")
        return False

    success=False
    try:
        date,plc=rid[:8],rid[4:6]; race_no=f"{int(rid[-2:])}R"; plc_nm=KEIBAJO.get(plc,"??")
        print(f"\n▶ {plc_nm}({jp_week(date)}) {race_no}  馬番={horses}")

        if not await page.locator('button[ui-sref="bet.basic"]').count():
            await ipat_login(page)

        await set_zoom_50(page)
        await page.locator('button[ui-sref="bet.basic"]').click()
        await page.wait_for_selector('button:has-text("R")',timeout=8000)
        await page.locator(f'button:has-text("{plc_nm}")').first.click()

        race_btn=page.locator(f'button:has-text("{race_no}")').first
        if await race_btn.is_disabled():
            print("  └─SKIP: 締切（購入処理なし）")
            return False
        await race_btn.click(); await set_zoom_50(page)

        await page.wait_for_selector('#bet-basic-type',timeout=12000)
        await page.select_option('#bet-basic-type',label='３連複')
        await page.select_option('#bet-basic-method',label='ボックス')

        for n in horses:
            if not await ensure_check(page,n):
                await safe_dump_dom(page,"check_fail"); return False

        await page.wait_for_function(
            "()=>Number(document.querySelector('div.selection-match strong')?.innerText||0)===10",
            timeout=3500)

        # 単価→『セット』（従来据え置き）
        await js_fill(page,'div.selection-amount input[ng-model="vm.nUnit"]',"1")
        await page.locator(
            'div.selection-buttons button:has-text("セット"):not([disabled])'
        ).first.click()

        # 入力終了 → モーダル → 合計/行金額チェック → 購入
        await page.get_by_role("button",name="入力終了").click()
        await page.wait_for_selector('.ipat-vote-list',timeout=6000)
        await set_zoom_50(page)
        ok = await ensure_total_and_rows_then_buy(page, total=TOTAL_AMOUNT, timeout_ms=8000)
        if not ok:
            print("  └─失敗: 合計/行金額の検証→購入ボタン押下に失敗")
            return False

        # 送信前の購入限度額（判定補助）
        limit_before = await read_purchase_limit(page)

        # ★ 最終確認 OK（今回の修正点）
        html_ok = await click_final_ok_strict(page, timeout_ms=5000)
        if not html_ok:
            try:
                dlg = await page.wait_for_event("dialog", timeout=2500)
                await dlg.accept()
                print("  └─確認ダイアログ(ネイティブ/compat): OK")
            except Exception:
                print("  └─確認ダイアログ: 表示なし（スキップ）")

        # 送信中オーバレイ → 結果
        try:
            loading = page.locator('.ipat-loading').first
            await loading.wait_for(state="visible", timeout=4000)
            print("  └─送信中オーバレイ表示")
            await loading.wait_for(state="hidden", timeout=15000)
        except Exception:
            pass

        label, txt = await wait_result_modal(page, timeout_ms=6000)
        if label == "success":
            print("  └─ 購入完了(結果モーダル):", (txt or "")[:120], "…")
            success=True
        elif label == "error":
            print("  └─エラー(結果モーダル):", (txt or "")[:160], "…")
            success=False
        else:
            if limit_before is not None:
                ok2 = await wait_limit_decreased(page, limit_before, TOTAL_AMOUNT, timeout_ms=12000)
                if ok2:
                    after_limit = await read_purchase_limit(page)
                    print(f"  └─購入完了(限度額差分): {limit_before:,} → {after_limit:,}")
                    success=True
                else:
                    print("  └─結果モーダル未検出＆限度額も減少せず")
                    await safe_dump_dom(page,"after_buy_state")
                    success=False

        # 結果OK閉じ（あれば）
        if success:
            try:
                await page.locator('button.btn-ok:has-text("OK"), button:has-text("OK")').first.click()
            except Exception:
                pass

    except Exception as e:
        print("  └─例外:",e)
        await safe_shot(page,"fatal"); await safe_dump_dom(page,"fatal")
        success=False

    return success

# ========== 全レース購入 ==========
async def vote_all():
    async with async_playwright() as pw:
        browser:Browser=await pw.chromium.launch(
            headless=False, slow_mo=SLOW_MO_MS,
            args=["--start-maximized"])
        ctx=await browser.new_context(viewport=None, locale="ja-JP")
        page=await ctx.new_page()

        await ipat_login(page)

        for b in load_bets():
            attempts=0
            while attempts<=RETRY_MAX:
                success=False
                try:
                    success=await vote_one(page,b["rid"],b["horses"])
                except Exception as e:
                    print("  └─vote_one 例外:",e)
                    success=False

                if success:
                    break
                attempts+=1
                if attempts<=RETRY_MAX:
                    print(f"  ↺ リトライ {attempts}/{RETRY_MAX} … 再ログインで初期化")
                    try:
                        if page.is_closed():
                            page = await ctx.new_page()
                        await ipat_login(page)
                    except Exception as e:
                        print("    ログイン再試行失敗:",e)
                        await safe_shot(page,"relogin_fail")
                    await asyncio.sleep(1)

            if not success:
                print(f"  最終失敗: {b['rid']}  (最大リトライ到達)")

        await ctx.close(); await browser.close()

# ========== main ==========
if __name__=="__main__":
    try:
        asyncio.run(vote_all())
    except KeyboardInterrupt:
        print("中断",file=sys.stderr)
