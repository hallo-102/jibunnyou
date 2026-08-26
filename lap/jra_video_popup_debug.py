from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import (
    BrowserContext,
    ConsoleMessage,
    Page,
    Request,
    Response,
    sync_playwright,
)


# ============================================================
# 設定
# ============================================================

TARGET = "202602040601"

JRA_TOP_URL = "https://www.jra.go.jp/"

BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = BASE_DIR / "output" / TARGET / "popup_debug"

PROFILE_DIR = BASE_DIR / ".chrome_jra_profile"

EVENT_LOG_FILE = OUTPUT_DIR / "events.jsonl"

SUMMARY_FILE = OUTPUT_DIR / "summary.json"

PLAY_ELEMENTS_FILE = OUTPUT_DIR / "play_elements.txt"

PAGE_HTML_FILE = OUTPUT_DIR / "page_before_play.html"

SCREENSHOT_DIR = OUTPUT_DIR / "screenshots"


# Google Chromeを画面表示
HEADLESS = False


# Enter後に追加監視する秒数
AFTER_ENTER_WAIT_SECONDS = 15


# ============================================================
# 共通
# ============================================================

def now_string() -> str:
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]


def prepare_directories() -> None:

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    SCREENSHOT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    PROFILE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def write_event(
    event_type: str,
    **kwargs,
) -> None:

    data = {
        "time": now_string(),
        "event": event_type,
        **kwargs,
    }

    with EVENT_LOG_FILE.open(
        "a",
        encoding="utf-8",
    ) as f:

        f.write(
            json.dumps(
                data,
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )


def safe_title(
    page: Page,
) -> str:

    try:
        return page.title()
    except Exception:
        return ""


def save_screenshot(
    page: Page,
    filename: str,
) -> None:

    try:

        path = SCREENSHOT_DIR / filename

        page.screenshot(
            path=str(path),
            full_page=True,
        )

        print(
            f"[SCREENSHOT] {path}"
        )

    except Exception as exc:

        print(
            f"[WARN] screenshot失敗: {exc}"
        )


# ============================================================
# PLAYボタン解析
# ============================================================

def inspect_play_elements(
    page: Page,
) -> list[dict]:

    results: list[dict] = []

    selectors = [
        "a",
        "button",
        "input",
    ]

    for selector in selectors:

        locator = page.locator(
            selector
        )

        try:
            count = locator.count()
        except Exception:
            continue

        for i in range(count):

            element = locator.nth(i)

            try:

                text = ""

                try:
                    text = (
                        element.inner_text(
                            timeout=300
                        )
                        .strip()
                    )
                except Exception:
                    pass

                href = element.get_attribute(
                    "href"
                )

                onclick = element.get_attribute(
                    "onclick"
                )

                title = element.get_attribute(
                    "title"
                )

                value = element.get_attribute(
                    "value"
                )

                target = element.get_attribute(
                    "target"
                )

                class_name = element.get_attribute(
                    "class"
                )

                outer_html = element.evaluate(
                    "(el) => el.outerHTML"
                )

                searchable = " ".join(
                    [
                        text or "",
                        href or "",
                        onclick or "",
                        title or "",
                        value or "",
                        class_name or "",
                        outer_html or "",
                    ]
                ).lower()

                if (
                    "play" not in searchable
                    and "映像" not in searchable
                    and "movie" not in searchable
                    and "video" not in searchable
                ):
                    continue

                item = {
                    "selector": selector,
                    "index": i,
                    "text": text,
                    "href": href,
                    "onclick": onclick,
                    "title": title,
                    "value": value,
                    "target": target,
                    "class": class_name,
                    "outer_html": outer_html,
                }

                results.append(
                    item
                )

            except Exception:
                continue

    return results


def save_play_elements(
    elements: list[dict],
) -> None:

    lines: list[str] = []

    for number, item in enumerate(
        elements,
        start=1,
    ):

        lines.append(
            "=" * 80
        )

        lines.append(
            f"候補 #{number}"
        )

        lines.append(
            "=" * 80
        )

        for key, value in item.items():

            lines.append(
                f"{key}:"
            )

            lines.append(
                str(value)
            )

            lines.append("")

    PLAY_ELEMENTS_FILE.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


# ============================================================
# Pageイベント
# ============================================================

def install_page_monitor(
    page: Page,
    label: str,
) -> None:

    print()
    print(
        f"[MONITOR] {label}"
    )

    print(
        f"URL={page.url}"
    )

    write_event(
        "page_monitor_installed",
        label=label,
        url=page.url,
    )

    # --------------------------------------------------------
    # Console
    # --------------------------------------------------------

    def on_console(
        message: ConsoleMessage,
    ) -> None:

        try:

            print(
                f"[CONSOLE][{label}] "
                f"{message.type}: "
                f"{message.text}"
            )

            write_event(
                "console",
                label=label,
                console_type=message.type,
                text=message.text,
                page_url=page.url,
            )

        except Exception:
            pass

    page.on(
        "console",
        on_console,
    )

    # --------------------------------------------------------
    # JavaScript error
    # --------------------------------------------------------

    def on_pageerror(
        error,
    ) -> None:

        print(
            f"[PAGEERROR][{label}] "
            f"{error}"
        )

        write_event(
            "pageerror",
            label=label,
            error=str(error),
            page_url=page.url,
        )

    page.on(
        "pageerror",
        on_pageerror,
    )

    # --------------------------------------------------------
    # Frame navigation
    # --------------------------------------------------------

    def on_framenavigated(
        frame,
    ) -> None:

        try:

            print(
                f"[NAVIGATE][{label}] "
                f"{frame.url}"
            )

            write_event(
                "frame_navigated",
                label=label,
                frame_url=frame.url,
                page_url=page.url,
            )

        except Exception:
            pass

    page.on(
        "framenavigated",
        on_framenavigated,
    )

    # --------------------------------------------------------
    # Popup
    # --------------------------------------------------------

    def on_popup(
        popup: Page,
    ) -> None:

        popup_label = (
            f"{label}_popup_"
            f"{len(page.context.pages)}"
        )

        print()
        print(
            "=" * 70
        )

        print(
            "[POPUP DETECTED]"
        )

        print(
            f"URL={popup.url}"
        )

        print(
            "=" * 70
        )

        write_event(
            "popup",
            parent_label=label,
            popup_url=popup.url,
        )

        install_page_monitor(
            popup,
            popup_label,
        )

    page.on(
        "popup",
        on_popup,
    )


# ============================================================
# Contextイベント
# ============================================================

def install_context_monitor(
    context: BrowserContext,
) -> None:

    # --------------------------------------------------------
    # 新規Page
    # --------------------------------------------------------

    def on_page(
        page: Page,
    ) -> None:

        label = (
            f"context_page_"
            f"{len(context.pages)}"
        )

        print()
        print(
            "[NEW PAGE]"
        )

        print(
            f"initial URL={page.url}"
        )

        write_event(
            "new_page",
            label=label,
            initial_url=page.url,
        )

        install_page_monitor(
            page,
            label,
        )

    context.on(
        "page",
        on_page,
    )

    # --------------------------------------------------------
    # Request
    # --------------------------------------------------------

    def on_request(
        request: Request,
    ) -> None:

        url = request.url

        interesting = any(
            word in url.lower()
            for word in (
                "stream",
                "movie",
                "video",
                "player",
                "onetag",
                "m3u8",
                ".mp4",
                ".m4s",
                ".ts",
            )
        )

        if interesting:

            print()
            print(
                "[REQUEST]"
            )

            print(
                request.method,
                url,
            )

            write_event(
                "interesting_request",
                method=request.method,
                url=url,
                resource_type=request.resource_type,
            )

    context.on(
        "request",
        on_request,
    )

    # --------------------------------------------------------
    # Response
    # --------------------------------------------------------

    def on_response(
        response: Response,
    ) -> None:

        try:

            url = response.url

            content_type = (
                response.headers
                .get(
                    "content-type",
                    "",
                )
                .lower()
            )

            interesting = (
                any(
                    word in url.lower()
                    for word in (
                        "stream",
                        "movie",
                        "video",
                        "player",
                        "onetag",
                        ".m3u8",
                        ".mp4",
                        ".m4s",
                        ".ts",
                    )
                )
                or content_type.startswith(
                    "video/"
                )
                or "mpegurl" in content_type
            )

            if not interesting:
                return

            print()
            print(
                "[RESPONSE]"
            )

            print(
                f"{response.status} "
                f"{content_type}"
            )

            print(
                url
            )

            write_event(
                "interesting_response",
                status=response.status,
                content_type=content_type,
                url=url,
            )

        except Exception:
            pass

    context.on(
        "response",
        on_response,
    )

    # --------------------------------------------------------
    # Request failure
    # --------------------------------------------------------

    def on_requestfailed(
        request: Request,
    ) -> None:

        try:

            print()
            print(
                "[REQUEST FAILED]"
            )

            print(
                request.url
            )

            print(
                request.failure
            )

            write_event(
                "request_failed",
                url=request.url,
                method=request.method,
                resource_type=request.resource_type,
                failure=str(
                    request.failure
                ),
            )

        except Exception:
            pass

    context.on(
        "requestfailed",
        on_requestfailed,
    )


# ============================================================
# 全ページ状態
# ============================================================

def dump_all_pages(
    context: BrowserContext,
    stage: str,
) -> list[dict]:

    results: list[dict] = []

    print()
    print(
        "=" * 70
    )

    print(
        f"[PAGE DUMP] {stage}"
    )

    print(
        f"page count = {len(context.pages)}"
    )

    print(
        "=" * 70
    )

    for i, page in enumerate(
        context.pages
    ):

        try:

            page.wait_for_timeout(
                300
            )

        except Exception:
            pass

        item = {
            "index": i,
            "url": page.url,
            "title": safe_title(page),
            "frames": [],
        }

        print()
        print(
            f"PAGE #{i}"
        )

        print(
            f"URL   = {page.url}"
        )

        print(
            f"TITLE = {safe_title(page)}"
        )

        # ----------------------------------------------------
        # opener
        # ----------------------------------------------------

        try:

            opener = page.opener

            if opener:

                item[
                    "opener_url"
                ] = opener.url

                print(
                    f"OPENER= {opener.url}"
                )

            else:

                item[
                    "opener_url"
                ] = None

                print(
                    "OPENER= None"
                )

        except Exception as exc:

            item[
                "opener_error"
            ] = str(exc)

        # ----------------------------------------------------
        # Frames
        # ----------------------------------------------------

        for frame_index, frame in enumerate(
            page.frames
        ):

            try:

                frame_data = {
                    "index": frame_index,
                    "url": frame.url,
                }

                item[
                    "frames"
                ].append(
                    frame_data
                )

                print(
                    f"FRAME #{frame_index}: "
                    f"{frame.url}"
                )

            except Exception:
                pass

        # ----------------------------------------------------
        # videoタグ
        # ----------------------------------------------------

        video_total = 0

        for frame in page.frames:

            try:

                count = frame.locator(
                    "video"
                ).count()

                video_total += count

            except Exception:
                pass

        item[
            "video_count"
        ] = video_total

        print(
            f"VIDEO = {video_total}"
        )

        save_screenshot(
            page,
            f"{stage}_page_{i}.png",
        )

        results.append(
            item
        )

    return results


# ============================================================
# about:blank解析
# ============================================================

def inspect_blank_pages(
    context: BrowserContext,
) -> list[dict]:

    results: list[dict] = []

    print()
    print(
        "=" * 70
    )

    print(
        "[about:blank解析]"
    )

    print(
        "=" * 70
    )

    for index, page in enumerate(
        context.pages
    ):

        if page.url != "about:blank":
            continue

        print()
        print(
            f"about:blank PAGE #{index}"
        )

        result = {
            "index": index,
            "url": page.url,
        }

        # ----------------------------------------------------
        # window.name
        # ----------------------------------------------------

        try:

            window_name = page.evaluate(
                "() => window.name"
            )

            result[
                "window_name"
            ] = window_name

            print(
                f"window.name="
                f"{window_name!r}"
            )

        except Exception as exc:

            result[
                "window_name_error"
            ] = str(exc)

        # ----------------------------------------------------
        # document.referrer
        # ----------------------------------------------------

        try:

            referrer = page.evaluate(
                "() => document.referrer"
            )

            result[
                "document_referrer"
            ] = referrer

            print(
                f"document.referrer="
                f"{referrer!r}"
            )

        except Exception as exc:

            result[
                "referrer_error"
            ] = str(exc)

        # ----------------------------------------------------
        # opener
        # ----------------------------------------------------

        try:

            opener_exists = page.evaluate(
                "() => !!window.opener"
            )

            result[
                "window_opener_exists"
            ] = opener_exists

            print(
                "window.opener exists="
                f"{opener_exists}"
            )

        except Exception as exc:

            result[
                "opener_error"
            ] = str(exc)

        # ----------------------------------------------------
        # HTML
        # ----------------------------------------------------

        try:

            html = page.content()

            result[
                "html_length"
            ] = len(html)

            print(
                f"HTML length={len(html)}"
            )

            blank_html_file = (
                OUTPUT_DIR
                / f"blank_page_{index}.html"
            )

            blank_html_file.write_text(
                html,
                encoding="utf-8",
            )

        except Exception as exc:

            result[
                "html_error"
            ] = str(exc)

        results.append(
            result
        )

    return results


# ============================================================
# main
# ============================================================

def main() -> int:

    prepare_directories()

    # 古いログを削除
    if EVENT_LOG_FILE.exists():
        EVENT_LOG_FILE.unlink()

    print(
        "=" * 70
    )

    print(
        "JRA Video Popup Debug v0.3"
    )

    print(
        "=" * 70
    )

    print(
        f"TARGET={TARGET}"
    )

    print()
    print(
        "今回は動画を保存しません。"
    )

    print(
        "PLAYクリック後にabout:blankになる"
    )

    print(
        "原因だけを調査します。"
    )

    summary = {
        "target": TARGET,
        "start_time": now_string(),
    }

    try:

        with sync_playwright() as p:

            # =================================================
            # persistent context
            # =================================================
            #
            # browser.launch()ではなく
            # launch_persistent_context()を使用する。
            #
            # Chromeのcookie/session/localStorage等を
            # 同一プロファイルへ保持できる。
            # =================================================

            context = (
                p.chromium.launch_persistent_context(
                    user_data_dir=str(
                        PROFILE_DIR
                    ),
                    channel="chrome",
                    headless=HEADLESS,
                    viewport={
                        "width": 1500,
                        "height": 950,
                    },
                    args=[
                        "--disable-popup-blocking",
                        "--autoplay-policy=no-user-gesture-required",
                    ],
                )
            )

            install_context_monitor(
                context
            )

            # =================================================
            # 最初のページ
            # =================================================

            if context.pages:

                page = context.pages[0]

            else:

                page = context.new_page()

            install_page_monitor(
                page,
                "main",
            )

            # =================================================
            # JRA TOP
            # =================================================

            print()
            print(
                "[STEP 1]"
            )

            print(
                "JRA公式トップページを開きます。"
            )

            page.goto(
                JRA_TOP_URL,
                wait_until="domcontentloaded",
                timeout=60000,
            )

            page.wait_for_timeout(
                3000
            )

            print()
            print(
                f"URL={page.url}"
            )

            print(
                f"TITLE={safe_title(page)}"
            )

            save_screenshot(
                page,
                "01_jra_top.png",
            )

            # =================================================
            # 手動操作
            # =================================================

            print()
            print(
                "=" * 70
            )

            print(
                "【ここからChromeを手動操作】"
            )

            print()
            print(
                "次のレースまで移動してください。"
            )

            print()
            print(
                "2026年5月10日"
            )

            print(
                "東京"
            )

            print(
                "1R"
            )

            print()
            print(
                "ただし、まだPLAYは押さないでください。"
            )

            print()
            print(
                "PLAYボタンが画面に表示されたら"
            )

            print(
                "ターミナルへ戻りEnterを押してください。"
            )

            print(
                "=" * 70
            )

            input()

            # =================================================
            # 現在アクティブと思われるJRAページを探索
            # =================================================

            jra_pages = [
                pge
                for pge in context.pages
                if (
                    "jra.go.jp"
                    in pge.url.lower()
                )
            ]

            if not jra_pages:

                raise RuntimeError(
                    "JRAページが見つかりません。"
                )

            current_page = (
                jra_pages[-1]
            )

            print()
            print(
                "[STEP 2]"
            )

            print(
                "PLAYボタンを解析します。"
            )

            print(
                f"現在URL={current_page.url}"
            )

            # HTML保存
            try:

                PAGE_HTML_FILE.write_text(
                    current_page.content(),
                    encoding="utf-8",
                )

            except Exception:
                pass

            play_elements = (
                inspect_play_elements(
                    current_page
                )
            )

            save_play_elements(
                play_elements
            )

            print()
            print(
                "PLAY/映像関連要素="
                f"{len(play_elements)}"
            )

            for i, item in enumerate(
                play_elements,
                start=1,
            ):

                print()
                print(
                    f"--- PLAY候補 #{i} ---"
                )

                print(
                    f"text="
                    f"{item.get('text')}"
                )

                print(
                    f"href="
                    f"{item.get('href')}"
                )

                print(
                    f"onclick="
                    f"{item.get('onclick')}"
                )

                print(
                    f"target="
                    f"{item.get('target')}"
                )

            save_screenshot(
                current_page,
                "02_before_play.png",
            )

            # =================================================
            # PLAYクリック
            # =================================================

            print()
            print(
                "=" * 70
            )

            print(
                "【PLAYを手動でクリックしてください】"
            )

            print()
            print(
                "今回はクリック後、"
            )

            print(
                "about:blankになってもそのまま待ってください。"
            )

            print()
            print(
                "10秒程度待った後、"
            )

            print(
                "ターミナルへ戻ってEnterを押してください。"
            )

            print(
                "=" * 70
            )

            input()

            # 少し待つ
            time.sleep(
                3
            )

            # =================================================
            # 状態取得
            # =================================================

            summary[
                "pages_after_play"
            ] = dump_all_pages(
                context,
                "after_play",
            )

            summary[
                "blank_pages"
            ] = inspect_blank_pages(
                context
            )

            # =================================================
            # 追加監視
            # =================================================

            print()
            print(
                "[INFO] "
                f"さらに{AFTER_ENTER_WAIT_SECONDS}秒"
                "通信を監視します。"
            )

            for remaining in range(
                AFTER_ENTER_WAIT_SECONDS,
                0,
                -1,
            ):

                print(
                    f"\r残り {remaining:2d} 秒",
                    end="",
                    flush=True,
                )

                time.sleep(
                    1
                )

            print()

            summary[
                "pages_final"
            ] = dump_all_pages(
                context,
                "final",
            )

            summary[
                "blank_pages_final"
            ] = inspect_blank_pages(
                context
            )

            summary[
                "end_time"
            ] = now_string()

            SUMMARY_FILE.write_text(
                json.dumps(
                    summary,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                ),
                encoding="utf-8",
            )

            print()
            print(
                "=" * 70
            )

            print(
                "調査完了"
            )

            print(
                "=" * 70
            )

            print()
            print(
                "以下のファイルを確認してください。"
            )

            print()
            print(
                f"1. イベントログ\n"
                f"{EVENT_LOG_FILE}"
            )

            print()
            print(
                f"2. PLAY要素\n"
                f"{PLAY_ELEMENTS_FILE}"
            )

            print()
            print(
                f"3. サマリー\n"
                f"{SUMMARY_FILE}"
            )

            print()
            print(
                f"4. スクリーンショット\n"
                f"{SCREENSHOT_DIR}"
            )

            print()
            print(
                "特に重要なのは"
            )

            print()
            print(
                "・onclick"
            )

            print(
                "・href"
            )

            print(
                "・window.name"
            )

            print(
                "・document.referrer"
            )

            print(
                "・PAGEERROR"
            )

            print(
                "・REQUEST FAILED"
            )

            print(
                "です。"
            )

            print()
            print(
                "Enterで終了します。"
            )

            input()

            context.close()

            return 0

    except KeyboardInterrupt:

        print()
        print(
            "[STOP] ユーザー中断"
        )

        return 1

    except Exception as exc:

        print()
        print(
            "=" * 70
        )

        print(
            "[ERROR]"
        )

        print(
            repr(exc)
        )

        print(
            "=" * 70
        )

        return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )