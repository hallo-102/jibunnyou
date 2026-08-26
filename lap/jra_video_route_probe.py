from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import (
    BrowserContext,
    Page,
    Response,
    sync_playwright,
)


# ============================================================
# 基本設定
# ============================================================

TARGET = "202602040601"

JRA_TOP_URL = "https://www.jra.go.jp/"

BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = BASE_DIR / "output" / TARGET

LOG_FILE = OUTPUT_DIR / "route_probe_log.json"

MEDIA_FILE = OUTPUT_DIR / "route_probe_media_urls.txt"

PAGE_LINK_FILE = OUTPUT_DIR / "page_links.txt"

SCREENSHOT_DIR = OUTPUT_DIR / "screenshots"


HEADLESS = False

WAIT_SECONDS = 5


# ============================================================
# 保存先
# ============================================================

def prepare_directories() -> None:

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    SCREENSHOT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


# ============================================================
# メディア判定
# ============================================================

def normalize_content_type(
    content_type: str | None,
) -> str:

    if not content_type:
        return ""

    return (
        content_type
        .lower()
        .split(";")[0]
        .strip()
    )


def is_media_response(
    url: str,
    content_type: str,
) -> bool:

    url_lower = url.lower()

    if any(
        ext in url_lower
        for ext in (
            ".m3u8",
            ".mp4",
            ".m4s",
            ".ts",
        )
    ):
        return True

    if content_type.startswith("video/"):
        return True

    if content_type in {
        "application/vnd.apple.mpegurl",
        "application/x-mpegurl",
        "application/mpegurl",
    }:
        return True

    return False


# ============================================================
# スクリーンショット
# ============================================================

def save_screenshot(
    page: Page,
    name: str,
) -> None:

    path = SCREENSHOT_DIR / name

    try:

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
# ページ情報表示
# ============================================================

def print_page_info(
    page: Page,
    label: str,
) -> None:

    print()
    print("=" * 70)

    print(
        f"[PAGE] {label}"
    )

    print(
        f"URL   : {page.url}"
    )

    try:

        print(
            f"TITLE : {page.title()}"
        )

    except Exception:

        pass

    print(
        "=" * 70
    )


# ============================================================
# リンク取得
# ============================================================

def collect_links(
    page: Page,
) -> list[dict]:

    result = []

    try:

        links = page.locator("a")

        count = links.count()

        print(
            f"[INFO] aタグ数: {count}"
        )

        for i in range(count):

            try:

                element = links.nth(i)

                text = (
                    element
                    .inner_text(timeout=1000)
                    .strip()
                )

                href = element.get_attribute(
                    "href"
                )

                title = element.get_attribute(
                    "title"
                )

                if not href:
                    continue

                item = {
                    "index": i,
                    "text": text,
                    "href": href,
                    "title": title,
                }

                result.append(
                    item
                )

            except Exception:
                continue

    except Exception as exc:

        print(
            f"[WARN] link取得失敗: {exc}"
        )

    return result


# ============================================================
# リンク保存
# ============================================================

def save_links(
    links: list[dict],
) -> None:

    lines = []

    for item in links:

        lines.append(
            f"INDEX={item['index']}"
        )

        lines.append(
            f"TEXT={item['text']}"
        )

        lines.append(
            f"TITLE={item['title']}"
        )

        lines.append(
            f"HREF={item['href']}"
        )

        lines.append(
            "-" * 60
        )

    PAGE_LINK_FILE.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


# ============================================================
# レース映像関連リンク
# ============================================================

def find_video_related_elements(
    page: Page,
) -> list[dict]:

    keywords = [
        "レース映像",
        "映像",
        "PLAY",
        "play",
        "movie",
        "video",
    ]

    results = []

    # --------------------------------------------------------
    # aタグ
    # --------------------------------------------------------

    locator = page.locator("a")

    for i in range(locator.count()):

        try:

            el = locator.nth(i)

            text = (
                el.inner_text(
                    timeout=500
                )
                .strip()
            )

            href = el.get_attribute(
                "href"
            )

            title = el.get_attribute(
                "title"
            )

            searchable = (
                f"{text} "
                f"{href or ''} "
                f"{title or ''}"
            )

            if any(
                keyword.lower()
                in searchable.lower()
                for keyword in keywords
            ):

                results.append(
                    {
                        "type": "a",
                        "index": i,
                        "text": text,
                        "href": href,
                        "title": title,
                    }
                )

        except Exception:
            continue

    # --------------------------------------------------------
    # button
    # --------------------------------------------------------

    locator = page.locator("button")

    for i in range(locator.count()):

        try:

            el = locator.nth(i)

            text = (
                el.inner_text(
                    timeout=500
                )
                .strip()
            )

            title = el.get_attribute(
                "title"
            )

            searchable = (
                f"{text} "
                f"{title or ''}"
            )

            if any(
                keyword.lower()
                in searchable.lower()
                for keyword in keywords
            ):

                results.append(
                    {
                        "type": "button",
                        "index": i,
                        "text": text,
                        "href": None,
                        "title": title,
                    }
                )

        except Exception:
            continue

    return results


# ============================================================
# frame情報
# ============================================================

def inspect_frames(
    page: Page,
) -> None:

    print()
    print(
        f"[INFO] frame数: {len(page.frames)}"
    )

    for i, frame in enumerate(
        page.frames
    ):

        print(
            f"[FRAME {i}] {frame.url}"
        )


# ============================================================
# videoタグ確認
# ============================================================

def inspect_video_tags(
    page: Page,
) -> int:

    total = 0

    for i, frame in enumerate(
        page.frames
    ):

        try:

            count = frame.locator(
                "video"
            ).count()

            total += count

            print(
                f"[FRAME {i}] "
                f"videoタグ={count}"
            )

            for j in range(count):

                video = frame.locator(
                    "video"
                ).nth(j)

                try:

                    data = video.evaluate(
                        """
                        (v) => ({
                            src: v.src,
                            currentSrc: v.currentSrc,
                            paused: v.paused,
                            currentTime: v.currentTime,
                            duration: v.duration,
                            readyState: v.readyState
                        })
                        """
                    )

                    print(
                        f"[VIDEO {j}] "
                        f"{json.dumps(data, ensure_ascii=False)}"
                    )

                except Exception as exc:

                    print(
                        "[WARN] video情報取得失敗:",
                        exc,
                    )

        except Exception:
            continue

    print(
        f"[INFO] videoタグ合計={total}"
    )

    return total


# ============================================================
# JRAトップ
# ============================================================

def open_jra_top(
    page: Page,
) -> None:

    print()
    print(
        "[STEP 1] JRAトップページを開きます"
    )

    page.goto(
        JRA_TOP_URL,
        wait_until="domcontentloaded",
        timeout=60000,
    )

    page.wait_for_timeout(
        3000
    )

    print_page_info(
        page,
        "JRA TOP",
    )

    save_screenshot(
        page,
        "01_jra_top.png",
    )


# ============================================================
# target文字列探索
# ============================================================

def find_target_in_links(
    links: list[dict],
) -> list[dict]:

    results = []

    for item in links:

        value = (
            f"{item.get('text', '')} "
            f"{item.get('href', '')} "
            f"{item.get('title', '')}"
        )

        if TARGET in value:

            results.append(
                item
            )

    return results


# ============================================================
# URL表示補助
# ============================================================

def print_candidate(
    number: int,
    item: dict,
) -> None:

    print()
    print(
        f"[候補 {number}]"
    )

    print(
        f"type  : {item.get('type')}"
    )

    print(
        f"index : {item.get('index')}"
    )

    print(
        f"text  : {item.get('text')}"
    )

    print(
        f"title : {item.get('title')}"
    )

    print(
        f"href  : {item.get('href')}"
    )


# ============================================================
# main
# ============================================================

def main() -> int:

    prepare_directories()

    print(
        "=" * 70
    )

    print(
        "JRA Video Route Probe"
    )

    print(
        "=" * 70
    )

    print(
        f"TARGET={TARGET}"
    )

    media_candidates: dict[
        str,
        dict
    ] = {}

    route_log = {
        "target": TARGET,
        "pages": [],
        "media": [],
    }

    try:

        with sync_playwright() as p:

            # =================================================
            # Chrome
            # =================================================

            browser = p.chromium.launch(
                channel="chrome",
                headless=HEADLESS,
            )

            context = browser.new_context(
                viewport={
                    "width": 1500,
                    "height": 950,
                }
            )

            page = context.new_page()

            # =================================================
            # ネットワーク監視
            # =================================================

            def on_response(
                response: Response,
            ) -> None:

                try:

                    url = response.url

                    content_type = (
                        normalize_content_type(
                            response.headers.get(
                                "content-type"
                            )
                        )
                    )

                    if not is_media_response(
                        url,
                        content_type,
                    ):
                        return

                    if url in media_candidates:
                        return

                    media_candidates[url] = {
                        "url": url,
                        "status": response.status,
                        "content_type": content_type,
                    }

                    print()
                    print(
                        "[MEDIA FOUND]"
                    )

                    print(
                        f"status={response.status}"
                    )

                    print(
                        f"type={content_type}"
                    )

                    print(
                        f"url={url}"
                    )

                except Exception as exc:

                    print(
                        "[WARN] "
                        f"response解析エラー: {exc}"
                    )

            context.on(
                "response",
                on_response,
            )

            # =================================================
            # JRAトップ
            # =================================================

            open_jra_top(
                page
            )

            route_log[
                "pages"
            ].append(
                {
                    "url": page.url,
                    "title": page.title(),
                }
            )

            # =================================================
            # リンク解析
            # =================================================

            links = collect_links(
                page
            )

            save_links(
                links
            )

            # TARGET直接リンクがあるか
            target_links = (
                find_target_in_links(
                    links
                )
            )

            print()
            print(
                "[INFO] "
                f"TARGETを含むリンク数="
                f"{len(target_links)}"
            )

            for i, item in enumerate(
                target_links,
                start=1,
            ):

                print_candidate(
                    i,
                    item,
                )

            # =================================================
            # レース映像候補
            # =================================================

            video_elements = (
                find_video_related_elements(
                    page
                )
            )

            print()
            print(
                "[INFO] "
                f"映像関連候補="
                f"{len(video_elements)}"
            )

            for i, item in enumerate(
                video_elements,
                start=1,
            ):

                print_candidate(
                    i,
                    item,
                )

            # =================================================
            # iframe/video
            # =================================================

            inspect_frames(
                page
            )

            inspect_video_tags(
                page
            )

            # =================================================
            # 手動操作フェーズ
            # =================================================

            print()
            print(
                "=" * 70
            )

            print(
                "【手動操作してください】"
            )

            print()
            print(
                "Chrome上でJRA公式サイトから"
            )

            print(
                "2026年5月10日 東京1R"
            )

            print(
                "のレース結果まで移動し、"
            )

            print(
                "「レース映像」をクリックしてください。"
            )

            print()
            print(
                "動画が実際に再生されたら、"
            )

            print(
                "このターミナルへ戻りEnterを押してください。"
            )

            print(
                "=" * 70
            )

            input()

            # =================================================
            # 開いている全ページ確認
            # =================================================

            pages = context.pages

            print()
            print(
                f"[INFO] 開いているページ数="
                f"{len(pages)}"
            )

            for i, current_page in enumerate(
                pages
            ):

                print_page_info(
                    current_page,
                    f"PAGE {i}",
                )

                route_log[
                    "pages"
                ].append(
                    {
                        "url": current_page.url,
                        "title": (
                            current_page.title()
                        ),
                    }
                )

                save_screenshot(
                    current_page,
                    f"after_play_{i}.png",
                )

                inspect_frames(
                    current_page
                )

                inspect_video_tags(
                    current_page
                )

            # =================================================
            # 追加監視
            # =================================================

            print()
            print(
                "[INFO] "
                "動画通信をさらに10秒監視します"
            )

            time.sleep(
                10
            )

            # =================================================
            # メディアログ
            # =================================================

            route_log[
                "media"
            ] = list(
                media_candidates.values()
            )

            lines = []

            for i, item in enumerate(
                media_candidates.values(),
                start=1,
            ):

                lines.append(
                    f"[{i}]"
                )

                lines.append(
                    f"status="
                    f"{item['status']}"
                )

                lines.append(
                    f"content_type="
                    f"{item['content_type']}"
                )

                lines.append(
                    f"url="
                    f"{item['url']}"
                )

                lines.append(
                    ""
                )

            MEDIA_FILE.write_text(
                "\n".join(lines),
                encoding="utf-8",
            )

            LOG_FILE.write_text(
                json.dumps(
                    route_log,
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            # =================================================
            # 結果
            # =================================================

            print()
            print(
                "=" * 70
            )

            print(
                "[RESULT]"
            )

            print(
                "media count = "
                f"{len(media_candidates)}"
            )

            print()
            print(
                f"MEDIA LOG:\n{MEDIA_FILE}"
            )

            print()
            print(
                f"ROUTE LOG:\n{LOG_FILE}"
            )

            print(
                "=" * 70
            )

            if media_candidates:

                print()
                print(
                    "[SUCCESS]"
                )

                print(
                    "動画通信を検出しました。"
                )

                print(
                    "次は自動導線化＋MP4保存へ進めます。"
                )

                browser.close()

                return 0

            print()
            print(
                "[FAILED]"
            )

            print(
                "まだ動画通信を検出できていません。"
            )

            browser.close()

            return 1

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