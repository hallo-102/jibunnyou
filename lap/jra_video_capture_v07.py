from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from playwright.sync_api import (
    BrowserContext,
    Frame,
    Page,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)


# ============================================================
# 設定
# ============================================================

# 2026年8月9日（日）
# 2回新潟6日
# 1R
TARGET = "202602040601"

JRA_TOP_URL = "https://www.jra.go.jp/"

BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = BASE_DIR / "output" / TARGET / "capture_v07"

PROFILE_DIR = BASE_DIR / ".chrome_jra_profile"

OUTPUT_MP4 = OUTPUT_DIR / f"race_{TARGET}.mp4"

M3U8_FILE = OUTPUT_DIR / "detected_playlist.m3u8"

MEDIA_LOG_FILE = OUTPUT_DIR / "media_urls.txt"

METADATA_FILE = OUTPUT_DIR / "capture_metadata.json"

DOM_DIAGNOSTIC_FILE = OUTPUT_DIR / "dom_diagnostic.txt"

POPUP_DIAGNOSTIC_FILE = OUTPUT_DIR / "popup_diagnostic.txt"


HEADLESS = False

TARGET_WAIT_SECONDS = 60

POPUP_WAIT_SECONDS = 20

PLAYER_INITIALIZE_SECONDS = 30

MEDIA_WAIT_SECONDS = 45

FFMPEG_TIMEOUT_SECONDS = 600

HTTP_TIMEOUT_SECONDS = 30


# ============================================================
# 共通
# ============================================================

def now_string() -> str:
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def prepare_directories() -> None:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    PROFILE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def check_ffmpeg() -> str:
    ffmpeg_path = shutil.which(
        "ffmpeg"
    )

    if not ffmpeg_path:
        raise RuntimeError(
            "ffmpegが見つかりません。\n"
            "ffmpeg -version が実行できる状態にしてください。"
        )

    print(
        f"[OK] FFmpeg: {ffmpeg_path}"
    )

    return ffmpeg_path


# ============================================================
# Content-Type
# ============================================================

def normalize_content_type(
    value: Optional[str],
) -> str:

    if not value:
        return ""

    return (
        value
        .lower()
        .split(";")[0]
        .strip()
    )


def is_m3u8(
    url: str,
    content_type: str,
) -> bool:

    if ".m3u8" in url.lower():
        return True

    if content_type in {
        "application/vnd.apple.mpegurl",
        "application/x-mpegurl",
        "application/mpegurl",
    }:
        return True

    return False


def is_video_segment(
    url: str,
    content_type: str,
) -> bool:

    lower = url.lower()

    if ".ts" in lower:
        return True

    if ".m4s" in lower:
        return True

    if content_type.startswith(
        "video/"
    ):
        return True

    return False


# ============================================================
# PLAY DOM調査
# ============================================================

def get_player_ids_from_frame(
    frame: Frame,
) -> list[str]:

    result: list[str] = []

    try:
        ids = frame.evaluate(
            """
            () => {
                return Array.from(
                    document.querySelectorAll(
                        '[id^="btn_player_"]'
                    )
                ).map(el => el.id);
            }
            """
        )

        for element_id in ids:

            match = re.fullmatch(
                r"btn_player_(\d{12})",
                element_id,
            )

            if match:
                result.append(
                    match.group(1)
                )

    except Exception:
        pass

    return result


def dump_browser_state(
    context: BrowserContext,
) -> None:

    lines: list[str] = []

    print()
    print("=" * 70)
    print("[Browser Diagnostic]")
    print(
        f"page count={len(context.pages)}"
    )
    print("=" * 70)

    for page_index, page in enumerate(
        context.pages
    ):

        try:
            title = page.title()
        except Exception:
            title = ""

        print()
        print(
            f"PAGE #{page_index}"
        )

        print(
            f"URL={page.url}"
        )

        print(
            f"TITLE={title}"
        )

        lines.append(
            f"PAGE #{page_index}"
        )

        lines.append(
            f"URL={page.url}"
        )

        lines.append(
            f"TITLE={title}"
        )

        for frame_index, frame in enumerate(
            page.frames
        ):

            ids = get_player_ids_from_frame(
                frame
            )

            print(
                f"  FRAME #{frame_index}"
            )

            print(
                f"  URL={frame.url}"
            )

            print(
                f"  PLAY={len(ids)}"
            )

            lines.append(
                f"FRAME #{frame_index}"
            )

            lines.append(
                f"FRAME_URL={frame.url}"
            )

            lines.append(
                f"RACE_IDS={ids}"
            )

            for race_id in ids:

                mark = (
                    " <- TARGET"
                    if race_id == TARGET
                    else ""
                )

                print(
                    f"    {race_id}{mark}"
                )

    DOM_DIAGNOSTIC_FILE.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


# ============================================================
# TARGET検索
# ============================================================

def find_target_frame(
    context: BrowserContext,
) -> tuple[
    Optional[Page],
    Optional[Frame],
]:

    target_id = (
        f"btn_player_{TARGET}"
    )

    for page in reversed(
        context.pages
    ):

        for frame in page.frames:

            try:
                exists = frame.evaluate(
                    """
                    (targetId) => {
                        return (
                            document.getElementById(
                                targetId
                            ) !== null
                        );
                    }
                    """,
                    target_id,
                )

                if exists:
                    return page, frame

            except Exception:
                continue

    return None, None


def wait_for_target_frame(
    context: BrowserContext,
    timeout_seconds: int,
) -> tuple[
    Optional[Page],
    Optional[Frame],
]:

    print()
    print("=" * 70)
    print("[TARGET検索]")
    print(
        f"TARGET={TARGET}"
    )
    print("=" * 70)

    start = time.time()

    while (
        time.time() - start
        < timeout_seconds
    ):

        page, frame = find_target_frame(
            context
        )

        if (
            page is not None
            and frame is not None
        ):

            print()
            print(
                "[OK] TARGET検出"
            )

            print(
                f"PAGE={page.url}"
            )

            print(
                f"FRAME={frame.url}"
            )

            return page, frame

        time.sleep(
            0.5
        )

    return None, None


# ============================================================
# Popup診断
# ============================================================

def dump_popup_state(
    popup: Page,
) -> None:

    lines: list[str] = []

    print()
    print("=" * 70)
    print("[POPUP DIAGNOSTIC]")
    print("=" * 70)

    print(
        f"URL={popup.url}"
    )

    try:
        title = popup.title()
    except Exception:
        title = ""

    print(
        f"TITLE={title}"
    )

    lines.append(
        f"URL={popup.url}"
    )

    lines.append(
        f"TITLE={title}"
    )

    lines.append(
        f"FRAME_COUNT={len(popup.frames)}"
    )

    print(
        f"FRAME COUNT={len(popup.frames)}"
    )

    for index, frame in enumerate(
        popup.frames
    ):

        print()
        print(
            f"FRAME #{index}"
        )

        print(
            f"URL={frame.url}"
        )

        lines.append(
            f"FRAME #{index}"
        )

        lines.append(
            f"FRAME_URL={frame.url}"
        )

        try:
            video_count = frame.locator(
                "video"
            ).count()
        except Exception:
            video_count = 0

        print(
            f"VIDEO={video_count}"
        )

        lines.append(
            f"VIDEO={video_count}"
        )

        if video_count > 0:

            try:
                info = frame.locator(
                    "video"
                ).first.evaluate(
                    """
                    (v) => ({
                        src: v.src,
                        currentSrc: v.currentSrc,
                        paused: v.paused,
                        readyState: v.readyState,
                        networkState: v.networkState,
                        currentTime: v.currentTime,
                        duration: v.duration
                    })
                    """
                )

                print(
                    json.dumps(
                        info,
                        ensure_ascii=False,
                        indent=2,
                    )
                )

                lines.append(
                    json.dumps(
                        info,
                        ensure_ascii=False,
                    )
                )

            except Exception as exc:

                lines.append(
                    f"VIDEO_INFO_ERROR={exc}"
                )

    POPUP_DIAGNOSTIC_FILE.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


# ============================================================
# PLAYクリック＋Popup取得
# ============================================================

def click_target_play_and_get_popup(
    race_page: Page,
    frame: Frame,
) -> Page:

    target_id = (
        f"btn_player_{TARGET}"
    )

    locator = frame.locator(
        f"#{target_id}"
    )

    info = frame.evaluate(
        """
        (targetId) => {

            const el =
                document.getElementById(
                    targetId
                );

            if (!el) {
                return null;
            }

            return {
                id: el.id,
                href: el.getAttribute('href'),
                onclick: el.getAttribute('onclick'),
                className: el.className,
                text: el.innerText
            };
        }
        """,
        target_id,
    )

    if not info:
        raise RuntimeError(
            "PLAYボタン情報を取得できません。"
        )

    print()
    print("=" * 70)
    print("[TARGET PLAY]")
    print(
        json.dumps(
            info,
            ensure_ascii=False,
            indent=2,
        )
    )
    print("=" * 70)

    try:
        locator.scroll_into_view_if_needed(
            timeout=5000
        )
    except Exception:
        pass

    print()
    print(
        "[INFO] Popupを待ちながらPLAYをクリックします。"
    )

    # --------------------------------------------------------
    # 最重要修正
    # PLAYとPopup待機を同時に行う
    # --------------------------------------------------------

    try:

        with race_page.expect_popup(
            timeout=POPUP_WAIT_SECONDS * 1000
        ) as popup_info:

            locator.click(
                timeout=10000,
            )

        popup = popup_info.value

    except PlaywrightTimeoutError:

        print()
        print(
            "[WARN] expect_popupでPopupを"
            "取得できませんでした。"
        )

        print(
            "[INFO] context.pagesから探します。"
        )

        race_page.wait_for_timeout(
            3000
        )

        context = race_page.context

        candidates = [
            page
            for page in context.pages
            if page != race_page
        ]

        player_candidates = [
            page
            for page in candidates
            if (
                "eqPcPlayer2.html"
                in page.url
                or "webcdn.stream.ne.jp"
                in page.url
            )
        ]

        if player_candidates:

            popup = (
                player_candidates[-1]
            )

        elif candidates:

            popup = candidates[-1]

        else:

            raise RuntimeError(
                "PLAYクリック後にPopupが生成されませんでした。"
            )

    print()
    print(
        "[OK] Popup取得"
    )

    print(
        f"初期URL={popup.url}"
    )

    return popup


# ============================================================
# Popup初期化待ち
# ============================================================

def wait_for_player_initialization(
    popup: Page,
) -> None:

    print()
    print()
    print("=" * 70)
    print("[PLAYER INITIALIZE]")
    print("=" * 70)

    start = time.time()

    previous_urls: tuple[str, ...] = ()

    while (
        time.time() - start
        < PLAYER_INITIALIZE_SECONDS
    ):

        try:

            frame_urls = tuple(
                frame.url
                for frame in popup.frames
            )

        except Exception:

            frame_urls = ()

        if frame_urls != previous_urls:

            previous_urls = frame_urls

            print()
            print(
                "[FRAME UPDATE]"
            )

            for frame_url in frame_urls:

                print(
                    frame_url
                )

        # ----------------------------------------------------
        # inner.htmlが出現したらplayer初期化成功
        # ----------------------------------------------------

        if any(
            "inner.html"
            in frame_url
            for frame_url in frame_urls
        ):

            print()
            print(
                "[OK] inner.htmlを検出しました。"
            )

            return

        popup.wait_for_timeout(
            250
        )

    print()
    print(
        "[WARN] inner.html検出タイムアウト"
    )


# ============================================================
# video再生開始
# ============================================================

def try_start_video(
    popup: Page,
) -> bool:

    print()
    print("=" * 70)
    print("[VIDEO START]")
    print("=" * 70)

    # player初期化を少し待つ
    start = time.time()

    while (
        time.time() - start
        < 20
    ):

        for frame_index, frame in enumerate(
            popup.frames
        ):

            try:

                count = frame.locator(
                    "video"
                ).count()

            except Exception:

                count = 0

            if count <= 0:
                continue

            print()
            print(
                f"[OK] videoタグ検出 "
                f"FRAME #{frame_index}"
            )

            try:

                result = frame.locator(
                    "video"
                ).first.evaluate(
                    """
                    async (video) => {

                        const before = {
                            paused: video.paused,
                            readyState: video.readyState,
                            currentTime: video.currentTime,
                            currentSrc: video.currentSrc
                        };

                        try {

                            video.muted = true;

                            await video.play();

                            return {
                                success: true,
                                before: before,
                                after: {
                                    paused: video.paused,
                                    readyState: video.readyState,
                                    currentTime: video.currentTime,
                                    currentSrc: video.currentSrc
                                }
                            };

                        } catch (e) {

                            return {
                                success: false,
                                before: before,
                                error: String(e)
                            };
                        }
                    }
                    """
                )

                print(
                    json.dumps(
                        result,
                        ensure_ascii=False,
                        indent=2,
                    )
                )

                if result.get(
                    "success"
                ):

                    print()
                    print(
                        "[OK] video.play()成功"
                    )

                    return True

            except Exception as exc:

                print(
                    f"[WARN] video.play()失敗: "
                    f"{exc}"
                )

        popup.wait_for_timeout(
            500
        )

    print()
    print(
        "[WARN] videoタグを起動できませんでした。"
    )

    return False


# ============================================================
# requests
# ============================================================

def build_requests_session(
    context: BrowserContext,
    user_agent: str,
    referer: str,
) -> requests.Session:

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": user_agent,
            "Referer": referer,
            "Accept": "*/*",
        }
    )

    for cookie in context.cookies():

        try:

            session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get(
                    "domain"
                ),
                path=cookie.get(
                    "path",
                    "/",
                ),
            )

        except Exception:
            pass

    return session


def build_cookie_header(
    context: BrowserContext,
) -> str:

    return "; ".join(
        f"{cookie['name']}="
        f"{cookie['value']}"
        for cookie
        in context.cookies()
    )


# ============================================================
# Playlist
# ============================================================

def fetch_playlist(
    session: requests.Session,
    url: str,
) -> str:

    print()
    print(
        "[INFO] m3u8取得"
    )

    print(
        url
    )

    response = session.get(
        url,
        timeout=HTTP_TIMEOUT_SECONDS,
    )

    print(
        f"[HTTP] {response.status_code}"
    )

    response.raise_for_status()

    return response.text


def inspect_playlist_encryption(
    session: requests.Session,
    playlist_url: str,
    visited: Optional[set[str]] = None,
    depth: int = 0,
) -> tuple[
    bool,
    Optional[str],
]:

    if visited is None:
        visited = set()

    if playlist_url in visited:
        return False, None

    visited.add(
        playlist_url
    )

    if depth > 5:
        return False, None

    text = fetch_playlist(
        session,
        playlist_url,
    )

    if depth == 0:

        M3U8_FILE.write_text(
            text,
            encoding="utf-8",
        )

    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]

    # 暗号化チェック
    for line in lines:

        if not line.startswith(
            "#EXT-X-KEY"
        ):
            continue

        upper = line.upper()

        if "METHOD=NONE" not in upper:
            return True, line

    # 子playlist
    for line in lines:

        if line.startswith("#"):
            continue

        if ".m3u8" not in line.lower():
            continue

        child_url = urljoin(
            playlist_url,
            line,
        )

        encrypted, detail = (
            inspect_playlist_encryption(
                session=session,
                playlist_url=child_url,
                visited=visited,
                depth=depth + 1,
            )
        )

        if encrypted:
            return True, detail

    return False, None


# ============================================================
# FFmpeg
# ============================================================

def download_with_ffmpeg(
    playlist_url: str,
    context: BrowserContext,
    user_agent: str,
    referer: str,
) -> None:

    check_ffmpeg()

    cookie_header = (
        build_cookie_header(
            context
        )
    )

    header_lines = [
        f"Referer: {referer}",
        f"User-Agent: {user_agent}",
    ]

    if cookie_header:

        header_lines.append(
            f"Cookie: {cookie_header}"
        )

    headers = (
        "\r\n".join(
            header_lines
        )
        + "\r\n"
    )

    command = [
        "ffmpeg",
        "-y",
        "-loglevel",
        "info",
        "-headers",
        headers,
        "-i",
        playlist_url,
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        str(
            OUTPUT_MP4
        ),
    ]

    print()
    print("=" * 70)
    print("[FFMPEG START]")
    print("=" * 70)

    result = subprocess.run(
        command,
        timeout=FFMPEG_TIMEOUT_SECONDS,
    )

    if result.returncode != 0:

        raise RuntimeError(
            "FFmpegによるMP4保存に失敗しました。"
        )


# ============================================================
# 動画検証
# ============================================================

def validate_video() -> bool:

    if not OUTPUT_MP4.exists():

        print(
            "[ERROR] MP4が存在しません。"
        )

        return False

    size = (
        OUTPUT_MP4.stat().st_size
    )

    print()
    print(
        f"[INFO] MP4 size="
        f"{size / 1024 / 1024:.2f} MB"
    )

    if size <= 0:
        return False

    ffprobe = shutil.which(
        "ffprobe"
    )

    if ffprobe:

        command = [
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration,size,bit_rate",
            "-show_entries",
            (
                "stream="
                "codec_name,"
                "codec_type,"
                "width,"
                "height,"
                "r_frame_rate"
            ),
            "-of",
            "json",
            str(
                OUTPUT_MP4
            ),
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:

            print()
            print(
                "[FFPROBE]"
            )

            print(
                result.stdout
            )

    return True


# ============================================================
# metadata
# ============================================================

def save_metadata(
    success: bool,
    m3u8_url: Optional[str],
    media_urls: list[dict],
    error: Optional[str],
) -> None:

    data = {
        "target": TARGET,
        "timestamp": now_string(),
        "success": success,
        "m3u8_url": m3u8_url,
        "output_mp4": str(
            OUTPUT_MP4
        ),
        "media_urls": media_urls,
        "error": error,
    }

    METADATA_FILE.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


# ============================================================
# Main
# ============================================================

def main() -> int:

    prepare_directories()

    print("=" * 70)
    print("JRA Video Capture PoC v0.7")
    print("=" * 70)

    print()
    print(
        "対象：2026年8月9日（日）"
    )

    print(
        "2回新潟6日 1R"
    )

    print(
        f"TARGET={TARGET}"
    )

    print(
        f"OUTPUT={OUTPUT_MP4}"
    )

    m3u8_url: Optional[str] = None

    media_urls: list[dict] = []

    error_message: Optional[str] = None

    success = False

    try:

        check_ffmpeg()

        with sync_playwright() as p:

            # =================================================
            # Chrome
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
                        (
                            "--autoplay-policy="
                            "no-user-gesture-required"
                        ),
                    ],
                )
            )

            if context.pages:
                page = context.pages[0]
            else:
                page = context.new_page()

            # =================================================
            # User-Agent
            # =================================================

            page.goto(
                "about:blank"
            )

            user_agent = page.evaluate(
                "() => navigator.userAgent"
            )

            print()
            print(
                f"[INFO] User-Agent="
                f"{user_agent}"
            )

            # =================================================
            # Network監視
            # =================================================

            def on_response(
                response: Response,
            ) -> None:

                nonlocal m3u8_url

                try:

                    url = response.url

                    content_type = (
                        normalize_content_type(
                            response.headers.get(
                                "content-type"
                            )
                        )
                    )

                    # -----------------------------------------
                    # m3u8
                    # -----------------------------------------

                    if is_m3u8(
                        url,
                        content_type,
                    ):

                        item = {
                            "type": "m3u8",
                            "status": response.status,
                            "content_type": content_type,
                            "url": url,
                        }

                        media_urls.append(
                            item
                        )

                        print()
                        print(
                            "[M3U8 FOUND]"
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

                        if (
                            response.status == 200
                            and m3u8_url is None
                        ):

                            m3u8_url = url

                    # -----------------------------------------
                    # segment
                    # -----------------------------------------

                    elif is_video_segment(
                        url,
                        content_type,
                    ):

                        item = {
                            "type": "segment",
                            "status": response.status,
                            "content_type": content_type,
                            "url": url,
                        }

                        media_urls.append(
                            item
                        )

                        print(
                            "[SEGMENT] "
                            f"{response.status} "
                            f"{url}"
                        )

                except Exception as exc:

                    print(
                        "[WARN] response解析失敗: "
                        f"{exc}"
                    )

            context.on(
                "response",
                on_response,
            )

            # =================================================
            # JRAトップ
            # =================================================

            print()
            print(
                "[STEP 1] JRAトップ"
            )

            page.goto(
                JRA_TOP_URL,
                wait_until="domcontentloaded",
                timeout=60000,
            )

            page.wait_for_timeout(
                2000
            )

            print()
            print("=" * 70)
            print("【手動操作】")
            print()
            print(
                "Pythonが開いたChromeで"
            )
            print()
            print(
                "2026年8月9日（日）"
            )
            print(
                "2回新潟6日"
            )
            print()
            print(
                "レース結果一覧へ移動してください。"
            )
            print()
            print(
                "1R～12RのPLAYが見えたら"
            )
            print(
                "PLAYは押さずに"
            )
            print(
                "ターミナルでEnter。"
            )
            print("=" * 70)

            input()

            # =================================================
            # DOM
            # =================================================

            dump_browser_state(
                context
            )

            race_page, race_frame = (
                wait_for_target_frame(
                    context=context,
                    timeout_seconds=TARGET_WAIT_SECONDS,
                )
            )

            if (
                race_page is None
                or race_frame is None
            ):

                raise RuntimeError(
                    "TARGETを検出できませんでした。"
                )

            # =================================================
            # Popup取得
            # =================================================

            popup = (
                click_target_play_and_get_popup(
                    race_page=race_page,
                    frame=race_frame,
                )
            )

            # =================================================
            # Popup遷移待ち
            # =================================================

            print()
            print(
                "[INFO] PopupのURL遷移を待ちます。"
            )

            try:

                popup.wait_for_url(
                    re.compile(
                        r".*eqPcPlayer2\.html.*"
                    ),
                    timeout=15000,
                )

            except Exception:

                pass

            print(
                f"[INFO] Popup URL={popup.url}"
            )

            # =================================================
            # player初期化
            # =================================================

            wait_for_player_initialization(
                popup
            )

            dump_popup_state(
                popup
            )

            # =================================================
            # video再生
            # =================================================

            try_start_video(
                popup
            )

            # =================================================
            # m3u8待機
            # =================================================

            print()
            print(
                f"[INFO] m3u8を最大"
                f"{MEDIA_WAIT_SECONDS}秒待機"
            )

            start = time.time()

            while (
                time.time() - start
                < MEDIA_WAIT_SECONDS
            ):

                if m3u8_url:
                    break

                # 定期的にvideo再生を再試行
                elapsed = (
                    time.time()
                    - start
                )

                if (
                    int(elapsed) > 0
                    and int(elapsed) % 5 == 0
                ):

                    try_start_video(
                        popup
                    )

                popup.wait_for_timeout(
                    250
                )

            dump_popup_state(
                popup
            )

            if not m3u8_url:

                raise RuntimeError(
                    "Popupとplayerの起動後も"
                    "m3u8を検出できませんでした。\n\n"
                    f"Popup診断:\n"
                    f"{POPUP_DIAGNOSTIC_FILE}"
                )

            print()
            print("=" * 70)
            print("[M3U8 DETECTED]")
            print(
                m3u8_url
            )
            print("=" * 70)

            # =================================================
            # media log
            # =================================================

            MEDIA_LOG_FILE.write_text(
                "\n".join(
                    json.dumps(
                        item,
                        ensure_ascii=False,
                    )
                    for item
                    in media_urls
                ),
                encoding="utf-8",
            )

            # =================================================
            # Referer
            # =================================================

            referer = popup.url

            print()
            print(
                f"[INFO] Referer={referer}"
            )

            # =================================================
            # requests
            # =================================================

            session = (
                build_requests_session(
                    context=context,
                    user_agent=user_agent,
                    referer=referer,
                )
            )

            # =================================================
            # HLS
            # =================================================

            print()
            print(
                "[STEP 2] HLS暗号化確認"
            )

            encrypted, detail = (
                inspect_playlist_encryption(
                    session=session,
                    playlist_url=m3u8_url,
                )
            )

            if encrypted:

                print()
                print("=" * 70)
                print("[STOP]")
                print(
                    "暗号化HLSを検出しました。"
                )
                print(
                    detail
                )
                print(
                    "保護機構の回避は行いません。"
                )
                print("=" * 70)

                raise RuntimeError(
                    "Encrypted HLS detected."
                )

            print()
            print(
                "[OK] 暗号化HLSではありません。"
            )

            # =================================================
            # MP4
            # =================================================

            print()
            print(
                "[STEP 3] MP4保存"
            )

            download_with_ffmpeg(
                playlist_url=m3u8_url,
                context=context,
                user_agent=user_agent,
                referer=referer,
            )

            # =================================================
            # 検証
            # =================================================

            print()
            print(
                "[STEP 4] MP4検証"
            )

            success = validate_video()

            if not success:

                raise RuntimeError(
                    "MP4検証に失敗しました。"
                )

            save_metadata(
                success=True,
                m3u8_url=m3u8_url,
                media_urls=media_urls,
                error=None,
            )

            print()
            print("=" * 70)
            print("[SUCCESS]")
            print()
            print(
                "JRAレース動画を"
                "MP4として保存しました。"
            )
            print()
            print(
                OUTPUT_MP4
            )
            print()
            print(
                "次工程："
            )
            print(
                "OpenCVフレーム解析"
            )
            print("=" * 70)

            input(
                "Enterで終了します。"
            )

            context.close()

            return 0

    except KeyboardInterrupt:

        error_message = (
            "ユーザーによる中断"
        )

        print()
        print(
            "[STOP] ユーザー中断"
        )

    except Exception as exc:

        error_message = str(
            exc
        )

        print()
        print("=" * 70)
        print("[ERROR]")
        print(
            error_message
        )
        print("=" * 70)

    save_metadata(
        success=False,
        m3u8_url=m3u8_url,
        media_urls=media_urls,
        error=error_message,
    )

    return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )