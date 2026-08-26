from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from playwright.sync_api import (
    BrowserContext,
    Page,
    Response,
    sync_playwright,
)


# ============================================================
# 設定
# ============================================================

TARGET = "202602040601"

PLAYER_URL = (
    "https://jra.webcdn.stream.ne.jp/web/jra/onetag2020/"
    f"eqPcPlayer2.html?target={TARGET}"
)

BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = BASE_DIR / "output" / TARGET

OUTPUT_MP4 = OUTPUT_DIR / f"race_{TARGET}.mp4"

MEDIA_URL_FILE = OUTPUT_DIR / "detected_media_urls.txt"

PLAYLIST_FILE = OUTPUT_DIR / "playlist.m3u8"

METADATA_FILE = OUTPUT_DIR / "capture_metadata.json"


# Chromeを画面表示する
HEADLESS = False


# ページ表示後に動画通信を待つ秒数
NETWORK_WAIT_SECONDS = 20


# 動画再生を試す
TRY_AUTOPLAY = True


# FFmpegのタイムアウト
FFMPEG_TIMEOUT_SECONDS = 600


# requestsタイムアウト
HTTP_TIMEOUT_SECONDS = 30


# ============================================================
# データクラス
# ============================================================

@dataclass
class MediaCandidate:
    url: str
    content_type: str
    status: int


# ============================================================
# 共通関数
# ============================================================

def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def check_ffmpeg() -> bool:
    """
    ffmpegがPATHに存在するか確認。
    """
    ffmpeg_path = shutil.which("ffmpeg")

    if ffmpeg_path is None:
        print("[WARN] ffmpeg がPATHに見つかりません。")
        return False

    print(f"[INFO] ffmpeg: {ffmpeg_path}")
    return True


def normalize_content_type(value: Optional[str]) -> str:
    if not value:
        return ""

    return value.lower().split(";")[0].strip()


def is_media_response(
    url: str,
    content_type: str,
) -> bool:

    url_lower = url.lower()

    media_extensions = (
        ".m3u8",
        ".mp4",
        ".ts",
        ".m4s",
    )

    if any(ext in url_lower for ext in media_extensions):
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


def classify_media_url(url: str) -> str:
    """
    URLから大まかな種類を判定。
    """

    path = urlparse(url).path.lower()

    if ".m3u8" in path:
        return "hls"

    if ".mp4" in path:
        return "mp4"

    return "unknown"


# ============================================================
# Playwright
# ============================================================

def get_user_agent(page: Page) -> str:
    return page.evaluate("navigator.userAgent")


def try_start_video(page: Page) -> None:
    """
    videoタグが存在する場合は再生を試みる。

    再生失敗してもプログラム全体は止めない。
    """

    if not TRY_AUTOPLAY:
        return

    print("[INFO] video再生を試行します。")

    try:
        video_count = page.locator("video").count()

        print(f"[INFO] videoタグ数: {video_count}")

        if video_count > 0:

            result = page.locator("video").first.evaluate(
                """
                async (video) => {
                    try {
                        video.muted = true;
                        await video.play();

                        return {
                            success: true,
                            currentTime: video.currentTime,
                            duration: video.duration,
                            paused: video.paused
                        };
                    } catch (e) {
                        return {
                            success: false,
                            error: String(e)
                        };
                    }
                }
                """
            )

            print(
                "[INFO] video.play()結果:",
                result,
            )

    except Exception as exc:
        print(
            "[WARN] video自動再生に失敗:",
            exc,
        )


def collect_media_urls(
    page: Page,
    seconds: int,
) -> list[MediaCandidate]:

    candidates: dict[str, MediaCandidate] = {}

    def on_response(response: Response) -> None:

        try:
            url = response.url

            headers = response.headers

            content_type = normalize_content_type(
                headers.get("content-type")
            )

            if not is_media_response(
                url,
                content_type,
            ):
                return

            if url not in candidates:

                candidate = MediaCandidate(
                    url=url,
                    content_type=content_type,
                    status=response.status,
                )

                candidates[url] = candidate

                print()
                print("[MEDIA]")
                print(f"status       : {response.status}")
                print(f"content-type : {content_type}")
                print(f"url          : {url}")

        except Exception as exc:
            print(
                "[WARN] response解析エラー:",
                exc,
            )

    page.on(
        "response",
        on_response,
    )

    print()
    print(
        f"[INFO] JRAプレイヤーへアクセス:\n{PLAYER_URL}"
    )

    page.goto(
        PLAYER_URL,
        wait_until="domcontentloaded",
        timeout=60000,
    )

    # JavaScriptプレイヤー初期化待ち
    page.wait_for_timeout(3000)

    try_start_video(page)

    print()
    print(
        f"[INFO] 動画通信を {seconds} 秒監視します。"
    )

    start = time.time()

    while time.time() - start < seconds:

        page.wait_for_timeout(500)

        # 動画が停止していたら再生を再試行
        try:
            if page.locator("video").count() > 0:

                paused = page.locator("video").first.evaluate(
                    "(video) => video.paused"
                )

                if paused:
                    page.locator("video").first.evaluate(
                        """
                        async (video) => {
                            video.muted = true;
                            try {
                                await video.play();
                            } catch (e) {
                            }
                        }
                        """
                    )

        except Exception:
            pass

    return list(candidates.values())


# ============================================================
# Cookie / HTTP
# ============================================================

def build_requests_session(
    context: BrowserContext,
    user_agent: str,
) -> requests.Session:

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": user_agent,
            "Referer": PLAYER_URL,
            "Accept": "*/*",
        }
    )

    cookies = context.cookies()

    for cookie in cookies:

        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )

    return session


def build_cookie_header(
    context: BrowserContext,
) -> str:

    cookies = context.cookies()

    return "; ".join(
        f"{cookie['name']}={cookie['value']}"
        for cookie in cookies
    )


# ============================================================
# HLSチェック
# ============================================================

def fetch_text(
    session: requests.Session,
    url: str,
) -> str:

    print(
        f"[INFO] playlist取得: {url}"
    )

    response = session.get(
        url,
        timeout=HTTP_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    return response.text


def check_hls_encryption_recursive(
    session: requests.Session,
    playlist_url: str,
    visited: Optional[set[str]] = None,
    depth: int = 0,
    max_depth: int = 4,
) -> tuple[bool, Optional[str]]:

    if visited is None:
        visited = set()

    if playlist_url in visited:
        return False, None

    visited.add(playlist_url)

    if depth > max_depth:
        return False, None

    text = fetch_text(
        session,
        playlist_url,
    )

    # 最初に取得したplaylistを保存
    if depth == 0:

        PLAYLIST_FILE.write_text(
            text,
            encoding="utf-8",
        )

    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]

    # --------------------------------------------------------
    # 暗号化チェック
    # --------------------------------------------------------

    for line in lines:

        if not line.startswith("#EXT-X-KEY"):
            continue

        upper = line.upper()

        # METHOD=NONE以外は今回は取得しない
        if "METHOD=NONE" not in upper:

            return True, line

    # --------------------------------------------------------
    # Master playlist内部のm3u8も確認
    # --------------------------------------------------------

    for line in lines:

        if line.startswith("#"):
            continue

        resolved = urljoin(
            playlist_url,
            line,
        )

        parsed_path = urlparse(
            resolved
        ).path.lower()

        if ".m3u8" not in parsed_path:
            continue

        encrypted, detail = check_hls_encryption_recursive(
            session=session,
            playlist_url=resolved,
            visited=visited,
            depth=depth + 1,
            max_depth=max_depth,
        )

        if encrypted:
            return True, detail

    return False, None


# ============================================================
# MP4直接取得
# ============================================================

def download_direct_mp4(
    session: requests.Session,
    url: str,
) -> None:

    print()
    print("[INFO] MP4直接取得開始")
    print(url)

    with session.get(
        url,
        stream=True,
        timeout=HTTP_TIMEOUT_SECONDS,
    ) as response:

        response.raise_for_status()

        with OUTPUT_MP4.open("wb") as file:

            total = 0

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):

                if not chunk:
                    continue

                file.write(chunk)

                total += len(chunk)

                print(
                    f"\r[DOWNLOAD] "
                    f"{total / 1024 / 1024:.1f} MB",
                    end="",
                    flush=True,
                )

    print()


# ============================================================
# FFmpeg HLS取得
# ============================================================

def download_hls_with_ffmpeg(
    url: str,
    user_agent: str,
    cookie_header: str,
) -> None:

    if not check_ffmpeg():

        raise RuntimeError(
            "FFmpegが見つかりません。"
        )

    print()
    print("[INFO] FFmpegでHLSをMP4へ保存します。")

    # FFmpegへブラウザと同じ基本ヘッダーを渡す
    headers_string = (
        f"Referer: {PLAYER_URL}\r\n"
        f"User-Agent: {user_agent}\r\n"
    )

    if cookie_header:

        headers_string += (
            f"Cookie: {cookie_header}\r\n"
        )

    command = [
        "ffmpeg",

        "-y",

        "-loglevel",
        "info",

        "-headers",
        headers_string,

        "-i",
        url,

        # 再エンコードせずコピー
        "-c",
        "copy",

        str(OUTPUT_MP4),
    ]

    print()
    print("[INFO] FFmpeg実行")

    result = subprocess.run(
        command,
        timeout=FFMPEG_TIMEOUT_SECONDS,
    )

    if result.returncode != 0:

        raise RuntimeError(
            "FFmpegによる動画取得に失敗しました。"
        )


# ============================================================
# 動画検証
# ============================================================

def validate_output_video() -> bool:

    if not OUTPUT_MP4.exists():

        print(
            "[ERROR] MP4が作成されていません。"
        )

        return False

    size = OUTPUT_MP4.stat().st_size

    print()
    print(
        f"[INFO] 出力サイズ: "
        f"{size / 1024 / 1024:.2f} MB"
    )

    if size <= 0:

        print(
            "[ERROR] MP4サイズが0です。"
        )

        return False

    # ffprobeがある場合のみ詳細確認
    ffprobe = shutil.which("ffprobe")

    if ffprobe:

        command = [
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration,size,bit_rate",
            "-show_entries",
            "stream=index,codec_name,codec_type,width,height,r_frame_rate",
            "-of",
            "json",
            str(OUTPUT_MP4),
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:

            print()
            print("[INFO] ffprobe結果")

            print(
                result.stdout
            )

    return True


# ============================================================
# 候補URL保存
# ============================================================

def save_candidates(
    candidates: list[MediaCandidate],
) -> None:

    lines = []

    for index, item in enumerate(
        candidates,
        start=1,
    ):

        lines.append(
            f"[{index}]"
        )

        lines.append(
            f"status={item.status}"
        )

        lines.append(
            f"content_type={item.content_type}"
        )

        lines.append(
            f"url={item.url}"
        )

        lines.append("")

    MEDIA_URL_FILE.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


# ============================================================
# URL選択
# ============================================================

def choose_best_media_candidate(
    candidates: list[MediaCandidate],
) -> Optional[MediaCandidate]:

    # --------------------------------------------------------
    # 1. m3u8を優先
    # --------------------------------------------------------

    hls = [
        item
        for item in candidates
        if classify_media_url(item.url) == "hls"
    ]

    if hls:

        # 一般的にMaster playlistが最初に来る可能性が高いため
        # 最初の候補を使う
        return hls[0]

    # --------------------------------------------------------
    # 2. mp4
    # --------------------------------------------------------

    mp4 = [
        item
        for item in candidates
        if classify_media_url(item.url) == "mp4"
    ]

    if mp4:
        return mp4[0]

    return None


# ============================================================
# metadata
# ============================================================

def save_metadata(
    user_agent: str,
    candidates: list[MediaCandidate],
    selected: Optional[MediaCandidate],
    success: bool,
    error: Optional[str] = None,
) -> None:

    metadata = {
        "target": TARGET,
        "player_url": PLAYER_URL,
        "user_agent": user_agent,
        "detected_media_count": len(candidates),
        "detected_media": [
            asdict(item)
            for item in candidates
        ],
        "selected_media": (
            asdict(selected)
            if selected
            else None
        ),
        "output_mp4": str(OUTPUT_MP4),
        "success": success,
        "error": error,
        "timestamp": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }

    METADATA_FILE.write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


# ============================================================
# main
# ============================================================

def main() -> int:

    ensure_output_dir()

    print("=" * 70)
    print("JRA Race Video Capture PoC")
    print("=" * 70)

    print(
        f"TARGET : {TARGET}"
    )

    print(
        f"URL    : {PLAYER_URL}"
    )

    print(
        f"OUTPUT : {OUTPUT_MP4}"
    )

    candidates: list[MediaCandidate] = []

    selected: Optional[MediaCandidate] = None

    user_agent = ""

    success = False

    error_message: Optional[str] = None

    try:

        with sync_playwright() as playwright:

            print()
            print(
                "[INFO] Google Chrome起動"
            )

            browser = playwright.chromium.launch(
                channel="chrome",
                headless=HEADLESS,
                args=[
                    "--autoplay-policy=no-user-gesture-required",
                ],
            )

            context = browser.new_context(
                viewport={
                    "width": 1400,
                    "height": 900,
                }
            )

            page = context.new_page()

            # ------------------------------------------------
            # user-agent
            # ------------------------------------------------

            page.goto(
                "about:blank"
            )

            user_agent = get_user_agent(
                page
            )

            print(
                f"[INFO] User-Agent: {user_agent}"
            )

            # ------------------------------------------------
            # メディアURL検出
            # ------------------------------------------------

            candidates = collect_media_urls(
                page=page,
                seconds=NETWORK_WAIT_SECONDS,
            )

            save_candidates(
                candidates
            )

            print()
            print(
                f"[INFO] メディア候補数: "
                f"{len(candidates)}"
            )

            # ------------------------------------------------
            # 最適候補選択
            # ------------------------------------------------

            selected = choose_best_media_candidate(
                candidates
            )

            if selected is None:

                raise RuntimeError(
                    "m3u8またはmp4を検出できませんでした。"
                )

            print()
            print("[INFO] 採用URL")
            print(selected.url)

            media_type = classify_media_url(
                selected.url
            )

            print(
                f"[INFO] media_type={media_type}"
            )

            # ------------------------------------------------
            # requests用Session作成
            # ------------------------------------------------

            session = build_requests_session(
                context=context,
                user_agent=user_agent,
            )

            cookie_header = build_cookie_header(
                context
            )

            # ------------------------------------------------
            # HLS
            # ------------------------------------------------

            if media_type == "hls":

                print()
                print(
                    "[INFO] HLS暗号化チェック"
                )

                encrypted, detail = (
                    check_hls_encryption_recursive(
                        session=session,
                        playlist_url=selected.url,
                    )
                )

                if encrypted:

                    print()
                    print("=" * 70)

                    print(
                        "[STOP] 暗号化されたHLSを検出しました。"
                    )

                    print(
                        f"detail={detail}"
                    )

                    print(
                        "このPoCでは保護機構の回避を行いません。"
                    )

                    print("=" * 70)

                    raise RuntimeError(
                        "Encrypted HLS detected."
                    )

                print(
                    "[OK] HLS暗号化なし"
                )

                download_hls_with_ffmpeg(
                    url=selected.url,
                    user_agent=user_agent,
                    cookie_header=cookie_header,
                )

            # ------------------------------------------------
            # MP4
            # ------------------------------------------------

            elif media_type == "mp4":

                download_direct_mp4(
                    session=session,
                    url=selected.url,
                )

            else:

                raise RuntimeError(
                    "対応していない動画形式です。"
                )

            # ------------------------------------------------
            # 動画検証
            # ------------------------------------------------

            success = validate_output_video()

            browser.close()

    except KeyboardInterrupt:

        error_message = (
            "ユーザーによって中断されました。"
        )

        print()
        print(
            "[STOP] ユーザー中断"
        )

    except Exception as exc:

        error_message = str(exc)

        print()
        print("=" * 70)
        print("[ERROR]")
        print(exc)
        print("=" * 70)

    finally:

        save_metadata(
            user_agent=user_agent,
            candidates=candidates,
            selected=selected,
            success=success,
            error=error_message,
        )

    # ========================================================
    # 最終結果
    # ========================================================

    print()
    print("=" * 70)

    if success:

        print("[SUCCESS]")
        print(
            "JRAレース映像をMP4として保存しました。"
        )

        print()
        print(
            f"保存先:\n{OUTPUT_MP4}"
        )

        print()
        print(
            "次工程：OpenCVによるフレーム分解へ進めます。"
        )

        print("=" * 70)

        return 0

    print("[FAILED]")

    print(
        "動画取得PoCは完了しませんでした。"
    )

    print()
    print(
        f"通信ログ:\n{MEDIA_URL_FILE}"
    )

    print()
    print(
        f"metadata:\n{METADATA_FILE}"
    )

    print("=" * 70)

    return 1


if __name__ == "__main__":
    sys.exit(main())