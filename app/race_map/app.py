"""レース質・脚質ポジションマップ Streamlitアプリ。"""

from pathlib import Path
import sys
from html import escape

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from src.race_map.constants import HORSE_NUMBER_COLORS
from src.race_map.excel_loader import discover_input_files, load_workbook
from src.race_map.export_service import analysis_payload, export_race_json, export_race_png
from src.race_map.race_analyzer import RaceAnalyzer
from src.race_map.race_repository import RaceRepository

st.set_page_config(page_title="Keiba AI Studio レース質マップ", page_icon="🏇", layout="wide")
css = (Path(__file__).parent / "assets" / "style.css").read_text(encoding="utf-8")
st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


@st.cache_data(show_spinner="Excelを読み込んでいます…")
def cached_load(path: str, modified: float) -> dict[str, pd.DataFrame]:
    # 更新時刻をキャッシュキーへ含め、原本更新時だけ再読込します。
    return load_workbook(path)


def map_figure(analysis):
    # 参考画像に合わせ、青から橙へ移る4象限背景と馬型マーカーで表現します。
    import numpy as np
    from matplotlib.colors import LinearSegmentedColormap
    from matplotlib import font_manager
    # 日本語ラベルが四角へ文字化けしないよう、Windows標準の日本語フォントを優先します。
    available_fonts = {font.name for font in font_manager.fontManager.ttflist}
    for font_name in ("Yu Gothic", "Meiryo", "Noto Sans CJK JP"):
        if font_name in available_fonts:
            plt.rcParams["font.family"] = font_name
            break
    figure, axis = plt.subplots(figsize=(12, 6.2), facecolor="#071426")
    gradient = np.tile(np.linspace(0, 1, 600), (320, 1))
    cmap = LinearSegmentedColormap.from_list("race-map", ["#102b78", "#29285c", "#69361e"])
    axis.imshow(gradient, extent=(-110, 110, -5, 105), origin="lower", aspect="auto", cmap=cmap, alpha=.82)
    axis.axhline(50, color="white", alpha=.8, linewidth=1.4); axis.axvline(0, color="white", alpha=.8, linewidth=1.4)
    for horse in analysis.horses:
        is_main = horse.horse_number == analysis.main_horse_number
        color = HORSE_NUMBER_COLORS.get(horse.horse_number, "#999")
        axis.scatter(horse.power_type_score, horse.position_score, s=1200 if is_main else 820, c=color, marker="$♞$", edgecolors="#ffca36" if is_main else "white", linewidths=3.5 if is_main else 1.3, zorder=4)
        axis.text(horse.power_type_score + 1, horse.position_score - 1, str(horse.horse_number), ha="center", va="center", weight="bold", fontsize=11, color="black" if horse.horse_number in (1, 5, 9, 11, 12, 14, 17) else "white", zorder=5)
        if is_main:
            axis.text(horse.power_type_score, horse.position_score + 12, "♛", ha="center", color="#ffd34d", fontsize=24, weight="bold", zorder=6)
    axis.set(xlim=(-110, 110), ylim=(-5, 105))
    axis.set_xticks([]); axis.set_yticks([])
    axis.text(0, 103, "先行", ha="center", va="top", color="white", fontsize=13, bbox=dict(boxstyle="round,pad=.35", facecolor="#102039", edgecolor="#d1a94e"))
    axis.text(0, -3, "後方", ha="center", va="bottom", color="white", fontsize=13, bbox=dict(boxstyle="round,pad=.35", facecolor="#102039", edgecolor="#d1a94e"))
    axis.text(-108, 51, "瞬発力型", ha="left", va="center", color="white", fontsize=13, bbox=dict(boxstyle="round,pad=.35", facecolor="#102039", edgecolor="#d1a94e"))
    axis.text(108, 51, "持続力型", ha="right", va="center", color="white", fontsize=13, bbox=dict(boxstyle="round,pad=.35", facecolor="#102039", edgecolor="#d1a94e"))
    for spine in axis.spines.values():
        spine.set_color("#c89942"); spine.set_linewidth(1.2)
    return figure


def horse_line(horse, badge: str) -> str:
    # 馬番・馬名・役割バッジを一行のカード要素へ整形します。
    return f'<div class="horse-line"><span class="number">{horse.horse_number}</span><span class="horse-name">{escape(horse.horse_name)}</span><span class="role">{escape(badge)}</span></div>'


files = discover_input_files(ROOT / "data" / "output")
st.title("🏇 レース質・脚質ポジションマップ")
if not files:
    st.error("data/outputに対象Excelがありません。")
    st.stop()
with st.sidebar:
    st.markdown('<div class="brand"><span class="brand-horse">♞</span><div><strong>Keiba AI Studio</strong><small>レース質・脚質ポジションマップ</small></div></div>', unsafe_allow_html=True)
    st.markdown('<div class="nav active">♞　レース質マップ</div><div class="nav">☷　レース一覧</div><div class="nav">⚑　買い目候補</div><div class="nav">⚠　危険馬</div><div class="nav">⚙　設定</div>', unsafe_allow_html=True)
    st.markdown('<p class="side-label">開催日・入力ファイル</p>', unsafe_allow_html=True)
    selected_file = st.selectbox("入力ファイル", files, format_func=lambda path: path.name)
    display_mode = st.radio("表示モード", ["標準", "シンプル", "詳細分析"], horizontal=True)
    if st.button("再読込", use_container_width=True):
        cached_load.clear(); st.rerun()

sheets = cached_load(str(selected_file), selected_file.stat().st_mtime)
repository = RaceRepository(sheets["今走レース情報"])
race_id = st.sidebar.selectbox("レース", repository.race_ids)
analysis = RaceAnalyzer().analyze(repository.get(race_id), repository.validation.columns, repository.validation.warnings)
st.markdown(f'<div class="race-header"><div><span class="race-title">{escape(analysis.racecourse)}{analysis.race_number}R</span><span class="start-time">{escape(analysis.start_time)}発走</span></div><p>{escape(analysis.race_name)} ／ {escape(analysis.course)} ／ {escape(analysis.track_condition)} ／ {escape(analysis.race_class)} ／ {len(analysis.horses)}頭</p></div>', unsafe_allow_html=True)
left, right = st.columns([.34, .66])
with left:
    main = analysis.horse(analysis.main_horse_number)
    quality_items = "".join(f"<li>{escape(label)}</li>" for label in analysis.race_quality_labels)
    st.markdown(f'<section class="analysis-card quality"><h3>今回のレース質 <span>ⓘ</span></h3><ul>{quality_items}</ul><div class="mini-metrics"><span>ペース<strong>{escape(analysis.pace_type)}</strong></span><span>確信度<strong>{analysis.pace_confidence:.0f}%</strong></span><span>混戦度<strong>{analysis.confusion_score:.0f}</strong></span></div></section>', unsafe_allow_html=True)
    if main:
        st.markdown(f'<section class="analysis-card main-card"><div class="main-row"><b>本命</b><span class="number">{main.horse_number}</span><strong>{escape(main.horse_name)}</strong><i>♛</i></div><div class="main-stats"><span>予想順位<strong>{main.prediction_rank or "－"}位</strong></span><span>Score<strong>{main.score or "－"}</strong></span><span>馬券内率<strong>{main.place_probability or "－"}</strong></span><span>適合度<strong>{main.race_fit_score:.0f}</strong></span></div></section>', unsafe_allow_html=True)
    if analysis.main_change_reason:
        st.warning(analysis.main_change_reason)
    opponent_html = "".join(horse_line(analysis.horse(number), "展開" if index == 1 else "安定" if index == 0 else "能力") for index, number in enumerate(analysis.opponent_numbers))
    st.markdown(f'<section class="analysis-card opponent-card"><h3>相手</h3>{opponent_html}</section>', unsafe_allow_html=True)
    if display_mode == "標準":
        value_html = "".join(horse_line(analysis.horse(number), "穴") for number in analysis.value_horse_numbers) or '<div class="empty">該当なし</div>'
        danger_html = "".join(horse_line(analysis.horse(number), "⚠") for number in analysis.danger_horse_numbers) or '<div class="empty">該当なし</div>'
        st.markdown(f'<div class="subcards"><section class="analysis-card value-card"><h3>穴馬</h3>{value_html}</section><section class="analysis-card danger-card"><h3>危険人気馬</h3>{danger_html}</section></div>', unsafe_allow_html=True)
with right:
    st.markdown('<div class="map-shell">', unsafe_allow_html=True)
    st.pyplot(map_figure(analysis), use_container_width=True)
    st.markdown('<div class="map-legend"><span>♛ 本命馬</span><span class="blue">♞ 相手候補</span><span class="red">♞ 危険人気馬</span><span class="gray">♞ その他の馬</span></div><p class="map-note">※このマップは強さの順位ではありません。上ほど前方、左ほど瞬発力型、右ほど持続力型です。</p></div>', unsafe_allow_html=True)
if display_mode != "シンプル":
    st.markdown('<h3 class="detail-title">各馬詳細</h3>', unsafe_allow_html=True)
    rows = [{"馬番": h.horse_number, "馬名": h.horse_name, "予想順位": h.prediction_rank, "score": h.score, "脚質": h.style_name,
             "位置": round(h.position_score, 1), "能力タイプ": round(h.power_type_score, 1), "レース質適合度": round(h.race_fit_score, 1), "危険度": round(h.danger_score, 1)} for h in analysis.horses]
    st.dataframe(pd.DataFrame(rows).sort_values(["予想順位", "馬番"], na_position="last"), use_container_width=True, hide_index=True)
if display_mode == "詳細分析":
    st.json(analysis_payload(analysis, selected_file))
output_base = ROOT / "data" / "race_map_output" / analysis.date
save_cols = st.columns(2)
if save_cols[0].button("JSON保存", use_container_width=True):
    path = export_race_json(analysis, selected_file, output_base / "json" / f"{analysis.race_id}.json"); st.success(f"保存しました: {path}")
if save_cols[1].button("PNG保存", use_container_width=True):
    path = export_race_png(analysis, output_base / "images" / f"{analysis.race_id}_{analysis.racecourse}{analysis.race_number}R.png"); st.success(f"保存しました: {path}")
for warning in analysis.warnings:
    st.warning(warning)
