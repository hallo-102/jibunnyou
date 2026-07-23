"use client";

import { Monitor, Moon, Sun } from "lucide-react";
import { useEffect, useState } from "react";

const THEME_STORAGE_KEY = "keiba-ai-studio-theme";

type ThemePreference = "system" | "light" | "dark";

export default function ThemeControl() {
  const [themePreference, setThemePreference] = useState<ThemePreference>("system");

  useEffect(() => {
    // 前回選択した表示設定を復元する。
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === "light" || stored === "dark" || stored === "system") {
      setThemePreference(stored);
    }
  }, []);

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const applyTheme = () => {
      // OS設定時だけシステムの配色変更へ追従する。
      document.documentElement.dataset.theme = themePreference === "system"
        ? (media.matches ? "dark" : "light")
        : themePreference;
    };

    applyTheme();
    if (themePreference === "system") {
      media.addEventListener("change", applyTheme);
      return () => media.removeEventListener("change", applyTheme);
    }
    return undefined;
  }, [themePreference]);

  function updateThemePreference(preference: ThemePreference) {
    // 明示的な選択はブラウザーへ保存し、次回起動時にも使用する。
    setThemePreference(preference);
    window.localStorage.setItem(THEME_STORAGE_KEY, preference);
  }

  return (
    <label className="themeControl">
      {themePreference === "dark" ? (
        <Moon size={16} aria-hidden="true" />
      ) : themePreference === "light" ? (
        <Sun size={16} aria-hidden="true" />
      ) : (
        <Monitor size={16} aria-hidden="true" />
      )}
      <span>テーマ</span>
      <select
        aria-label="表示テーマ"
        value={themePreference}
        onChange={(event) => updateThemePreference(event.target.value as ThemePreference)}
      >
        <option value="system">OS設定</option>
        <option value="light">ライト</option>
        <option value="dark">ダーク</option>
      </select>
    </label>
  );
}
