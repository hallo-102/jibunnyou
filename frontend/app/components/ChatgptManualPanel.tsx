"use client";

import { useEffect, useMemo, useRef, useState } from "react";


const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "/api";

type PromptResult = {
  history_id: string;
  race_id: string;
  prompt_text: string;
  prompt_length: number;
  warning_threshold: number;
  length_warning: boolean;
  chatgpt_url: string;
};

type ManualHistory = {
  id: string;
  race_id: string;
  source: "chatgpt_manual";
  prompt_text: string;
  response_text?: string | null;
  created_at: string;
  updated_at: string;
};

type ChatgptManualPanelProps = {
  onPromptReady: (ready: boolean) => void;
  onResponseSaved: (saved: boolean) => void;
  selectedRaceId: string;
  pythonPredictionReady: boolean;
};

async function requestJson<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    cache: "no-store",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options?.headers || {})
    }
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => null) as { detail?: string } | null;
    throw new Error(payload?.detail || `処理に失敗しました（HTTP ${response.status}）`);
  }
  return response.json() as Promise<T>;
}

export default function ChatgptManualPanel({
  onPromptReady,
  onResponseSaved,
  selectedRaceId,
  pythonPredictionReady
}: ChatgptManualPanelProps) {
  const [historyId, setHistoryId] = useState("");
  const [prompt, setPrompt] = useState("");
  const [responseText, setResponseText] = useState("");
  const [chatgptUrl, setChatgptUrl] = useState("https://chatgpt.com/");
  const [warningThreshold, setWarningThreshold] = useState(50000);
  const [history, setHistory] = useState<ManualHistory[]>([]);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [isBusy, setIsBusy] = useState(false);
  const promptRef = useRef<HTMLTextAreaElement>(null);
  const promptLength = useMemo(() => prompt.length, [prompt]);

  useEffect(() => {
    setHistoryId("");
    setPrompt("");
    setResponseText("");
    setMessage("");
    setError("");
    onPromptReady(false);
    onResponseSaved(false);
    if (!selectedRaceId) {
      setHistory([]);
      return;
    }
    void loadHistory(selectedRaceId);
  }, [selectedRaceId]);

  async function loadHistory(raceId: string) {
    try {
      const records = await requestJson<ManualHistory[]>(
        `/v1/races/${encodeURIComponent(raceId)}/chatgpt-predictions`
      );
      setHistory(records);
    } catch (err) {
      setError(err instanceof Error ? err.message : "過去のChatGPT予想履歴を読み込めませんでした");
    }
  }

  async function generatePrompt(): Promise<PromptResult | null> {
    if (!selectedRaceId) {
      setError("対象レースを選択してください");
      return null;
    }
    if (!pythonPredictionReady) {
      setError("Python予想が未実行です。先にPython予想を実行してください");
      return null;
    }
    setIsBusy(true);
    setError("");
    setMessage("");
    try {
      const result = await requestJson<PromptResult>("/v1/chatgpt/prompts", {
        method: "POST",
        body: JSON.stringify({ race_id: selectedRaceId })
      });
      setHistoryId(result.history_id);
      setPrompt(result.prompt_text);
      onPromptReady(true);
      setChatgptUrl(result.chatgpt_url);
      setWarningThreshold(result.warning_threshold);
      setMessage(
        result.length_warning
          ? `プロンプトを作成しました。${result.prompt_length.toLocaleString()}文字あるため、内容を確認してください`
          : "ChatGPT用プロンプトを作成しました。内容を確認・編集できます"
      );
      await loadHistory(selectedRaceId);
      return result;
    } catch (err) {
      setError(err instanceof Error ? err.message : "ChatGPT用プロンプトの生成に失敗しました");
      return null;
    } finally {
      setIsBusy(false);
    }
  }

  async function copyPrompt(text: string = prompt): Promise<boolean> {
    if (!text.trim()) {
      setError("コピーするプロンプトが空です");
      return false;
    }
    try {
      await navigator.clipboard.writeText(text);
      setError("");
      setMessage("ChatGPT用プロンプトをクリップボードへコピーしました");
      return true;
    } catch {
      promptRef.current?.focus();
      promptRef.current?.select();
      setError(
        "クリップボードへコピーできませんでした。確認欄を選択したので、Ctrl+Cで手動コピーしてください"
      );
      return false;
    }
  }

  async function copyAndOpenChatgpt() {
    if (!selectedRaceId) {
      setError("対象レースを選択してください");
      return;
    }
    const popup = window.open("about:blank", "_blank");
    if (popup) {
      // 新しいタブから元画面を操作できないようにしつつ、非同期生成後も同じタブを利用する。
      popup.opener = null;
    }
    const result = prompt.trim() ? null : await generatePrompt();
    const targetPrompt = result?.prompt_text || prompt;
    const targetUrl = result?.chatgpt_url || chatgptUrl;
    if (!targetPrompt.trim()) {
      popup?.close();
      return;
    }
    const copied = await copyPrompt(targetPrompt);
    if (!copied) {
      popup?.close();
      return;
    }
    if (popup) {
      popup.location.href = targetUrl;
    } else {
      window.open(targetUrl, "_blank", "noopener,noreferrer");
    }
    setMessage(
      "ChatGPT用プロンプトをコピーし、ChatGPTを開きました。入力欄でCtrl+Vを押し、内容を確認してから手動で送信してください"
    );
  }

  function openChatgpt() {
    const opened = window.open(chatgptUrl, "_blank");
    if (!opened) {
      setError(`ChatGPTをブラウザで開けませんでした。手動で開いてください: ${chatgptUrl}`);
      return;
    }
    opened.opener = null;
  }

  async function saveResponse() {
    if (!selectedRaceId) {
      setError("対象レースを選択してください");
      return;
    }
    if (!responseText.trim()) {
      setError("ChatGPTの回答が空欄です");
      return;
    }
    if (!prompt.trim()) {
      setError("保存するプロンプトが空です。先にプロンプトを作成してください");
      return;
    }
    setIsBusy(true);
    setError("");
    try {
      const saved = await requestJson<ManualHistory>("/v1/chatgpt/responses", {
        method: "POST",
        body: JSON.stringify({
          race_id: selectedRaceId,
          history_id: historyId || null,
          prompt_text: prompt,
          response_text: responseText
        })
      });
      setHistoryId(saved.id);
      onResponseSaved(true);
      setMessage("ChatGPT予想結果を対象レースへ保存しました");
      await loadHistory(selectedRaceId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "ChatGPT予想結果の保存に失敗しました");
    } finally {
      setIsBusy(false);
    }
  }

  function clearInputs() {
    setHistoryId("");
    setPrompt("");
    setResponseText("");
    onPromptReady(false);
    onResponseSaved(false);
    setMessage("入力内容をクリアしました");
    setError("");
  }

  return (
    <section data-route-section="analysis" id="chatgpt-manual">
      <div className="sectionHeader">
        <h2>ChatGPT手動予想</h2>
        <span>APIキー不要・送信は手動</span>
      </div>
      <div className="chatgptManualPanel">
        <p className="manualFlowNote">
          この機能は、ChatGPTに最新情報をWeb調査させ、Python予想への賛否と独立した最終予想を
          作成するためのプロンプトを生成します。Python予想の再説明が目的ではありません。
          貼り付け後はChatGPTのWeb検索が有効になっていることを確認し、送信と回答の取り込みは
          手動で行ってください。
        </p>
        <div className="manualActions">
          <button disabled={isBusy || !selectedRaceId || !pythonPredictionReady} onClick={() => void generatePrompt()} type="button">
            {prompt ? "プロンプトを再生成" : "ChatGPT用プロンプトを作成"}
          </button>
          <button disabled={isBusy || !prompt.trim()} onClick={() => void copyPrompt()} type="button">
            プロンプトをコピー
          </button>
          <button disabled={isBusy || !selectedRaceId || !pythonPredictionReady} onClick={() => void copyAndOpenChatgpt()} type="button">
            コピーしてChatGPTを開く
          </button>
          <button disabled={isBusy} onClick={openChatgpt} type="button">ChatGPTを開く</button>
          <button disabled={isBusy || (!prompt && !responseText)} onClick={clearInputs} type="button">入力内容をクリア</button>
        </div>
        {!pythonPredictionReady && selectedRaceId && (
          <p className="manualWarning">Python予想が未実行です。先にPython予想を実行してください。</p>
        )}
        <label className="manualTextField">
          <span>
            プロンプト確認・編集
            <strong className={promptLength > warningThreshold ? "lengthWarning" : ""}>
              {promptLength.toLocaleString()}文字
            </strong>
          </span>
          <textarea
            ref={promptRef}
            value={prompt}
            onChange={(event) => setPrompt(event.target.value)}
            placeholder="対象レースを選択し、「ChatGPT用プロンプトを作成」を押してください"
            rows={18}
          />
        </label>
        {promptLength > warningThreshold && (
          <p className="manualWarning">
            プロンプトが{warningThreshold.toLocaleString()}文字を超えています。不要な箇所を編集して短くすることを推奨します。
          </p>
        )}
        <label className="manualTextField">
          <span>ChatGPT予想結果を貼り付け</span>
          <textarea
            value={responseText}
            onChange={(event) => setResponseText(event.target.value)}
            placeholder="ChatGPTの回答全文をここへ手動で貼り付けてください"
            rows={12}
          />
        </label>
        <div className="manualActions">
          <button disabled={isBusy || !responseText.trim()} onClick={() => void saveResponse()} type="button">
            予想結果を保存
          </button>
        </div>
        {message && <p className="manualSuccess" role="status">{message}</p>}
        {error && <p className="manualError" role="alert">{error}</p>}

        <details className="manualHistory">
          <summary>過去に保存したChatGPT予想結果（{history.filter((item) => item.response_text).length}件）</summary>
          {history.filter((item) => item.response_text).length ? (
            history.filter((item) => item.response_text).map((item) => (
              <article key={item.id}>
                <strong>{new Date(item.updated_at).toLocaleString("ja-JP")}</strong>
                <pre>{item.response_text}</pre>
              </article>
            ))
          ) : (
            <p>保存済みのChatGPT予想結果はありません。</p>
          )}
        </details>
      </div>
    </section>
  );
}
