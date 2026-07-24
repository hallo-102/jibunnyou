import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const pageSource = await readFile(new URL("../app/page.tsx", import.meta.url), "utf8");
const layoutSource = await readFile(new URL("../app/layout.tsx", import.meta.url), "utf8");
const actionToolbarSource = await readFile(
  new URL("../app/components/ActionToolbar.tsx", import.meta.url),
  "utf8"
);
const themeControlSource = await readFile(
  new URL("../app/components/ThemeControl.tsx", import.meta.url),
  "utf8"
);
const workspaceNavSource = await readFile(
  new URL("../app/components/WorkspaceNav.tsx", import.meta.url),
  "utf8"
);
const aiAnalysisPanelsSource = await readFile(
  new URL("../app/components/AiAnalysisPanels.tsx", import.meta.url),
  "utf8"
);
const chatgptManualPanelSource = await readFile(
  new URL("../app/components/ChatgptManualPanel.tsx", import.meta.url),
  "utf8"
);
const betPlanningPanelSource = await readFile(
  new URL("../app/components/BetPlanningPanel.tsx", import.meta.url),
  "utf8"
);
const operationsPanelsSource = await readFile(
  new URL("../app/components/OperationsPanels.tsx", import.meta.url),
  "utf8"
);
const performancePanelSource = await readFile(
  new URL("../app/components/PerformancePanel.tsx", import.meta.url),
  "utf8"
);
const raceWorkspaceSource = await readFile(
  new URL("../app/components/RaceWorkspace.tsx", import.meta.url),
  "utf8"
);
const workspaceHeaderSource = await readFile(
  new URL("../app/components/WorkspaceHeader.tsx", import.meta.url),
  "utf8"
);
const cssSource = await readFile(new URL("../app/globals.css", import.meta.url), "utf8");
const primaryUiSource = `${pageSource}\n${actionToolbarSource}\n${aiAnalysisPanelsSource}\n${betPlanningPanelSource}\n${operationsPanelsSource}\n${performancePanelSource}\n${raceWorkspaceSource}\n${workspaceHeaderSource}`;

test("主要6領域へのナビゲーションと次操作を表示する", () => {
  const anchors = [
    "dashboard",
    "race-workspace",
    "ai-analysis",
    "bet-planning",
    "performance",
    "operations"
  ];

  for (const anchor of anchors) {
    assert.match(primaryUiSource, new RegExp(`(?:id=|href=)[{\"'#]*${anchor}`));
  }
  assert.match(pageSource, /const nextAction = useMemo/);
  assert.match(workspaceHeaderSource, /aria-label="処理順"/);
});

test("処理中、失敗、キーボード操作を意味付きで伝える", () => {
  assert.match(pageSource, /aria-busy=\{isBusy\}/);
  assert.match(workspaceHeaderSource, /role="alert"/);
  assert.match(primaryUiSource, /role="status"/);
  assert.match(workspaceHeaderSource, /className="skipLink" href=\{`#\$\{routeAnchor\}`\}/);
  assert.match(cssSource, /:focus-visible/);
});

test("買い目画面は候補保存と手動記録だけを案内する", () => {
  assert.match(betPlanningPanelSource, /自動投票は無効/);
  assert.match(betPlanningPanelSource, /外部購入後の手動記録/);
  assert.doesNotMatch(primaryUiSource, /自動投票を実行|JRAへ投票|IPATへ送信/);
});

test("小画面と印刷時の退避レイアウトを持つ", () => {
  assert.match(cssSource, /@media \(max-width: 980px\)/);
  assert.match(cssSource, /@media print/);
  assert.match(cssSource, /overflow-x: auto/);
});

test("旧API利用枠エラーをChatGPT手動予想の案内へ変換する", () => {
  assert.match(pageSource, /insufficient_quota/);
  assert.match(pageSource, /旧API予想方式は廃止/);
  assert.match(pageSource, /ChatGPT手動予想を使用/);
});

test("開催日欄からoutputの特徴量Excelを選択して取り込む", () => {
  assert.match(actionToolbarSource, /開催日・Excel/);
  assert.match(pageSource, /\/v1\/race-workbooks/);
  assert.match(pageSource, /\/v1\/race-workbooks\/select/);
  assert.match(actionToolbarSource, /読込済み/);
  assert.match(actionToolbarSource, /未読込/);
});

test("レース一覧と出走馬一覧はデスクトップで同じ行の左右に並ぶ", () => {
  const anchorIndex = raceWorkspaceSource.indexOf('id="race-workspace"');
  const workspaceIndex = raceWorkspaceSource.indexOf('className="workspace"');

  assert.ok(anchorIndex >= 0 && anchorIndex < workspaceIndex);
  assert.match(cssSource, /\.workspace\s*\{[^}]*display:\s*grid;[^}]*grid-template-columns:[^;]+;[^}]*align-items:\s*start;/s);
  assert.match(cssSource, /@media \(max-width: 980px\)[\s\S]*?\.workspace,[\s\S]*?grid-template-columns:\s*1fr;/);
});

test("専用通知センターで未読件数と既読操作を提供する", () => {
  assert.match(workspaceHeaderSource, /通知センター/);
  assert.match(pageSource, /\/v1\/notifications/);
  assert.match(workspaceHeaderSource, /すべて既読/);
  assert.match(workspaceHeaderSource, /未読に戻す/);
  assert.match(workspaceHeaderSource, /aria-controls="notification-center"/);
  assert.match(cssSource, /\.notificationBadge/);
  assert.match(cssSource, /\.notificationList/);
});

test("ライト・ダーク・OS設定のテーマを端末へ保存する", () => {
  assert.match(workspaceHeaderSource, /<ThemeControl \/>/);
  assert.match(themeControlSource, /keiba-ai-studio-theme/);
  assert.match(themeControlSource, /prefers-color-scheme: dark/);
  assert.match(themeControlSource, /aria-label="表示テーマ"/);
  assert.match(themeControlSource, /OS設定/);
  assert.match(themeControlSource, /ライト/);
  assert.match(themeControlSource, /ダーク/);
  assert.match(layoutSource, /themeInitializationScript/);
  assert.match(layoutSource, /suppressHydrationWarning/);
  assert.match(cssSource, /:root\[data-theme="dark"\]/);
  assert.match(cssSource, /color-scheme:\s*dark/);
});

test("主要6領域をURLで分離し現在位置をナビゲーションへ表示する", async () => {
  const routes = ["races", "analysis", "bets", "performance", "operations"];

  for (const route of routes) {
    const routeSource = await readFile(new URL(`../app/${route}/page.tsx`, import.meta.url), "utf8");
    assert.match(routeSource, /export \{ default \} from "\.\.\/page"/);
    assert.match(workspaceNavSource, new RegExp(`/${route}`));
  }
  assert.match(workspaceHeaderSource, /<WorkspaceNav \/>/);
  assert.match(workspaceNavSource, /usePathname/);
  assert.match(workspaceNavSource, /aria-current=/);
  assert.match(workspaceNavSource, /workspaceHref/);
  assert.match(cssSource, /\.workspaceNav a\[aria-current="page"\]/);
});

test("各routeでは担当領域だけを表示して見出しとskip先を切り替える", () => {
  assert.match(pageSource, /usePathname/);
  assert.match(pageSource, /data-workspace-route=\{routePresentation\.key\}/);
  assert.match(pageSource, /routeTitle=\{routePresentation\.title\}/);
  assert.match(pageSource, /routeAnchor=\{routePresentation\.anchor\}/);
  assert.match(workspaceHeaderSource, /<h1>\{routeTitle\}<\/h1>/);
  assert.match(workspaceHeaderSource, /href=\{`#\$\{routeAnchor\}`\}/);
  assert.match(pageSource, /data-route-section="dashboard"/);
  assert.match(raceWorkspaceSource, /data-route-section="races"/);
  assert.match(aiAnalysisPanelsSource, /data-route-section="analysis"/);
  assert.match(betPlanningPanelSource, /data-route-section="bets"/);
  assert.match(performancePanelSource, /data-route-section="performance"/);
  assert.match(operationsPanelsSource, /data-route-section="operations"/);
  assert.match(cssSource, /data-workspace-route="analysis"/);
  assert.match(cssSource, /data-route-section~="analysis"/);
});

test("日単位の一覧APIをrouteごとに必要な範囲だけ取得する", () => {
  assert.match(pageSource, /async function loadRouteData\(routeKey: string, raceDate: string\)/);
  assert.match(pageSource, /routeKey === "dashboard" \|\| routeKey === "races"/);
  assert.match(pageSource, /routeKey === "dashboard" \|\| routeKey === "performance"/);
  assert.match(pageSource, /routeKey === "races" \|\| routeKey === "operations"/);
  assert.match(pageSource, /routeKey === "operations"/);
  assert.match(pageSource, /loadRouteData\(activeRouteKey, nextDate\)/);
  assert.match(pageSource, /const shouldPollCollections = activeRouteKey === "races"/);
  assert.match(pageSource, /useEffect\(\(\) => \{\s*void refreshAll\(\);\s*\}, \[pathname\]\)/s);
  assert.match(pageSource, /async function loadSelectedRaceData\(raceId: string\)/);
  assert.match(pageSource, /targetRaceId && targetRaceId === selectedRaceId/);
  assert.match(pageSource, /void loadSelectedRaceData\(selectedRaceId\)/);
});

test("取得状況・ジョブ・品質を運用コンポーネントへ分離する", () => {
  assert.match(pageSource, /<OperationsPanels/);
  assert.match(pageSource, /collections=\{visibleCollections\}/);
  assert.match(pageSource, /onRetryCollection=\{retryCollection\}/);
  assert.match(operationsPanelsSource, /<h2>取得状況<\/h2>/);
  assert.match(operationsPanelsSource, /<h2>ジョブ<\/h2>/);
  assert.match(operationsPanelsSource, /<h2>品質チェック<\/h2>/);
  assert.match(operationsPanelsSource, /onRetryCollection\(collection\.job_run_id\)/);
  assert.match(operationsPanelsSource, /disabled=\{isBusy\}/);
});

test("成績分析をcontrolledな専用コンポーネントへ分離する", () => {
  assert.match(pageSource, /<PerformancePanel/);
  assert.match(pageSource, /analytics=\{analytics\}/);
  assert.match(pageSource, /onRefresh=\{\(\) => void loadAnalytics\(selectedDate\)\}/);
  assert.match(pageSource, /onScopeChange=\{setAnalyticsScope\}/);
  assert.match(performancePanelSource, /<h2>成績分析<\/h2>/);
  assert.match(performancePanelSource, /onScopeChange\(event\.target\.value/);
  assert.match(performancePanelSource, /onSourceChange\(event\.target\.value\)/);
  assert.match(performancePanelSource, /onBetTypeChange\(event\.target\.value\)/);
  assert.match(performancePanelSource, /onGroupChange\(event\.target\.value\)/);
  assert.match(performancePanelSource, /aria-label="成績集計期間"/);
  assert.match(performancePanelSource, /aria-label="成績集計ソース"/);
  assert.match(performancePanelSource, /aria-label="成績集計券種"/);
  assert.match(performancePanelSource, /aria-label="成績集計内訳"/);
  assert.match(performancePanelSource, /raceResult\?\.finish_order/);
});

test("旧API AI履歴を読み取り専用コンポーネントへ分離する", () => {
  assert.match(pageSource, /<AiAnalysisPanels/);
  assert.match(pageSource, /independentAnalysis=\{independentAnalysis\}/);
  assert.match(pageSource, /integrationAnalysis=\{integrationAnalysis\}/);
  assert.match(pageSource, /aiDiff=\{aiDiff\}/);
  assert.match(pageSource, /formatAiJobFailure=\{formatAiJobFailure\}/);
  assert.match(aiAnalysisPanelsSource, /id="ai-analysis"/);
  assert.match(aiAnalysisPanelsSource, /<h2>過去のAPI独立AI履歴（読み取り専用）<\/h2>/);
  assert.match(aiAnalysisPanelsSource, /<h2>過去のAPI比較・統合履歴（読み取り専用）<\/h2>/);
  assert.match(aiAnalysisPanelsSource, /<h2>旧AI補正（互換表示）<\/h2>/);
  assert.match(aiAnalysisPanelsSource, /independentAnalysis\.output_locked/);
  assert.match(aiAnalysisPanelsSource, /integrationAnalysis\.comparison_locked/);
  assert.match(aiAnalysisPanelsSource, /integrationAnalysis\.integration_output_hash/);
  assert.match(aiAnalysisPanelsSource, /formatAiJobFailure\(independentAnalysis\.error_message\)/);
});

test("ChatGPT手動予想は生成・編集・コピー・手動回答保存を提供する", () => {
  assert.match(pageSource, /<ChatgptManualPanel/);
  assert.match(pageSource, /pythonPredictionReady=\{predictionResults\.length >= 2\}/);
  assert.match(chatgptManualPanelSource, /ChatGPT用プロンプトを作成/);
  assert.match(chatgptManualPanelSource, /コピーしてChatGPTを開く/);
  assert.match(chatgptManualPanelSource, /navigator\.clipboard\.writeText/);
  assert.match(chatgptManualPanelSource, /window\.open/);
  assert.match(chatgptManualPanelSource, /ChatGPT予想結果を貼り付け/);
  assert.match(chatgptManualPanelSource, /予想結果を保存/);
  assert.match(chatgptManualPanelSource, /\/v1\/chatgpt\/responses/);
  assert.match(chatgptManualPanelSource, /Ctrl\+V/);
});

test("ChatGPT手動予想はWeb最新情報の独立評価であることを案内する", () => {
  assert.match(chatgptManualPanelSource, /最新情報をWeb調査/);
  assert.match(chatgptManualPanelSource, /Python予想への賛否と独立した最終予想/);
  assert.match(chatgptManualPanelSource, /Python予想の再説明が目的ではありません/);
  assert.match(chatgptManualPanelSource, /Web検索が有効/);
});

test("買い目条件・安全案内・候補操作をcontrolledな専用コンポーネントへ分離する", () => {
  assert.match(pageSource, /<BetPlanningPanel/);
  assert.match(pageSource, /onGenerate=\{\(\) => void generateBetPreviews\(\)\}/);
  assert.match(pageSource, /onStatusChange=\{\(betId, status, reason\) => void updateBetCandidateStatus/);
  assert.match(pageSource, /canUseIntegratedAi=\{Boolean\(integrationAnalysis\?\.integration_locked\)\}/);
  assert.match(betPlanningPanelSource, /id="bet-planning"/);
  assert.match(betPlanningPanelSource, /aria-label="買い目予想ソース"/);
  assert.match(betPlanningPanelSource, /aria-label="買い目券種"/);
  assert.match(betPlanningPanelSource, /aria-label="買い目方式"/);
  assert.match(betPlanningPanelSource, /aria-label="買い目1点金額"/);
  assert.match(betPlanningPanelSource, /aria-label="買い目レース上限"/);
  assert.match(betPlanningPanelSource, /aria-label="買い目1日上限"/);
  assert.match(betPlanningPanelSource, /aria-label="買い目最大点数"/);
  assert.match(betPlanningPanelSource, /onStatusChange\(bet\.id, "purchased"/);
});

test("レース一覧・処理状況・出走馬比較表を専用コンポーネントへ分離する", () => {
  assert.match(pageSource, /<RaceWorkspace/);
  assert.match(pageSource, /visibleRaces=\{visibleRaces\}/);
  assert.match(pageSource, /onSelectedRaceChange=\{setSelectedRaceId\}/);
  assert.match(pageSource, /onEntrySort=\{updateEntrySort\}/);
  assert.match(raceWorkspaceSource, /<h2>レース一覧<\/h2>/);
  assert.match(raceWorkspaceSource, /<h2>出走馬<\/h2>/);
  assert.match(raceWorkspaceSource, /predictionByRaceId\.get\(race\.race_id\)/);
  assert.match(raceWorkspaceSource, /integrationByRaceId\.get\(race\.race_id\)/);
  assert.match(raceWorkspaceSource, /onSelectedRaceChange\(race\.race_id\)/);
  assert.match(raceWorkspaceSource, /function EntrySortHeader/);
  assert.match(raceWorkspaceSource, /predictionByHorseNo\.get\(entry\.horse_no\)/);
  assert.match(raceWorkspaceSource, /integratedByHorseNo\.get\(entry\.horse_no\)/);
});

test("共通ヘッダー・通知・次操作・実行ツールバーを専用コンポーネントへ分離する", () => {
  assert.match(pageSource, /<WorkspaceHeader/);
  assert.match(pageSource, /<ActionToolbar/);
  assert.match(pageSource, /onRefresh=\{\(\) => void refreshAll\(\)\}/);
  assert.match(pageSource, /onRunJob=\{\(jobType\) => void runJob\(jobType\)\}/);
  assert.match(pageSource, /onWorkbookSelection=\{\(fileName\) => void handleWorkbookSelection\(fileName\)\}/);
  assert.match(workspaceHeaderSource, /API \{health\?\.status \|\| "loading"\}/);
  assert.match(workspaceHeaderSource, /aria-label="通知センター"/);
  assert.match(workspaceHeaderSource, /nextAction\.label/);
  assert.match(actionToolbarSource, /aria-label="開催日Excelファイル"/);
  assert.match(actionToolbarSource, /aria-label="レース検索"/);
  assert.match(actionToolbarSource, /onRunJob\("prediction\.python"\)/);
  assert.doesNotMatch(actionToolbarSource, /ai\.independent|onRunComparisonIntegration/);
  assert.match(actionToolbarSource, /自動投票は行いません/);
});
