"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import ActionToolbar, { type RaceWorkbook } from "./components/ActionToolbar";
import ChatgptManualPanel from "./components/ChatgptManualPanel";
import AiAnalysisPanels, {
  type AiBetStrategy,
  type AiEvaluation,
  type AiIntegrationAnalysis,
  type IndependentAiAnalysis
} from "./components/AiAnalysisPanels";
import BetPlanningPanel, {
  type BetCandidate,
  type BetGenerationResult,
  type BetSourceMode,
  type BetStrategyMode,
  type BetType
} from "./components/BetPlanningPanel";
import OperationsPanels, {
  type CollectionRun,
  type Issue,
  type Job
} from "./components/OperationsPanels";
import PerformancePanel, {
  type AnalyticsSummary,
  type RaceResult
} from "./components/PerformancePanel";
import RaceWorkspace, {
  type Entry,
  type EntrySortKey,
  type FinalPrediction,
  type PredictionResult,
  type PredictionRun,
  type PredictionStatus,
  type QualityStatus,
  type Race,
  type SortDirection
} from "./components/RaceWorkspace";
import WorkspaceHeader, {
  type Health,
  type Notification
} from "./components/WorkspaceHeader";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "/api";

type RaceDay = {
  id: number;
  race_date: string;
  status: string;
  source?: string | null;
};

type RaceWorkbookSelection = {
  workbook: RaceWorkbook;
  import_summary: {
    source_file: string;
    race_date?: string | null;
    races: number;
    entries: number;
    past_performances: number;
    issues: number;
  };
  quality_summary: {
    checked_races: number;
    green: number;
    yellow: number;
    red: number;
  };
};

type AiStatus = {
  race_id: string;
  latest_run_id: string;
  ai_mode: string;
  status: string;
  evaluations: number;
  final_predictions: number;
  has_upgrade: boolean;
  has_downgrade: boolean;
  python_rank_full_match: boolean;
  created_at?: string | null;
};

type NotificationSummary = {
  total_count: number;
  unread_count: number;
  error_count: number;
  warning_count: number;
};

type SortValue = string | number | boolean | null | undefined;

async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": crypto.randomUUID()
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

async function apiPatch<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": crypto.randomUUID()
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

function formatNumber(value?: number | null, digits = 1) {
  if (value === null || value === undefined) {
    return "";
  }
  return value.toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function formatCurrency(value?: number | null) {
  if (value === null || value === undefined) {
    return "-";
  }
  return `${value.toLocaleString("ja-JP")}円`;
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined) {
    return "-";
  }
  return `${formatNumber(value, 1)}%`;
}

function formatAiJobFailure(message?: string | null) {
  const fallback = "ジョブログを確認してください";
  if (!message) {
    return fallback;
  }
  if (message.includes("insufficient_quota") || message.includes("exceeded your current quota")) {
    return "この旧API予想方式は廃止されています。ChatGPT手動予想を使用してください。";
  }
  return message.replace(/^AiIndependentError:\s*/, "");
}

function defaultEntrySortDirection(key: EntrySortKey): SortDirection {
  return [
    "python_score",
    "estimated_in3_rate",
    "expected_value",
    "win_odds",
    "place_odds",
    "independent_ai_confidence",
    "integrated_confidence",
    "integrated_score",
    "ai_adjust_score"
  ].includes(key)
    ? "desc"
    : "asc";
}

function compareSortValues(left: SortValue, right: SortValue, direction: SortDirection) {
  const leftEmpty = left === null || left === undefined || left === "";
  const rightEmpty = right === null || right === undefined || right === "";
  if (leftEmpty && rightEmpty) {
    return 0;
  }
  if (leftEmpty) {
    return 1;
  }
  if (rightEmpty) {
    return -1;
  }

  let result = 0;
  if (typeof left === "number" && typeof right === "number") {
    result = left - right;
  } else if (typeof left === "boolean" && typeof right === "boolean") {
    result = Number(left) - Number(right);
  } else {
    result = String(left).localeCompare(String(right), "ja-JP", {
      numeric: true,
      sensitivity: "base"
    });
  }
  return direction === "asc" ? result : -result;
}

export default function Home() {
  const pathname = usePathname();
  const routePresentation = {
    "/": { key: "dashboard", title: "概要", anchor: "dashboard" },
    "/races": { key: "races", title: "レース・予想", anchor: "primary-workspace" },
    "/analysis": { key: "analysis", title: "ChatGPT予想", anchor: "chatgpt-manual" },
    "/bets": { key: "bets", title: "買い目候補", anchor: "bet-planning" },
    "/performance": { key: "performance", title: "成績分析", anchor: "performance" },
    "/operations": { key: "operations", title: "ジョブ・品質", anchor: "operations" }
  }[pathname] || { key: "dashboard", title: "概要", anchor: "dashboard" };
  const activeRouteKey = routePresentation.key;
  const [health, setHealth] = useState<Health | null>(null);
  const [raceDays, setRaceDays] = useState<RaceDay[]>([]);
  const [raceWorkbooks, setRaceWorkbooks] = useState<RaceWorkbook[]>([]);
  const [selectedWorkbookFile, setSelectedWorkbookFile] = useState("");
  const [workbookSelectionMessage, setWorkbookSelectionMessage] = useState("");
  const [selectedDate, setSelectedDate] = useState<string>("");
  const [races, setRaces] = useState<Race[]>([]);
  const [selectedRaceId, setSelectedRaceId] = useState<string>("");
  const [entries, setEntries] = useState<Entry[]>([]);
  const [predictionRuns, setPredictionRuns] = useState<PredictionRun[]>([]);
  const [predictionResults, setPredictionResults] = useState<PredictionResult[]>([]);
  const [predictionStatuses, setPredictionStatuses] = useState<PredictionStatus[]>([]);
  const [aiStatuses, setAiStatuses] = useState<AiStatus[]>([]);
  const [independentAnalyses, setIndependentAnalyses] = useState<IndependentAiAnalysis[]>([]);
  const [independentAnalysis, setIndependentAnalysis] = useState<IndependentAiAnalysis | null>(null);
  const [integrationAnalyses, setIntegrationAnalyses] = useState<AiIntegrationAnalysis[]>([]);
  const [integrationAnalysis, setIntegrationAnalysis] = useState<AiIntegrationAnalysis | null>(null);
  const [aiEvaluations, setAiEvaluations] = useState<AiEvaluation[]>([]);
  const [finalPredictions, setFinalPredictions] = useState<FinalPrediction[]>([]);
  const [aiBetStrategy, setAiBetStrategy] = useState<AiBetStrategy | null>(null);
  const [chatgptPromptReady, setChatgptPromptReady] = useState(false);
  const [chatgptResponseSaved, setChatgptResponseSaved] = useState(false);
  const [bets, setBets] = useState<BetCandidate[]>([]);
  const [selectedRaceBets, setSelectedRaceBets] = useState<BetCandidate[]>([]);
  const [raceResult, setRaceResult] = useState<RaceResult | null>(null);
  const [betSourceMode, setBetSourceMode] = useState<BetSourceMode>("python");
  const [betType, setBetType] = useState<BetType>("3連複");
  const [betStrategyMode, setBetStrategyMode] = useState<BetStrategyMode>("formation");
  const [stakePerPoint, setStakePerPoint] = useState(500);
  const [maxRaceAmount, setMaxRaceAmount] = useState(3000);
  const [maxDayAmount, setMaxDayAmount] = useState(12000);
  const [maxBetPoints, setMaxBetPoints] = useState(20);
  const [betGenerationMessage, setBetGenerationMessage] = useState("");
  const [analytics, setAnalytics] = useState<AnalyticsSummary | null>(null);
  const [analyticsScope, setAnalyticsScope] = useState<"day" | "all">("day");
  const [analyticsSource, setAnalyticsSource] = useState("");
  const [analyticsBetType, setAnalyticsBetType] = useState("");
  const [analyticsGroup, setAnalyticsGroup] = useState("source_type");
  const [jobs, setJobs] = useState<Job[]>([]);
  const [collections, setCollections] = useState<CollectionRun[]>([]);
  const [issues, setIssues] = useState<Issue[]>([]);
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [notificationCenterOpen, setNotificationCenterOpen] = useState(false);
  const [qualityStatuses, setQualityStatuses] = useState<QualityStatus[]>([]);
  const [isBusy, setIsBusy] = useState(false);
  const [searchText, setSearchText] = useState("");
  const [error, setError] = useState<string>("");
  const observedTerminalJobs = useRef<Set<string>>(new Set());
  const terminalJobsInitialized = useRef(false);
  const [entrySort, setEntrySort] = useState<{ key: EntrySortKey; direction: SortDirection }>({
    key: "integrated_rank",
    direction: "asc"
  });

  const selectedRace = useMemo(
    () => races.find((race) => race.race_id === selectedRaceId),
    [races, selectedRaceId]
  );

  const visibleRaces = useMemo(() => {
    const keyword = searchText.trim();
    if (!keyword) {
      return races;
    }
    return races.filter((race) =>
      [race.race_id, race.venue, race.name, race.course]
        .filter(Boolean)
        .some((value) => String(value).includes(keyword))
    );
  }, [races, searchText]);

  const qualityByRaceId = useMemo(() => {
    return new Map(qualityStatuses.map((quality) => [quality.race_id, quality]));
  }, [qualityStatuses]);

  const predictionByRaceId = useMemo(() => {
    return new Map(predictionStatuses.map((prediction) => [prediction.race_id, prediction]));
  }, [predictionStatuses]);

  const aiByRaceId = useMemo(() => {
    return new Map(aiStatuses.map((statusItem) => [statusItem.race_id, statusItem]));
  }, [aiStatuses]);

  const independentByRaceId = useMemo(() => {
    const map = new Map<string, IndependentAiAnalysis>();
    for (const analysis of independentAnalyses) {
      if (!map.has(analysis.race_id)) {
        map.set(analysis.race_id, analysis);
      }
    }
    return map;
  }, [independentAnalyses]);

  const integrationByRaceId = useMemo(() => {
    const map = new Map<string, AiIntegrationAnalysis>();
    for (const analysis of integrationAnalyses) {
      if (!map.has(analysis.race_id)) {
        map.set(analysis.race_id, analysis);
      }
    }
    return map;
  }, [integrationAnalyses]);

  const betByRaceId = useMemo(() => {
    const map = new Map<string, BetCandidate>();
    for (const bet of bets) {
      if (!map.has(bet.race_id)) {
        map.set(bet.race_id, bet);
      }
    }
    return map;
  }, [bets]);

  const predictionByHorseNo = useMemo(() => {
    return new Map(predictionResults.map((prediction) => [prediction.horse_no, prediction]));
  }, [predictionResults]);

  const aiByHorseNo = useMemo(() => {
    return new Map(aiEvaluations.map((evaluation) => [evaluation.horse_no, evaluation]));
  }, [aiEvaluations]);

  const independentByHorseNo = useMemo(() => {
    return new Map(
      (independentAnalysis?.output?.runners || []).map((evaluation) => [
        evaluation.horse_no,
        evaluation
      ])
    );
  }, [independentAnalysis]);

  const comparisonByHorseNo = useMemo(() => {
    return new Map(
      (integrationAnalysis?.comparison?.horses || []).map((comparison) => [
        comparison.horse_no,
        comparison
      ])
    );
  }, [integrationAnalysis]);

  const integratedByHorseNo = useMemo(() => {
    return new Map(
      (integrationAnalysis?.integration?.horses || []).map((prediction) => [
        prediction.horse_no,
        prediction
      ])
    );
  }, [integrationAnalysis]);

  const finalByHorseNo = useMemo(() => {
    return new Map(finalPredictions.map((prediction) => [prediction.horse_no, prediction]));
  }, [finalPredictions]);

  const aiDiff = useMemo(() => {
    return {
      upgrades: aiEvaluations.filter((evaluation) => evaluation.ai_adjust_score > 0),
      downgrades: aiEvaluations.filter((evaluation) => evaluation.ai_adjust_score < 0),
      dangerous: aiEvaluations.filter(
        (evaluation) =>
          evaluation.ai_bet_role === "消し" ||
          (evaluation.ai_action.includes("下げ") &&
            (predictionByHorseNo.get(evaluation.horse_no)?.popularity ?? 99) <= 3)
      ),
      values: aiEvaluations.filter(
        (evaluation) =>
          evaluation.ai_adjust_score > 0 &&
          ((predictionByHorseNo.get(evaluation.horse_no)?.popularity ?? 0) >= 5 ||
            (predictionByHorseNo.get(evaluation.horse_no)?.win_odds ?? 0) >= 10)
      )
    };
  }, [aiEvaluations, predictionByHorseNo]);

  const qualityAlertCount = useMemo(() => {
    return qualityStatuses.filter((quality) => quality.status === "RED" || quality.status === "YELLOW")
      .length;
  }, [qualityStatuses]);

  const unreadNotificationCount = useMemo(
    () => notifications.filter((notification) => !notification.is_read).length,
    [notifications]
  );

  const visibleCollections = useMemo(() => {
    if (!selectedDate) {
      return collections;
    }
    return collections.filter((collection) => collection.race_date === selectedDate);
  }, [collections, selectedDate]);

  const predictionJobActive = useMemo(
    () =>
      jobs.some(
        (job) =>
          ["prediction.run", "prediction.python"].includes(job.job_type) &&
          ["queued", "running"].includes(job.status) &&
          (!selectedDate || job.race_date === selectedDate) &&
          (!selectedRaceId || job.race_id === selectedRaceId)
      ),
    [jobs, selectedDate, selectedRaceId]
  );

  const displayEntries = useMemo(() => {
    const entrySortValue = (entry: Entry, key: EntrySortKey): SortValue => {
      const prediction = predictionByHorseNo.get(entry.horse_no);
      const independent = independentByHorseNo.get(entry.horse_no);
      const comparison = comparisonByHorseNo.get(entry.horse_no);
      const integrated = integratedByHorseNo.get(entry.horse_no);
      const ai = aiByHorseNo.get(entry.horse_no);
      const final = finalByHorseNo.get(entry.horse_no);
      switch (key) {
        case "horse_no":
          return entry.horse_no;
        case "horse_name":
          return entry.horse_name;
        case "jockey":
          return entry.jockey;
        case "popularity":
          return prediction?.popularity ?? entry.popularity;
        case "win_odds":
          return prediction?.win_odds ?? entry.win_odds;
        case "place_odds":
          return prediction?.place_odds ?? entry.place_odds;
        case "python_rank":
          return prediction?.prediction_rank ?? entry.prediction_rank;
        case "python_score":
          return prediction?.prediction_score ?? entry.prediction_score;
        case "estimated_in3_rate":
          return prediction?.estimated_in3_rate ?? entry.estimated_in3_rate;
        case "expected_value":
          return prediction?.expected_value ?? entry.expected_value;
        case "risk_flag":
          return prediction?.risk_flag;
        case "independent_ai_rank":
          return independent?.ai_rank;
        case "independent_ai_confidence":
          return independent?.confidence;
        case "comparison_gap":
          return comparison?.rank_gap;
        case "integrated_rank":
          return integrated?.integrated_rank;
        case "integrated_score":
          return integrated?.integrated_score;
        case "integrated_confidence":
          return integrated?.confidence;
        case "ai_rank":
          return ai?.ai_rank;
        case "ai_action":
          return ai?.ai_action;
        case "ai_adjust_score":
          return ai?.ai_adjust_score;
        case "final_rank":
          return final?.final_rank;
        case "final_bet_role":
          return final?.final_bet_role;
        case "reason":
          return ai?.ai_reason || prediction?.evaluation_reason;
        default:
          return null;
      }
    };

    return [...entries].sort((left, right) => {
      const result = compareSortValues(
        entrySortValue(left, entrySort.key),
        entrySortValue(right, entrySort.key),
        entrySort.direction
      );
      if (result !== 0) {
        return result;
      }
      return left.horse_no - right.horse_no;
    });
  }, [aiByHorseNo, comparisonByHorseNo, entries, entrySort, finalByHorseNo, independentByHorseNo, integratedByHorseNo, predictionByHorseNo]);

  const latestSelectedPredictionRun = useMemo(() => {
    const latestRunId = predictionResults[0]?.prediction_run_id;
    return predictionRuns.find((run) => run.id === latestRunId) || null;
  }, [predictionResults, predictionRuns]);

  const nextAction = useMemo(() => {
    const quality = selectedRaceId ? qualityByRaceId.get(selectedRaceId) : null;
    if (!selectedDate || !selectedRaceId) {
      return { label: "開催日とレースを選択", detail: "最初に対象レースを決めます", anchor: "#race-workspace" };
    }
    if (quality?.status === "RED") {
      return { label: "品質エラーを解消", detail: quality.summary || "品質チェックを確認してください", anchor: "#operations" };
    }
    if (predictionResults.length < 2) {
      return { label: "Python予想を実行", detail: "出走馬データから基準予想を作成します", anchor: "#race-workspace" };
    }
    if (!chatgptPromptReady) {
      return { label: "ChatGPT用プロンプトを作成", detail: "内容を確認してChatGPTへ手動で送信します", anchor: "#chatgpt-manual" };
    }
    if (!selectedRaceBets.length) {
      return { label: "買い目候補を作成", detail: "予算上限内の候補だけを保存します", anchor: "#bet-planning" };
    }
    if (!raceResult) {
      return { label: "結果を取得", detail: "確定結果と払戻を取り込みます", anchor: "#operations" };
    }
    return { label: "成績を確認", detail: "source・券種・条件別KPIを確認します", anchor: "#performance" };
  }, [chatgptPromptReady, predictionResults.length, qualityByRaceId, raceResult, selectedDate, selectedRaceBets.length, selectedRaceId]);

  async function loadRouteData(routeKey: string, raceDate: string) {
    const routeTasks: Array<Promise<unknown>> = [];

    if (routeKey === "dashboard" || routeKey === "races") {
      routeTasks.push(
        apiGet<PredictionRun[]>("/v1/prediction-runs").then(setPredictionRuns),
        loadPredictionStatuses(raceDate),
        loadIndependentAnalyses(raceDate),
        loadIntegrationAnalyses(raceDate),
        loadAiStatuses(raceDate),
        loadBets(raceDate)
      );
    }
    if (routeKey === "dashboard" || routeKey === "performance") {
      routeTasks.push(loadAnalytics(raceDate));
    }
    if (routeKey === "races" || routeKey === "operations") {
      const collectionQuery = raceDate ? `?race_date=${raceDate}` : "";
      routeTasks.push(
        apiGet<CollectionRun[]>(`/v1/collections${collectionQuery}`).then(setCollections)
      );
    }
    if (routeKey === "operations") {
      routeTasks.push(apiGet<Issue[]>("/v1/data-quality/issues").then(setIssues));
    }

    await Promise.all(routeTasks);
  }

  async function refreshAll() {
    setError("");
    try {
      const [healthData, dayData, workbookData, jobData, notificationData] = await Promise.all([
        apiGet<Health>("/health"),
        apiGet<RaceDay[]>("/v1/race-days"),
        apiGet<RaceWorkbook[]>("/v1/race-workbooks"),
        apiGet<Job[]>("/v1/jobs"),
        apiGet<Notification[]>("/v1/notifications")
      ]);
      setHealth(healthData);
      setRaceDays(dayData);
      setRaceWorkbooks(workbookData);
      setJobs(jobData);
      // 初回読込より前の完了jobは履歴表示だけにし、新規完了通知として再表示しない。
      if (!terminalJobsInitialized.current) {
        for (const job of jobData) {
          if (["completed", "failed"].includes(job.status)) {
            observedTerminalJobs.current.add(job.id);
          }
        }
        terminalJobsInitialized.current = true;
      }
      setNotifications(notificationData);

      const nextDate = selectedDate || dayData[0]?.race_date || "";
      if (nextDate !== selectedDate) {
        setSelectedDate(nextDate);
      }
      const matchingWorkbook = workbookData.find((workbook) => workbook.race_date === nextDate);
      if (!selectedWorkbookFile && matchingWorkbook) {
        setSelectedWorkbookFile(matchingWorkbook.file_name);
      }
      await Promise.all([
        loadRaces(nextDate),
        loadQualityStatuses(nextDate),
        loadRouteData(activeRouteKey, nextDate)
      ]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "unknown error");
    }
  }

  async function handleWorkbookSelection(fileName: string) {
    setSelectedWorkbookFile(fileName);
    setWorkbookSelectionMessage("");
    setError("");
    if (!fileName) {
      setSelectedDate("");
      await loadRaces("");
      return;
    }

    const workbook = raceWorkbooks.find((item) => item.file_name === fileName);
    if (!workbook) {
      setError("選択したExcelファイルが一覧にありません。再読み込みしてください。");
      return;
    }

    setIsBusy(true);
    setSelectedDate(workbook.race_date);
    setSelectedRaceId("");
    try {
      const result = await apiPost<RaceWorkbookSelection>("/v1/race-workbooks/select", {
        file_name: fileName
      });
      setRaceDays(await apiGet<RaceDay[]>("/v1/race-days"));
      setRaceWorkbooks(await apiGet<RaceWorkbook[]>("/v1/race-workbooks"));
      await Promise.all([
        loadRaces(workbook.race_date),
        loadQualityStatuses(workbook.race_date),
        loadRouteData(activeRouteKey, workbook.race_date)
      ]);
      setWorkbookSelectionMessage(
        `${result.workbook.file_name} を読み込みました（${result.import_summary.races}レース・${result.import_summary.entries}頭）`
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Excelファイルの読込に失敗しました");
    } finally {
      setIsBusy(false);
    }
  }

  async function loadQualityStatuses(raceDate: string) {
    const query = raceDate ? `?race_date=${raceDate}` : "";
    const qualityData = await apiGet<QualityStatus[]>(`/v1/data-quality/statuses${query}`);
    setQualityStatuses(qualityData);
  }

  async function loadCollections(raceDate: string) {
    const query = raceDate ? `?race_date=${raceDate}` : "";
    const collectionData = await apiGet<CollectionRun[]>(`/v1/collections${query}`);
    setCollections(collectionData);
  }

  async function loadPredictionStatuses(raceDate: string) {
    const query = raceDate ? `?race_date=${raceDate}` : "";
    const predictionStatusData = await apiGet<PredictionStatus[]>(`/v1/prediction-statuses${query}`);
    setPredictionStatuses(predictionStatusData);
  }

  async function loadAiStatuses(raceDate: string) {
    const query = raceDate ? `?race_date=${raceDate}` : "";
    const statusData = await apiGet<AiStatus[]>(`/v1/ai/statuses${query}`);
    setAiStatuses(statusData);
  }

  async function loadIndependentAnalyses(raceDate: string) {
    const query = raceDate ? `?race_date=${raceDate}` : "";
    const analysisData = await apiGet<IndependentAiAnalysis[]>(
      `/v1/ai/independent-analyses${query}`
    );
    setIndependentAnalyses(analysisData);
  }

  async function loadIndependentRaceData(raceId: string) {
    if (!raceId) {
      setIndependentAnalysis(null);
      return;
    }
    const analysisData = await apiGet<IndependentAiAnalysis | null>(
      `/v1/races/${raceId}/ai-independent-analysis`
    );
    setIndependentAnalysis(analysisData);
  }

  async function loadIntegrationAnalyses(raceDate: string) {
    const query = raceDate ? `?race_date=${raceDate}` : "";
    const analysisData = await apiGet<AiIntegrationAnalysis[]>(
      `/v1/ai/integration-analyses${query}`
    );
    setIntegrationAnalyses(analysisData);
  }

  async function loadIntegrationRaceData(raceId: string) {
    if (!raceId) {
      setIntegrationAnalysis(null);
      return;
    }
    const analysisData = await apiGet<AiIntegrationAnalysis | null>(
      `/v1/races/${raceId}/ai-integration-analysis`
    );
    setIntegrationAnalysis(analysisData);
  }

  async function loadBets(raceDate: string) {
    const query = raceDate ? `?race_date=${raceDate}` : "";
    const betData = await apiGet<BetCandidate[]>(`/v1/bets${query}`);
    setBets(betData);
  }

  async function loadRaceBets(raceId: string) {
    if (!raceId) {
      setSelectedRaceBets([]);
      return;
    }
    const betData = await apiGet<BetCandidate[]>(`/v1/races/${raceId}/bets`);
    setSelectedRaceBets(betData);
  }

  async function loadRaceResult(raceId: string) {
    if (!raceId) {
      setRaceResult(null);
      return;
    }
    const resultData = await apiGet<RaceResult | null>(`/v1/races/${raceId}/result`);
    setRaceResult(resultData);
  }

  async function loadAiRaceData(raceId: string) {
    if (!raceId) {
      setAiEvaluations([]);
      setFinalPredictions([]);
      setAiBetStrategy(null);
      return;
    }
    const [evaluationData, finalData, strategyData] = await Promise.all([
      apiGet<AiEvaluation[]>(`/v1/races/${raceId}/ai-evaluations`),
      apiGet<FinalPrediction[]>(`/v1/races/${raceId}/final-predictions`),
      apiGet<AiBetStrategy | null>(`/v1/races/${raceId}/ai-bet-strategy`)
    ]);
    setAiEvaluations(evaluationData);
    setFinalPredictions(finalData);
    setAiBetStrategy(strategyData);
  }

  async function loadAnalytics(raceDate: string) {
    const params = new URLSearchParams();
    if (analyticsScope === "day" && raceDate) {
      params.set("race_date", raceDate);
    }
    if (analyticsSource) {
      params.set("source_type", analyticsSource);
    }
    if (analyticsBetType) {
      params.set("bet_type", analyticsBetType);
    }
    params.set("group_by", analyticsGroup);
    const analyticsData = await apiGet<AnalyticsSummary>(`/v1/analytics?${params.toString()}`);
    setAnalytics(analyticsData);
  }

  async function loadSelectedRaceData(raceId: string) {
    await Promise.all([
      loadEntries(raceId),
      loadPredictionResults(raceId),
      loadIndependentRaceData(raceId),
      loadIntegrationRaceData(raceId),
      loadAiRaceData(raceId),
      loadRaceBets(raceId),
      loadRaceResult(raceId)
    ]);
  }

  async function loadRaces(raceDate: string) {
    if (!raceDate) {
      setRaces([]);
      setSelectedRaceId("");
      setEntries([]);
      setPredictionResults([]);
      setIndependentAnalysis(null);
      setIntegrationAnalysis(null);
      setAiEvaluations([]);
      setFinalPredictions([]);
      setAiBetStrategy(null);
      setSelectedRaceBets([]);
      setRaceResult(null);
      return;
    }
    const raceData = await apiGet<Race[]>(`/v1/races?race_date=${raceDate}`);
    setRaces(raceData);
    const nextRaceId = raceData[0]?.race_id || "";
    const targetRaceId = raceData.some((race) => race.race_id === selectedRaceId)
      ? selectedRaceId
      : nextRaceId;
    setSelectedRaceId(targetRaceId);
    if (targetRaceId && targetRaceId === selectedRaceId) {
      // 選択が変わらない再読込ではeffectが発火しないため、ここで最新化する。
      await loadSelectedRaceData(targetRaceId);
    } else {
      setEntries([]);
      setPredictionResults([]);
      setIndependentAnalysis(null);
      setIntegrationAnalysis(null);
      setAiEvaluations([]);
      setFinalPredictions([]);
      setAiBetStrategy(null);
      setSelectedRaceBets([]);
      setRaceResult(null);
    }
  }

  async function loadEntries(raceId: string) {
    if (!raceId) {
      setEntries([]);
      return;
    }
    const entryData = await apiGet<Entry[]>(`/v1/races/${raceId}/entries`);
    setEntries(entryData);
  }

  async function loadPredictionResults(raceId: string) {
    if (!raceId) {
      setPredictionResults([]);
      return;
    }
    const predictionData = await apiGet<PredictionResult[]>(`/v1/races/${raceId}/prediction-results`);
    setPredictionResults(predictionData);
  }

  async function runJob(jobType: string) {
    setIsBusy(true);
    setError("");
    try {
      await apiPost<Job>("/v1/jobs", {
        job_type: jobType,
        race_date: selectedDate || null,
        race_id: selectedRaceId || null,
        force: false
      });
      await refreshAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "unknown error");
    } finally {
      setIsBusy(false);
    }
  }

  async function generateBetPreviews() {
    setIsBusy(true);
    setError("");
    setBetGenerationMessage("");
    try {
      const sourceModes =
        betSourceMode === "both" ? ["python", "ai_integrated"] : [betSourceMode];
      const summary = await apiPost<BetGenerationResult>("/v1/bets/generate", {
        race_id: selectedRaceId,
        race_date: selectedDate || null,
        prediction_run_id: predictionResults[0]?.prediction_run_id || null,
        source_modes: sourceModes,
        bet_types: [betType],
        strategy_modes: [betStrategyMode],
        ai_analysis_id:
          betSourceMode === "python" ? null : integrationAnalysis?.id || null,
        stake_per_point: stakePerPoint,
        max_race_amount: maxRaceAmount,
        max_day_amount: maxDayAmount,
        max_points: maxBetPoints,
        allow_manual_review: false
      });
      setBetGenerationMessage(
        `保存 ${summary.generated}件 / 候補 ${summary.candidates} / 手動確認 ${summary.review_required} / 停止 ${summary.blocked + summary.skipped}`
      );
      if (summary.warnings.length) {
        setError(summary.warnings.join(" / "));
      }
      await Promise.all([loadBets(selectedDate), loadRaceBets(selectedRaceId)]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "unknown error");
    } finally {
      setIsBusy(false);
    }
  }

  async function retryCollection(jobRunId: string) {
    setIsBusy(true);
    setError("");
    try {
      // Retry APIは元ジョブの対象・引数を引き継ぎ、force=trueの新規runを発行する。
      await apiPost<Job>(`/v1/jobs/${jobRunId}/retry`, {});
      await refreshAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "unknown error");
    } finally {
      setIsBusy(false);
    }
  }

  async function updateBetCandidateStatus(betId: number, status: string, reason?: string) {
    setIsBusy(true);
    setError("");
    try {
      await apiPatch<BetCandidate>(`/v1/bets/${betId}/status`, { status, reason });
      await Promise.all([
        loadBets(selectedDate),
        loadAnalytics(selectedDate),
        selectedRaceId ? loadRaceBets(selectedRaceId) : Promise.resolve()
      ]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "unknown error");
    } finally {
      setIsBusy(false);
    }
  }

  async function updateNotificationReadState(notificationId: string, isRead: boolean) {
    try {
      const updated = await apiPatch<Notification>(
        `/v1/notifications/${notificationId}/read`,
        { is_read: isRead }
      );
      setNotifications((current) =>
        current.map((notification) => notification.id === updated.id ? updated : notification)
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "通知の更新に失敗しました");
    }
  }

  async function markAllNotificationsRead() {
    try {
      await apiPost<NotificationSummary>("/v1/notifications/read-all", {});
      setNotifications((current) =>
        current.map((notification) => ({ ...notification, is_read: true }))
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "通知の一括既読に失敗しました");
    }
  }

  function updateEntrySort(key: EntrySortKey) {
    setEntrySort((current) => {
      if (current.key === key) {
        return {
          key,
          direction: current.direction === "asc" ? "desc" : "asc"
        };
      }
      return {
        key,
        direction: defaultEntrySortDirection(key)
      };
    });
  }

  useEffect(() => {
    void refreshAll();
  }, [pathname]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      // 長時間Collectorのqueued/running変化だけを軽量に追跡する。
      const shouldPollCollections = activeRouteKey === "races" || activeRouteKey === "operations";
      void Promise.all([
        apiGet<Job[]>("/v1/jobs"),
        shouldPollCollections
          ? apiGet<CollectionRun[]>(
              selectedDate ? `/v1/collections?race_date=${selectedDate}` : "/v1/collections"
            )
          : Promise.resolve<CollectionRun[] | null>(null),
        apiGet<Notification[]>("/v1/notifications")
      ])
        .then(([jobData, collectionData, notificationData]) => {
          setJobs(jobData);
          if (collectionData) {
            setCollections(collectionData);
          }
          setNotifications(notificationData);
          const completedPrediction = jobData.find(
            (job) =>
              ["prediction.run", "prediction.python"].includes(job.job_type) &&
              job.status === "completed" &&
              !observedTerminalJobs.current.has(job.id) &&
              (!selectedDate || job.race_date === selectedDate) &&
              (!selectedRaceId || job.race_id === selectedRaceId)
          );
          if (
            selectedRaceId &&
            completedPrediction &&
            !observedTerminalJobs.current.has(completedPrediction.id)
          ) {
            observedTerminalJobs.current.add(completedPrediction.id);
            // 長時間ジョブ完了時だけ、選択レースの予想表示を更新する。
            void Promise.all([
              loadEntries(selectedRaceId),
              loadPredictionResults(selectedRaceId),
              loadPredictionStatuses(selectedDate),
              apiGet<PredictionRun[]>("/v1/prediction-runs").then(setPredictionRuns)
            ]);
          }
          const finishedAi = jobData.find(
            (job) =>
              job.job_type === "ai.independent" &&
              ["completed", "failed"].includes(job.status) &&
              !observedTerminalJobs.current.has(job.id) &&
              (!selectedDate || job.race_date === selectedDate) &&
              (!selectedRaceId || job.race_id === selectedRaceId)
          );
          if (selectedRaceId && finishedAi) {
            observedTerminalJobs.current.add(finishedAi.id);
            void Promise.all([
              loadIndependentRaceData(selectedRaceId),
              loadIndependentAnalyses(selectedDate)
            ]);
            if (finishedAi.status === "failed") {
              setError(`独立AI分析に失敗しました: ${formatAiJobFailure(finishedAi.message)}`);
            }
          }
          const finishedIntegration = jobData.find(
            (job) =>
              job.job_type === "ai.compare_integrate" &&
              ["completed", "failed"].includes(job.status) &&
              !observedTerminalJobs.current.has(job.id) &&
              (!selectedDate || job.race_date === selectedDate) &&
              (!selectedRaceId || job.race_id === selectedRaceId)
          );
          if (
            selectedRaceId &&
            finishedIntegration
          ) {
            observedTerminalJobs.current.add(finishedIntegration.id);
            void Promise.all([
              loadIntegrationRaceData(selectedRaceId),
              loadIntegrationAnalyses(selectedDate)
            ]);
            if (finishedIntegration.status === "failed") {
              setError(
                `AI比較・統合に失敗しました: ${formatAiJobFailure(finishedIntegration.message)}`
              );
            }
          }
        })
        .catch(() => {
          // 一時的なpoll失敗は既存表示を維持し、手動再読込時に詳細を表示する。
        });
    }, 5000);
    return () => window.clearInterval(timer);
  }, [activeRouteKey, selectedDate, selectedRaceId]);

  useEffect(() => {
    setChatgptPromptReady(false);
    setChatgptResponseSaved(false);
    if (selectedRaceId) {
      void loadSelectedRaceData(selectedRaceId);
    }
  }, [selectedRaceId]);

  return (
    <main
      className="appShell"
      data-workspace-route={routePresentation.key}
      id="main-content"
      aria-busy={isBusy}
    >
      <WorkspaceHeader
        error={error}
        health={health}
        isBusy={isBusy}
        nextAction={nextAction}
        notificationCenterOpen={notificationCenterOpen}
        notifications={notifications}
        onMarkAllNotificationsRead={() => void markAllNotificationsRead()}
        onRefresh={() => void refreshAll()}
        onToggleNotificationCenter={() => setNotificationCenterOpen((current) => !current)}
        onUpdateNotificationReadState={(notificationId, isRead) =>
          void updateNotificationReadState(notificationId, isRead)}
        progress={{
          python: predictionResults.length >= 2,
          chatgptPrompt: chatgptPromptReady,
          chatgptSaved: chatgptResponseSaved,
          bets: selectedRaceBets.length > 0,
          result: raceResult?.result_status === "confirmed"
        }}
        routeAnchor={routePresentation.anchor}
        routeTitle={routePresentation.title}
        unreadNotificationCount={unreadNotificationCount}
      />

      <ActionToolbar
        canGenerateBets={Boolean(selectedRaceId) && predictionResults.length >= 2}
        isBusy={isBusy}
        onGenerateBets={() => void generateBetPreviews()}
        onRunJob={(jobType) => void runJob(jobType)}
        onSearchTextChange={setSearchText}
        onWorkbookSelection={(fileName) => void handleWorkbookSelection(fileName)}
        predictionJobActive={predictionJobActive}
        raceWorkbooks={raceWorkbooks}
        searchText={searchText}
        selectedDate={selectedDate}
        selectedRaceId={selectedRaceId}
        selectedWorkbookFile={selectedWorkbookFile}
        workbookSelectionMessage={workbookSelectionMessage}
      />

      <section className="metrics" data-route-section="dashboard" id="dashboard">
        <div>
          <span>開催日</span>
          <strong>{raceDays.length}</strong>
        </div>
        <div>
          <span>レース</span>
          <strong>{races.length}</strong>
        </div>
        <div>
          <span>出走馬</span>
          <strong>{entries.length}</strong>
        </div>
        <div>
          <span>品質警告</span>
          <strong>{qualityAlertCount}</strong>
        </div>
        <div>
          <span>Python予想済み</span>
          <strong>{predictionStatuses.length}</strong>
        </div>
        <div>
          <span>旧API独立AI履歴</span>
          <strong>{independentAnalyses.filter((analysis) => analysis.status === "succeeded").length}</strong>
        </div>
        <div>
          <span>旧API比較・統合履歴</span>
          <strong>{integrationAnalyses.filter((analysis) => analysis.status === "succeeded").length}</strong>
        </div>
        <div>
          <span>買い目</span>
          <strong>{bets.length}</strong>
        </div>
        <div>
          <span>回収率</span>
          <strong>{analytics ? formatPercent(analytics.roi) : "-"}</strong>
        </div>
      </section>

      <RaceWorkspace
        aiByHorseNo={aiByHorseNo}
        betByRaceId={betByRaceId}
        comparisonByHorseNo={comparisonByHorseNo}
        displayEntries={displayEntries}
        entrySort={entrySort}
        finalByHorseNo={finalByHorseNo}
        formatNumber={formatNumber}
        formatPercent={formatPercent}
        independentByHorseNo={independentByHorseNo}
        independentByRaceId={independentByRaceId}
        integratedByHorseNo={integratedByHorseNo}
        integrationByRaceId={integrationByRaceId}
        latestSelectedPredictionRun={latestSelectedPredictionRun}
        onEntrySort={updateEntrySort}
        onSelectedRaceChange={setSelectedRaceId}
        predictionByHorseNo={predictionByHorseNo}
        predictionByRaceId={predictionByRaceId}
        qualityByRaceId={qualityByRaceId}
        selectedDate={selectedDate}
        selectedRace={selectedRace}
        selectedRaceId={selectedRaceId}
        visibleRaces={visibleRaces}
      />

      <div className="lowerGrid">
        <OperationsPanels
          collections={visibleCollections}
          isBusy={isBusy}
          issues={issues}
          jobs={jobs}
          onRetryCollection={retryCollection}
        />

        <ChatgptManualPanel
          onPromptReady={setChatgptPromptReady}
          onResponseSaved={setChatgptResponseSaved}
          pythonPredictionReady={predictionResults.length >= 2}
          selectedRaceId={selectedRaceId}
        />

        <AiAnalysisPanels
          aiBetStrategy={aiBetStrategy}
          aiDiff={aiDiff}
          aiEvaluations={aiEvaluations}
          formatAiJobFailure={formatAiJobFailure}
          formatNumber={formatNumber}
          independentAnalysis={independentAnalysis}
          integrationAnalysis={integrationAnalysis}
        />

        <section className="helpPanel" data-route-section="analysis bets">
          <div className="sectionHeader">
            <h2>操作ヘルプ</h2>
            <span>安全な順序</span>
          </div>
          <details>
            <summary>予想から成績確認まで</summary>
            <ol>
              <li>レースと品質状態を確認してPython予想を実行します。</li>
              <li>ChatGPT用プロンプトを作成・コピーし、ChatGPTへ手動で貼り付けて送信します。</li>
              <li>ChatGPTの回答を手動で貼り付け、対象レースへ保存します。</li>
              <li>予算と最大点数を設定し、買い目候補だけを保存します。</li>
              <li>外部購入した場合だけ、確認後に手動購入を記録します。</li>
              <li>確定結果を取得して精算し、条件別成績を確認します。</li>
            </ol>
          </details>
        </section>

        <BetPlanningPanel
          betGenerationMessage={betGenerationMessage}
          betSourceMode={betSourceMode}
          betStrategyMode={betStrategyMode}
          betType={betType}
          canGenerate={Boolean(selectedRaceId) && predictionResults.length >= 2}
          canUseIntegratedAi={Boolean(integrationAnalysis?.integration_locked)}
          formatCurrency={formatCurrency}
          isBusy={isBusy}
          maxBetPoints={maxBetPoints}
          maxDayAmount={maxDayAmount}
          maxRaceAmount={maxRaceAmount}
          onBetSourceModeChange={setBetSourceMode}
          onBetStrategyModeChange={setBetStrategyMode}
          onBetTypeChange={setBetType}
          onGenerate={() => void generateBetPreviews()}
          onMaxBetPointsChange={setMaxBetPoints}
          onMaxDayAmountChange={setMaxDayAmount}
          onMaxRaceAmountChange={setMaxRaceAmount}
          onStakePerPointChange={setStakePerPoint}
          onStatusChange={(betId, status, reason) => void updateBetCandidateStatus(betId, status, reason)}
          selectedRaceBets={selectedRaceBets}
          stakePerPoint={stakePerPoint}
        />

        <PerformancePanel
          analytics={analytics}
          betType={analyticsBetType}
          formatCurrency={formatCurrency}
          formatPercent={formatPercent}
          group={analyticsGroup}
          onBetTypeChange={setAnalyticsBetType}
          onGroupChange={setAnalyticsGroup}
          onRefresh={() => void loadAnalytics(selectedDate)}
          onScopeChange={setAnalyticsScope}
          onSourceChange={setAnalyticsSource}
          raceResult={raceResult}
          scope={analyticsScope}
          selectedDate={selectedDate}
          source={analyticsSource}
        />

      </div>
    </main>
  );
}
