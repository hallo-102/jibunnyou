"use client";

import {
  Activity,
  AlertTriangle,
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  BadgeCent,
  CheckCircle2,
  CircleSlash,
  Database,
  ListRestart,
  Play,
  RefreshCw,
  Search
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "/api";

type RaceDay = {
  id: number;
  race_date: string;
  status: string;
  source?: string | null;
};

type RaceWorkbook = {
  file_name: string;
  race_date: string;
  size_bytes: number;
  modified_at: string;
  is_imported: boolean;
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

type Race = {
  race_id: string;
  race_date?: string | null;
  race_number?: number | null;
  venue?: string | null;
  name?: string | null;
  start_time?: string | null;
  course?: string | null;
  track_condition?: string | null;
  headcount?: number | null;
};

type Entry = {
  id: number;
  race_id: string;
  horse_no: number;
  horse_name: string;
  jockey?: string | null;
  trainer?: string | null;
  popularity?: number | null;
  win_odds?: number | null;
  place_odds?: number | null;
  prediction_rank?: number | null;
  prediction_score?: number | null;
  estimated_in3_rate?: number | null;
  expected_value?: number | null;
};

type PredictionRun = {
  id: string;
  status: string;
  race_date?: string | null;
  race_id?: string | null;
  prediction_version: string;
  feature_version: string;
  weight_version: string;
  model_version: string;
  result_count: number;
  matched_count: number;
  mismatch_count: number;
  finished_at?: string | null;
};

type PredictionResult = {
  id: number;
  prediction_run_id: string;
  race_id: string;
  horse_no: number;
  horse_name: string;
  popularity?: number | null;
  win_odds?: number | null;
  place_odds?: number | null;
  prediction_rank?: number | null;
  prediction_score?: number | null;
  estimated_in3_rate?: number | null;
  expected_value?: number | null;
  risk_flag: boolean;
  risk_score?: number | null;
  risk_reason?: string | null;
  evaluation_reason?: string | null;
};

type PredictionStatus = {
  race_id: string;
  latest_run_id: string;
  status: string;
  result_count: number;
  predicted_at?: string | null;
  top_horse_no?: number | null;
  top_horse_name?: string | null;
  top_score?: number | null;
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

type IndependentAiRunner = {
  horse_no: number;
  horse_name: string;
  ai_rank?: number | null;
  rank_range_low?: number | null;
  rank_range_high?: number | null;
  confidence: number;
  risk_level: "low" | "medium" | "high" | "unknown";
  positive_factors: string[];
  negative_factors: string[];
  uncertainties: string[];
  rationale: string;
};

type IndependentAiOutput = {
  schema_version: "ai_independent_result_v1";
  race_id: string;
  decision_status: "completed" | "insufficient_data";
  data_confidence: "low" | "medium" | "high";
  manual_review_required: boolean;
  race_assessment: {
    pace_assessment: "slow" | "middle" | "fast" | "uncertain";
    track_bias_assessment: string;
    main_risks: string[];
    summary: string;
  };
  runners: IndependentAiRunner[];
  unknowns: string[];
  final_comment: string;
};

type IndependentAiAnalysis = {
  id: string;
  race_id: string;
  race_date?: string | null;
  analysis_sequence: number;
  status: "queued" | "running" | "succeeded" | "failed" | "degraded";
  model_name: string;
  prompt_version: string;
  input_data_version?: string | null;
  input_snapshot_hash?: string | null;
  prompt_tokens?: number | null;
  completion_tokens?: number | null;
  duration_ms?: number | null;
  error_message?: string | null;
  rerun_reason?: string | null;
  created_at: string;
  finished_at?: string | null;
  output?: IndependentAiOutput | null;
  output_hash?: string | null;
  output_locked: boolean;
};

type HorseComparison = {
  horse_no: number;
  horse_name: string;
  python_rank: number;
  ai_rank?: number | null;
  rank_gap?: number | null;
  agreement_level: "exact" | "small_difference" | "material_difference" | "unknown";
  python_view: string;
  ai_view: string;
  counterpoints: string[];
  material_opposition: boolean;
  uncertainty: string;
};

type ComparisonOutput = {
  schema_version: "ai_comparison_result_v1";
  race_id: string;
  independent_analysis_id: string;
  python_prediction_run_id: string;
  overall_alignment: "high" | "medium" | "low" | "unknown";
  data_confidence: "low" | "medium" | "high";
  manual_review_required: boolean;
  horses: HorseComparison[];
  opposition: {
    has_material_opposition: boolean;
    horse_nos: number[];
    summary: string;
  };
  summary: string;
};

type IntegratedHorsePrediction = {
  horse_no: number;
  horse_name: string;
  python_rank: number;
  ai_rank?: number | null;
  integrated_rank: number;
  integrated_score?: number | null;
  decision_basis: "python_priority" | "ai_priority" | "balanced" | "insufficient_data";
  confidence: number;
  uncertainty_level: "low" | "medium" | "high";
  reasons: string[];
  risk_summary: string;
};

type IntegrationOutput = {
  schema_version: "ai_integration_result_v1";
  race_id: string;
  independent_analysis_id: string;
  python_prediction_run_id: string;
  integration_strategy: "python_priority" | "ai_priority" | "balanced" | "no_decision";
  data_confidence: "low" | "medium" | "high";
  manual_review_required: boolean;
  horses: IntegratedHorsePrediction[];
  key_disagreements: string[];
  uncertainties: string[];
  final_comment: string;
};

type AiIntegrationAnalysis = {
  id: string;
  race_id: string;
  race_date?: string | null;
  prediction_run_id?: string | null;
  independent_analysis_id?: string | null;
  analysis_sequence: number;
  status: "queued" | "running" | "succeeded" | "failed" | "degraded";
  model_name: string;
  prompt_version: string;
  input_snapshot_hash?: string | null;
  error_message?: string | null;
  created_at: string;
  comparison?: ComparisonOutput | null;
  comparison_output_hash?: string | null;
  comparison_locked: boolean;
  integration?: IntegrationOutput | null;
  integration_output_hash?: string | null;
  integration_locked: boolean;
};

type AiEvaluation = {
  id: number;
  ai_run_id: string;
  race_id: string;
  horse_no: number;
  horse_name: string;
  python_rank?: number | null;
  ai_rank?: number | null;
  ai_action: string;
  ai_adjust_score: number;
  ai_reason: string;
  ai_risk?: string | null;
  ai_bet_role: string;
  ai_confidence?: number | null;
};

type FinalPrediction = {
  id: number;
  ai_run_id: string;
  race_id: string;
  horse_no: number;
  horse_name: string;
  python_rank?: number | null;
  python_score?: number | null;
  ai_rank?: number | null;
  ai_adjust_score: number;
  final_score: number;
  final_rank?: number | null;
  final_bet_role: string;
};

type AiBetStrategy = {
  id: number;
  ai_run_id: string;
  race_id: string;
  strategy_type: string;
  main_axis?: number[] | null;
  second_axis_candidates?: number[] | null;
  main_opponents?: number[] | null;
  wide_opponents?: number[] | null;
  exclude_candidates?: number[] | null;
  suggestion?: string | null;
  bet_count: number;
};

type BetCandidate = {
  id: number;
  prediction_run_id: string;
  source_type: "python" | "ai_integrated" | "legacy_ai" | "manual";
  ai_analysis_id?: string | null;
  race_id: string;
  race_date?: string | null;
  rank: string;
  status: string;
  bet_type: string;
  strategy: string;
  strategy_mode: "formation" | "box" | "wheel" | "manual";
  bet_rule_version: string;
  axis_horse_nos?: number[] | null;
  opponent_horse_nos?: number[] | null;
  combinations?: number[][] | null;
  points: number;
  stake_per_point: number;
  total_amount: number;
  max_race_amount: number;
  max_day_amount: number;
  expected_value?: number | null;
  reason?: string | null;
  skip_reason?: string | null;
  warning_codes: string[];
  requires_confirmation: boolean;
  purchase_execution_enabled: boolean;
  source_snapshot_hash?: string | null;
};

type BetGenerationResult = {
  generated: number;
  candidates: number;
  skipped: number;
  blocked: number;
  review_required: number;
  total_planned_amount: number;
  warnings: string[];
};

type RaceResult = {
  id: number;
  race_id: string;
  race_date?: string | null;
  finish_order: number[];
  result_status: "provisional" | "confirmed" | "cancelled";
  payout_amount: number;
  payout_type: string;
  payouts_json: Array<{
    bet_type: string;
    combination: number[];
    payout_per_100: number;
    status: string;
  }>;
  cancelled_horse_nos: number[];
  disqualified_horse_nos: number[];
  has_dead_heat: boolean;
  source_file?: string | null;
  imported_at: string;
};

type AnalyticsSummary = {
  race_date?: string | null;
  bets: number;
  settled_bets: number;
  hits: number;
  hit_rate: number;
  stake_amount: number;
  payout_amount: number;
  profit_loss: number;
  roi: number;
  max_consecutive_losses: number;
  max_drawdown: number;
  breakdown: Array<{
    dimension: string;
    value: string;
    bets: number;
    settled_bets: number;
    hits: number;
    hit_rate: number;
    stake_amount: number;
    payout_amount: number;
    profit_loss: number;
    roi: number;
  }>;
};

type Job = {
  id: string;
  job_type: string;
  status: string;
  race_date?: string | null;
  race_id?: string | null;
  message?: string | null;
  created_at: string;
};

type CollectionRun = {
  id: string;
  job_run_id: string;
  source_code: string;
  data_kind: string;
  status: string;
  mode: string;
  race_date?: string | null;
  race_id?: string | null;
  cache_hit: boolean;
  attempt_count: number;
  retry_count: number;
  request_count: number;
  quality_status?: string | null;
  error_code?: string | null;
  error_message?: string | null;
  created_at: string;
};

type Issue = {
  id: number;
  severity: string;
  code: string;
  message: string;
  source_file?: string | null;
  race_id?: string | null;
  row_number?: number | null;
};

type QualityStatus = {
  id: number;
  race_id: string;
  status: string;
  summary?: string | null;
  issue_count: number;
  red_count: number;
  yellow_count: number;
  checked_at: string;
};

type Health = {
  status: string;
  database: string;
  redis: string;
};

type EntrySortKey =
  | "horse_no"
  | "horse_name"
  | "jockey"
  | "popularity"
  | "win_odds"
  | "place_odds"
  | "python_rank"
  | "python_score"
  | "estimated_in3_rate"
  | "expected_value"
  | "risk_flag"
  | "independent_ai_rank"
  | "independent_ai_confidence"
  | "comparison_gap"
  | "integrated_rank"
  | "integrated_score"
  | "integrated_confidence"
  | "ai_rank"
  | "ai_action"
  | "ai_adjust_score"
  | "final_rank"
  | "final_bet_role"
  | "reason";

type SortDirection = "asc" | "desc";
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
    return "OpenAI APIの利用枠がありません。API Platform側のBillingで支払い方法・残高・利用上限を確認してください。ChatGPTの契約とは別管理です。";
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

function EntrySortHeader({
  label,
  columnKey,
  sortKey,
  sortDirection,
  onSort
}: {
  label: string;
  columnKey: EntrySortKey;
  sortKey: EntrySortKey;
  sortDirection: SortDirection;
  onSort: (key: EntrySortKey) => void;
}) {
  const isActive = sortKey === columnKey;
  const Icon = isActive ? (sortDirection === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <button
      className={isActive ? "sortHeader active" : "sortHeader"}
      onClick={() => onSort(columnKey)}
      title={`${label}でソート`}
      type="button"
    >
      <span>{label}</span>
      <Icon size={13} aria-hidden="true" />
    </button>
  );
}

export default function Home() {
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
  const [bets, setBets] = useState<BetCandidate[]>([]);
  const [selectedRaceBets, setSelectedRaceBets] = useState<BetCandidate[]>([]);
  const [raceResult, setRaceResult] = useState<RaceResult | null>(null);
  const [betSourceMode, setBetSourceMode] = useState<"python" | "ai_integrated" | "both">("python");
  const [betType, setBetType] = useState<"3連複" | "ワイド">("3連複");
  const [betStrategyMode, setBetStrategyMode] = useState<"formation" | "box" | "wheel">("formation");
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

  const aiJobActive = useMemo(
    () =>
      jobs.some(
        (job) =>
          job.job_type === "ai.independent" &&
          ["queued", "running"].includes(job.status) &&
          (!selectedDate || job.race_date === selectedDate) &&
          (!selectedRaceId || job.race_id === selectedRaceId)
      ),
    [jobs, selectedDate, selectedRaceId]
  );

  const integrationJobActive = useMemo(
    () =>
      jobs.some(
        (job) =>
          job.job_type === "ai.compare_integrate" &&
          ["queued", "running"].includes(job.status) &&
          (!selectedDate || job.race_date === selectedDate) &&
          (!selectedRaceId || job.race_id === selectedRaceId)
      ),
    [jobs, selectedDate, selectedRaceId]
  );

  const canRunIntegration = Boolean(
    independentAnalysis?.status === "succeeded" &&
      independentAnalysis.output_locked &&
      independentAnalysis.output &&
      predictionResults.length >= 2
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
    if (!independentAnalysis?.output_locked) {
      return { label: "独立AI分析を実行", detail: "Python順位を見せずにAI評価を固定します", anchor: "#ai-analysis" };
    }
    if (!integrationAnalysis?.integration_locked) {
      return { label: "Python/AIを比較・統合", detail: "固定済み独立結果とPython予想を比較します", anchor: "#ai-analysis" };
    }
    if (!selectedRaceBets.length) {
      return { label: "買い目候補を作成", detail: "予算上限内の候補だけを保存します", anchor: "#bet-planning" };
    }
    if (!raceResult) {
      return { label: "結果を取得", detail: "確定結果と払戻を取り込みます", anchor: "#operations" };
    }
    return { label: "成績を確認", detail: "source・券種・条件別KPIを確認します", anchor: "#performance" };
  }, [independentAnalysis, integrationAnalysis, predictionResults.length, qualityByRaceId, raceResult, selectedDate, selectedRaceBets.length, selectedRaceId]);

  async function refreshAll() {
    setError("");
    try {
      const [healthData, dayData, workbookData, jobData, collectionData, issueData, runData] = await Promise.all([
        apiGet<Health>("/health"),
        apiGet<RaceDay[]>("/v1/race-days"),
        apiGet<RaceWorkbook[]>("/v1/race-workbooks"),
        apiGet<Job[]>("/v1/jobs"),
        apiGet<CollectionRun[]>("/v1/collections"),
        apiGet<Issue[]>("/v1/data-quality/issues"),
        apiGet<PredictionRun[]>("/v1/prediction-runs")
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
      setCollections(collectionData);
      setIssues(issueData);
      setPredictionRuns(runData);

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
        loadPredictionStatuses(nextDate),
        loadIndependentAnalyses(nextDate),
        loadIntegrationAnalyses(nextDate),
        loadAiStatuses(nextDate),
        loadBets(nextDate),
        loadAnalytics(nextDate)
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
        loadCollections(workbook.race_date),
        loadQualityStatuses(workbook.race_date),
        loadPredictionStatuses(workbook.race_date),
        loadIndependentAnalyses(workbook.race_date),
        loadIntegrationAnalyses(workbook.race_date),
        loadAiStatuses(workbook.race_date),
        loadBets(workbook.race_date),
        loadAnalytics(workbook.race_date)
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
    if (targetRaceId) {
      await Promise.all([
        loadEntries(targetRaceId),
        loadPredictionResults(targetRaceId),
        loadIndependentRaceData(targetRaceId),
        loadIntegrationRaceData(targetRaceId),
        loadAiRaceData(targetRaceId),
        loadRaceBets(targetRaceId),
        loadRaceResult(targetRaceId)
      ]);
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

  async function runComparisonIntegration() {
    setIsBusy(true);
    setError("");
    try {
      await apiPost<Job>("/v1/ai/comparison-integration", {
        race_id: selectedRaceId,
        race_date: selectedDate || null,
        independent_analysis_id: independentAnalysis?.id || null,
        prediction_run_id: predictionResults[0]?.prediction_run_id || null,
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
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      // 長時間Collectorのqueued/running変化だけを軽量に追跡する。
      void Promise.all([
        apiGet<Job[]>("/v1/jobs"),
        apiGet<CollectionRun[]>(
          selectedDate ? `/v1/collections?race_date=${selectedDate}` : "/v1/collections"
        )
      ])
        .then(([jobData, collectionData]) => {
          setJobs(jobData);
          setCollections(collectionData);
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
  }, [selectedDate, selectedRaceId]);

  useEffect(() => {
    if (selectedRaceId) {
      void Promise.all([
        loadEntries(selectedRaceId),
        loadPredictionResults(selectedRaceId),
        loadIndependentRaceData(selectedRaceId),
        loadIntegrationRaceData(selectedRaceId),
        loadAiRaceData(selectedRaceId),
        loadRaceBets(selectedRaceId),
        loadRaceResult(selectedRaceId)
      ]);
    }
  }, [selectedRaceId]);

  return (
    <main className="appShell" id="main-content" aria-busy={isBusy}>
      <a className="skipLink" href="#primary-workspace">主要操作へ移動</a>
      <header className="topBar">
        <div>
          <p className="eyebrow">Keiba AI Studio</p>
          <h1>データ取込・レース確認</h1>
        </div>
        <div className="statusStrip">
          <span className={health?.status === "ok" ? "status ok" : "status warn"}>
            <Activity size={16} aria-hidden="true" />
            API {health?.status || "loading"}
          </span>
          <span className="status">
            <Database size={16} aria-hidden="true" />
            DB {health?.database || "-"}
          </span>
          <button className="iconButton" type="button" onClick={refreshAll} title="再読み込み">
            <RefreshCw size={18} aria-hidden="true" />
          </button>
        </div>
      </header>

      <nav className="workspaceNav" aria-label="主要画面">
        <a href="#dashboard">概要</a>
        <a href="#race-workspace">レース・予想</a>
        <a href="#ai-analysis">AI比較</a>
        <a href="#bet-planning">買い目候補</a>
        <a href="#performance">成績分析</a>
        <a href="#operations">ジョブ・品質</a>
      </nav>

      <section className="nextActionCard" aria-live="polite">
        <div>
          <span>次に行う操作</span>
          <strong>{nextAction.label}</strong>
          <p>{nextAction.detail}</p>
        </div>
        <a href={nextAction.anchor}>対象画面へ</a>
        <ol aria-label="処理順">
          <li className={predictionResults.length >= 2 ? "done" : "current"}>Python</li>
          <li className={independentAnalysis?.output_locked ? "done" : ""}>独立AI</li>
          <li className={integrationAnalysis?.integration_locked ? "done" : ""}>比較・統合</li>
          <li className={selectedRaceBets.length ? "done" : ""}>候補</li>
          <li className={raceResult?.result_status === "confirmed" ? "done" : ""}>結果</li>
        </ol>
      </section>

      {error && (
        <div className="errorBanner" role="alert">
          <strong>処理を完了できませんでした</strong>
          <span>{error}</span>
          <small>対象レースの品質・ジョブ失敗理由・APIキー設定を確認し、原因を解消してから再実行してください。</small>
        </div>
      )}

      {isBusy && <div className="busyBanner" role="status">処理中です。完了までこの画面を閉じずにお待ちください。</div>}

      <section className="toolbar">
        <label className="field workbookField">
          <span>開催日・Excel</span>
          <select
            aria-label="開催日Excelファイル"
            disabled={isBusy}
            value={selectedWorkbookFile}
            onChange={(event) => void handleWorkbookSelection(event.target.value)}
          >
            <option value="">未選択</option>
            {raceWorkbooks.map((workbook) => (
              <option key={workbook.file_name} value={workbook.file_name}>
                {workbook.race_date}｜{workbook.file_name}（{workbook.is_imported ? "読込済み" : "未読込"}）
              </option>
            ))}
          </select>
        </label>
        {workbookSelectionMessage && (
          <span className="workbookStatus" role="status">{workbookSelectionMessage}</span>
        )}
        <label className="searchField">
          <Search size={16} aria-hidden="true" />
          <input
            value={searchText}
            onChange={(event) => setSearchText(event.target.value)}
            placeholder="レース検索"
          />
        </label>
        <button disabled={isBusy} onClick={() => runJob("collection.race_info")} type="button">
          <Play size={16} aria-hidden="true" />
          レース取込
        </button>
        <button disabled={isBusy} onClick={() => runJob("collection.odds")} type="button">
          <Play size={16} aria-hidden="true" />
          オッズ取込
        </button>
        <button disabled={isBusy} onClick={() => runJob("prediction.feature_generation")} type="button">
          <ListRestart size={16} aria-hidden="true" />
          特徴量反映
        </button>
        <button
          disabled={isBusy || predictionJobActive || !selectedDate || !selectedRaceId}
          onClick={() => runJob("prediction.python")}
          title={
            !selectedDate || !selectedRaceId
              ? "開催日とレースを選択してください"
              : predictionJobActive
                ? "Python予想を実行中です"
                : "選択レースのPython予想を実行"
          }
          type="button"
        >
          {predictionJobActive ? <RefreshCw size={16} aria-hidden="true" /> : <Play size={16} aria-hidden="true" />}
          {predictionJobActive ? "Python予想 実行中" : "Python予想"}
        </button>
        <button
          disabled={isBusy || aiJobActive || !selectedDate || !selectedRaceId}
          onClick={() => runJob("ai.independent")}
          title={
            !selectedDate || !selectedRaceId
              ? "開催日とレースを選択してください"
              : aiJobActive
                ? "独立AI分析を実行中です"
                : "Python予想を見せずに選択レースを独立分析"
          }
          type="button"
        >
          {aiJobActive ? <RefreshCw size={16} aria-hidden="true" /> : <Play size={16} aria-hidden="true" />}
          {aiJobActive ? "独立AI 実行中" : "独立AI分析"}
        </button>
        <button
          disabled={isBusy || integrationJobActive || !canRunIntegration}
          onClick={runComparisonIntegration}
          title={
            !canRunIntegration
              ? "固定済み独立AI結果とPython予想が必要です"
              : integrationJobActive
                ? "Python/AI比較・統合を実行中です"
                : "固定済み独立AI結果を変更せずPython予想と比較・統合"
          }
          type="button"
        >
          {integrationJobActive ? <RefreshCw size={16} aria-hidden="true" /> : <ArrowUpDown size={16} aria-hidden="true" />}
          {integrationJobActive ? "比較・統合 実行中" : "Python/AI比較・統合"}
        </button>
        <button
          disabled={isBusy || !selectedRaceId || predictionResults.length < 2}
          onClick={generateBetPreviews}
          title="下の買い目設定を使って候補を保存します。自動投票は行いません"
          type="button"
        >
          <BadgeCent size={16} aria-hidden="true" />
          買い目候補を保存
        </button>
        <button disabled={isBusy} onClick={() => runJob("maintenance.data_quality_check")} type="button">
          <AlertTriangle size={16} aria-hidden="true" />
          品質チェック
        </button>
        <button disabled={isBusy} onClick={() => runJob("collection.results")} type="button">
          <Play size={16} aria-hidden="true" />
          結果取得
        </button>
        <button disabled={isBusy} onClick={() => runJob("result.settlement")} type="button">
          <CheckCircle2 size={16} aria-hidden="true" />
          精算
        </button>
      </section>

      <section className="metrics" id="dashboard">
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
          <span>独立AI済み</span>
          <strong>{independentAnalyses.filter((analysis) => analysis.status === "succeeded").length}</strong>
        </div>
        <div>
          <span>比較・統合済み</span>
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

      <span id="race-workspace" className="anchorTarget" aria-hidden="true" />
      <div className="workspace" id="primary-workspace">
        <section className="racePane">
          <div className="sectionHeader">
            <h2>レース一覧</h2>
            <span>{selectedDate || "-"}</span>
          </div>
          <div className="tableWrap">
            <table>
              <thead>
                <tr>
                  <th>R</th>
                  <th>場所</th>
                  <th>発走</th>
                  <th>レース名</th>
                  <th>条件</th>
                  <th>頭数</th>
                  <th>Python</th>
                  <th>独立AI</th>
                  <th>比較・統合</th>
                  <th>買い目</th>
                  <th>品質</th>
                </tr>
              </thead>
              <tbody>
                {visibleRaces.map((race) => {
                  const quality = qualityByRaceId.get(race.race_id);
                  const prediction = predictionByRaceId.get(race.race_id);
                  const independentStatus = independentByRaceId.get(race.race_id);
                  const integrationStatus = integrationByRaceId.get(race.race_id);
                  const bet = betByRaceId.get(race.race_id);
                  return (
                    <tr
                      key={race.race_id}
                      className={race.race_id === selectedRaceId ? "selected" : ""}
                      onClick={() => setSelectedRaceId(race.race_id)}
                    >
                      <td>{race.race_number}</td>
                      <td>{race.venue}</td>
                      <td>{race.start_time}</td>
                      <td>{race.name}</td>
                      <td>{race.course}</td>
                      <td>{race.headcount}</td>
                      <td>
                        {prediction ? (
                          <span className="predictionBadge done" title={prediction.top_horse_name || ""}>
                            済
                          </span>
                        ) : (
                          <span className="predictionBadge pending">未</span>
                        )}
                      </td>
                      <td>
                        {independentStatus?.status === "succeeded" ? (
                          <span
                            className="predictionBadge done"
                            title="Python予想を非表示にした独立分析が固定保存されています"
                          >
                            済
                          </span>
                        ) : independentStatus?.status === "failed" ? (
                          <span className="predictionBadge error" title={independentStatus.error_message || "独立AI分析失敗"}>
                            失敗
                          </span>
                        ) : (
                          <span className="predictionBadge pending">未</span>
                        )}
                      </td>
                      <td>
                        {integrationStatus?.status === "succeeded" ? (
                          <span
                            className="predictionBadge done"
                            title={integrationStatus.integration?.final_comment || "比較・統合結果を固定保存済み"}
                          >
                            済
                          </span>
                        ) : integrationStatus?.status === "degraded" ? (
                          <span className="predictionBadge warn" title={integrationStatus.error_message || "比較のみ完了"}>
                            一部
                          </span>
                        ) : integrationStatus?.status === "failed" ? (
                          <span className="predictionBadge error" title={integrationStatus.error_message || "比較・統合失敗"}>
                            失敗
                          </span>
                        ) : (
                          <span className="predictionBadge pending">未</span>
                        )}
                      </td>
                      <td>
                        {bet ? (
                          <span className={`rankBadge ${bet.rank.toLowerCase()}`} title={bet.skip_reason || bet.reason || ""}>
                            {bet.rank}
                          </span>
                        ) : (
                          <span className="rankBadge none">-</span>
                        )}
                      </td>
                      <td>
                        {quality ? (
                          <span
                            className={`qualityBadge ${quality.status.toLowerCase()}`}
                            title={quality.summary || ""}
                          >
                            {quality.status}
                          </span>
                        ) : (
                          <span className="qualityBadge gray">-</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>

        <section className="entryPane">
          <div className="sectionHeader">
            <h2>出走馬</h2>
            <span>
              {latestSelectedPredictionRun
                ? `${selectedRace?.race_id || "-"} / ${latestSelectedPredictionRun.prediction_version}`
                : selectedRace?.race_id || "-"}
            </span>
          </div>
          <div className="tableWrap">
            <table>
              <thead>
                <tr>
                  <th>
                    <EntrySortHeader label="馬番" columnKey="horse_no" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="馬名" columnKey="horse_name" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="騎手" columnKey="jockey" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="人気" columnKey="popularity" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="単勝" columnKey="win_odds" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="複勝" columnKey="place_odds" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="Python順位" columnKey="python_rank" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="Python score" columnKey="python_score" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="推定内率" columnKey="estimated_in3_rate" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="期待値" columnKey="expected_value" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="危険馬" columnKey="risk_flag" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="独立AI順位" columnKey="independent_ai_rank" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="独立AI自信度" columnKey="independent_ai_confidence" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <span>独立AI根拠</span>
                  </th>
                  <th>
                    <EntrySortHeader label="順位差" columnKey="comparison_gap" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <span>反対材料</span>
                  </th>
                  <th>
                    <EntrySortHeader label="統合順位" columnKey="integrated_rank" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="統合score" columnKey="integrated_score" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="統合自信度" columnKey="integrated_confidence" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <span>統合根拠</span>
                  </th>
                  <th>
                    <EntrySortHeader label="旧AI順位" columnKey="ai_rank" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="旧AI判断" columnKey="ai_action" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="旧AI補正" columnKey="ai_adjust_score" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="最終順位" columnKey="final_rank" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="馬券役割" columnKey="final_bet_role" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                  <th>
                    <EntrySortHeader label="評価理由" columnKey="reason" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={updateEntrySort} />
                  </th>
                </tr>
              </thead>
              <tbody>
                {displayEntries.map((entry) => {
                  const prediction = predictionByHorseNo.get(entry.horse_no);
                  const independent = independentByHorseNo.get(entry.horse_no);
                  const comparison = comparisonByHorseNo.get(entry.horse_no);
                  const integrated = integratedByHorseNo.get(entry.horse_no);
                  const ai = aiByHorseNo.get(entry.horse_no);
                  const final = finalByHorseNo.get(entry.horse_no);
                  return (
                    <tr key={entry.id}>
                      <td>{entry.horse_no}</td>
                      <td>{entry.horse_name}</td>
                      <td>{entry.jockey}</td>
                      <td>{prediction?.popularity ?? entry.popularity}</td>
                      <td>{formatNumber(prediction?.win_odds ?? entry.win_odds)}</td>
                      <td>{formatNumber(prediction?.place_odds ?? entry.place_odds)}</td>
                      <td>{prediction?.prediction_rank ?? entry.prediction_rank}</td>
                      <td>{formatNumber(prediction?.prediction_score ?? entry.prediction_score, 2)}</td>
                      <td>{formatNumber(prediction?.estimated_in3_rate ?? entry.estimated_in3_rate, 2)}</td>
                      <td>{formatNumber(prediction?.expected_value ?? entry.expected_value, 2)}</td>
                      <td>
                        {prediction?.risk_flag ? (
                          <span className="riskBadge risk" title={prediction.risk_reason || ""}>
                            あり
                          </span>
                        ) : (
                          <span className="riskBadge safe">なし</span>
                        )}
                      </td>
                      <td>{independent?.ai_rank ?? ""}</td>
                      <td>{independent ? formatPercent(independent.confidence * 100) : ""}</td>
                      <td className="reasonCell">
                        {independent ? (
                          <span title={[...independent.positive_factors, ...independent.negative_factors, ...independent.uncertainties].join(" / ")}>
                            {independent.rationale}
                          </span>
                        ) : (
                          ""
                        )}
                      </td>
                      <td>{comparison?.rank_gap ?? ""}</td>
                      <td>
                        {comparison ? (
                          <span
                            className={comparison.material_opposition ? "riskBadge risk" : "riskBadge safe"}
                            title={comparison.counterpoints.join(" / ") || comparison.uncertainty}
                          >
                            {comparison.material_opposition ? "あり" : "なし"}
                          </span>
                        ) : (
                          ""
                        )}
                      </td>
                      <td>{integrated?.integrated_rank ?? ""}</td>
                      <td>{formatNumber(integrated?.integrated_score, 2)}</td>
                      <td>{integrated ? formatPercent(integrated.confidence * 100) : ""}</td>
                      <td className="reasonCell">
                        {integrated ? (
                          <span title={integrated.risk_summary}>
                            {integrated.decision_basis}: {integrated.reasons.join(" / ")}
                          </span>
                        ) : (
                          ""
                        )}
                      </td>
                      <td>{ai?.ai_rank ?? ""}</td>
                      <td>
                        {ai ? (
                          <span className={`aiActionBadge ${ai.ai_action}`}>{ai.ai_action}</span>
                        ) : (
                          ""
                        )}
                      </td>
                      <td>{ai ? formatNumber(ai.ai_adjust_score, 1) : ""}</td>
                      <td>{final?.final_rank ?? ""}</td>
                      <td>{final?.final_bet_role ?? ""}</td>
                      <td className="reasonCell">{ai?.ai_reason || prediction?.evaluation_reason || ""}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <div className="lowerGrid">
        <section>
          <div className="sectionHeader">
            <h2>取得状況</h2>
            <span>{visibleCollections.length}</span>
          </div>
          <div className="logList">
            {visibleCollections.slice(0, 8).map((collection) => (
              <div key={collection.id} className="collectionRow">
                <div className="collectionMain">
                  <span className={`pill ${collection.status}`}>{collection.status}</span>
                  <strong>{collection.data_kind}</strong>
                  <small>{collection.source_code}</small>
                  <button
                    disabled={isBusy}
                    onClick={() => retryCollection(collection.job_run_id)}
                    title="同じ対象を強制再取得"
                    type="button"
                  >
                    <RefreshCw size={14} aria-hidden="true" />
                    再取得
                  </button>
                </div>
                <div className="collectionMeta">
                  <span>品質 {collection.quality_status || "-"}</span>
                  <span>{collection.cache_hit ? "cache hit" : `試行 ${collection.attempt_count}`}</span>
                  <span>retry {collection.retry_count}</span>
                  <span>{collection.mode}</span>
                </div>
                {collection.error_message && (
                  <p className="collectionError">
                    {collection.error_code || "COLLECTION_ERROR"}: {collection.error_message}
                  </p>
                )}
              </div>
            ))}
            {visibleCollections.length === 0 && (
              <div className="emptyState">取得履歴はまだありません</div>
            )}
          </div>
        </section>

        <section id="ai-analysis">
          <div className="sectionHeader">
            <h2>独立AI分析</h2>
            <span>
              {independentAnalysis
                ? `${independentAnalysis.status} / #${independentAnalysis.analysis_sequence}`
                : "未実行"}
            </span>
          </div>
          {independentAnalysis?.output ? (
            <div className="independentAiPanel">
              <div className="independentAiSummary">
                <div>
                  <span>判定</span>
                  <strong>{independentAnalysis.output.decision_status}</strong>
                </div>
                <div>
                  <span>データ信頼度</span>
                  <strong>{independentAnalysis.output.data_confidence}</strong>
                </div>
                <div>
                  <span>展開</span>
                  <strong>{independentAnalysis.output.race_assessment.pace_assessment}</strong>
                </div>
                <div>
                  <span>固定保存</span>
                  <strong>{independentAnalysis.output_locked ? "済" : "要確認"}</strong>
                </div>
              </div>
              <p className="independentAiComment">{independentAnalysis.output.race_assessment.summary}</p>
              <div className="independentAiRanks">
                {[...independentAnalysis.output.runners]
                  .sort((left, right) => (left.ai_rank ?? 99) - (right.ai_rank ?? 99))
                  .slice(0, 5)
                  .map((runner) => (
                    <div key={runner.horse_no}>
                      <span>{runner.ai_rank ? `${runner.ai_rank}位` : "順位不明"}</span>
                      <strong>{runner.horse_no} {runner.horse_name}</strong>
                      <small>{runner.rationale}</small>
                    </div>
                  ))}
              </div>
              <div className="resultStrip">
                <span>{independentAnalysis.model_name}</span>
                <strong>Python結果 非表示</strong>
                <span title={independentAnalysis.output_hash || ""}>
                  hash {independentAnalysis.output_hash?.slice(0, 12) || "-"}
                </span>
              </div>
            </div>
          ) : independentAnalysis?.status === "failed" ? (
            <div className="independentAiError">
              <strong>独立AI分析に失敗しました</strong>
              <p>{independentAnalysis.error_message || "ジョブログを確認してください"}</p>
            </div>
          ) : (
            <div className="emptyState">選択レースの独立AI分析はまだありません</div>
          )}
        </section>

        <section>
          <div className="sectionHeader">
            <h2>Python / 独立AI 比較・統合</h2>
            <span>
              {integrationAnalysis
                ? `${integrationAnalysis.status} / #${integrationAnalysis.analysis_sequence}`
                : "未実行"}
            </span>
          </div>
          {integrationAnalysis?.comparison ? (
            <div className="integrationPanel">
              <div className="independentAiSummary">
                <div>
                  <span>一致度</span>
                  <strong>{integrationAnalysis.comparison.overall_alignment}</strong>
                </div>
                <div>
                  <span>反対材料</span>
                  <strong>
                    {integrationAnalysis.comparison.opposition.has_material_opposition
                      ? `${integrationAnalysis.comparison.opposition.horse_nos.length}頭`
                      : "なし"}
                  </strong>
                </div>
                <div>
                  <span>統合方針</span>
                  <strong>{integrationAnalysis.integration?.integration_strategy || "比較のみ"}</strong>
                </div>
                <div>
                  <span>手動確認</span>
                  <strong>
                    {integrationAnalysis.integration?.manual_review_required ||
                    integrationAnalysis.comparison.manual_review_required
                      ? "必要"
                      : "不要"}
                  </strong>
                </div>
              </div>
              <p className="independentAiComment">
                {integrationAnalysis.comparison.opposition.summary}。{integrationAnalysis.comparison.summary}
              </p>
              {integrationAnalysis.integration ? (
                <div className="independentAiRanks">
                  {[...integrationAnalysis.integration.horses]
                    .sort((left, right) => left.integrated_rank - right.integrated_rank)
                    .slice(0, 5)
                    .map((horse) => (
                      <div key={horse.horse_no}>
                        <span>{horse.integrated_rank}位</span>
                        <strong>{horse.horse_no} {horse.horse_name}</strong>
                        <small>
                          score {formatNumber(horse.integrated_score, 2)} / {horse.decision_basis} / Python {horse.python_rank}位 / 独立AI {horse.ai_rank ?? "不明"}位 — {horse.reasons.join(" / ")}
                        </small>
                      </div>
                    ))}
                </div>
              ) : (
                <div className="independentAiError">
                  <strong>統合処理は完了していません</strong>
                  <p>{integrationAnalysis.error_message || "比較結果のみ固定保存されています"}</p>
                </div>
              )}
              <div className="resultStrip">
                <span title={integrationAnalysis.comparison_output_hash || ""}>
                  比較hash {integrationAnalysis.comparison_output_hash?.slice(0, 10) || "-"}
                </span>
                <strong>{integrationAnalysis.comparison_locked ? "比較固定済み" : "比較未固定"}</strong>
                <span title={integrationAnalysis.integration_output_hash || ""}>
                  統合hash {integrationAnalysis.integration_output_hash?.slice(0, 10) || "-"}
                </span>
              </div>
            </div>
          ) : integrationAnalysis?.status === "failed" ? (
            <div className="independentAiError">
              <strong>比較・統合に失敗しました</strong>
              <p>{integrationAnalysis.error_message || "ジョブログを確認してください"}</p>
            </div>
          ) : (
            <div className="emptyState">固定済み独立AI結果とPython予想を比較すると表示されます</div>
          )}
        </section>

        <section>
          <div className="sectionHeader">
            <h2>旧AI補正（互換表示）</h2>
            <span>{aiEvaluations.length ? `${aiDiff.upgrades.length}上げ / ${aiDiff.downgrades.length}下げ` : "未実行"}</span>
          </div>
          <div className="aiDiffGrid">
            <div>
              <strong>上げ馬</strong>
              <p>{aiDiff.upgrades.map((item) => `${item.horse_no} ${item.horse_name}`).join(" / ") || "-"}</p>
            </div>
            <div>
              <strong>下げ馬</strong>
              <p>{aiDiff.downgrades.map((item) => `${item.horse_no} ${item.horse_name}`).join(" / ") || "-"}</p>
            </div>
            <div>
              <strong>危険人気馬</strong>
              <p>{aiDiff.dangerous.map((item) => `${item.horse_no} ${item.horse_name}`).join(" / ") || "-"}</p>
            </div>
            <div>
              <strong>穴馬候補</strong>
              <p>{aiDiff.values.map((item) => `${item.horse_no} ${item.horse_name}`).join(" / ") || "-"}</p>
            </div>
          </div>
          <div className="resultStrip">
            <span>AI買い目補正</span>
            <strong>{aiBetStrategy ? `${aiBetStrategy.strategy_type} ${aiBetStrategy.bet_count}点` : "未作成"}</strong>
            <span>{aiBetStrategy?.suggestion || ""}</span>
          </div>
        </section>

        <section className="helpPanel">
          <div className="sectionHeader">
            <h2>操作ヘルプ</h2>
            <span>安全な順序</span>
          </div>
          <details>
            <summary>予想から成績確認まで</summary>
            <ol>
              <li>レースと品質状態を確認してPython予想を実行します。</li>
              <li>独立AIを固定してからPython/AI比較・統合を実行します。</li>
              <li>予算と最大点数を設定し、買い目候補だけを保存します。</li>
              <li>外部購入した場合だけ、確認後に手動購入を記録します。</li>
              <li>確定結果を取得して精算し、条件別成績を確認します。</li>
            </ol>
          </details>
        </section>

        <section id="bet-planning">
          <div className="sectionHeader">
            <h2>買い目候補</h2>
            <span>{selectedRaceBets.length}</span>
          </div>
          <div className="betControlGrid">
            <label>
              <span>予想source</span>
              <select
                value={betSourceMode}
                onChange={(event) => setBetSourceMode(event.target.value as "python" | "ai_integrated" | "both")}
              >
                <option value="python">Python案</option>
                <option value="ai_integrated" disabled={!integrationAnalysis?.integration_locked}>AI統合案</option>
                <option value="both" disabled={!integrationAnalysis?.integration_locked}>両方を比較</option>
              </select>
            </label>
            <label>
              <span>券種</span>
              <select value={betType} onChange={(event) => setBetType(event.target.value as "3連複" | "ワイド")}>
                <option value="3連複">3連複</option>
                <option value="ワイド">ワイド</option>
              </select>
            </label>
            <label>
              <span>方式</span>
              <select
                value={betStrategyMode}
                onChange={(event) => setBetStrategyMode(event.target.value as "formation" | "box" | "wheel")}
              >
                <option value="formation">フォーメーション</option>
                <option value="wheel">1頭軸流し</option>
                <option value="box">5頭BOX</option>
              </select>
            </label>
            <label>
              <span>1点</span>
              <input type="number" min={100} step={100} value={stakePerPoint} onChange={(event) => setStakePerPoint(Number(event.target.value))} />
            </label>
            <label>
              <span>レース上限</span>
              <input type="number" min={100} step={100} value={maxRaceAmount} onChange={(event) => setMaxRaceAmount(Number(event.target.value))} />
            </label>
            <label>
              <span>1日上限</span>
              <input type="number" min={100} step={100} value={maxDayAmount} onChange={(event) => setMaxDayAmount(Number(event.target.value))} />
            </label>
            <label>
              <span>最大点数</span>
              <input type="number" min={1} max={500} value={maxBetPoints} onChange={(event) => setMaxBetPoints(Number(event.target.value))} />
            </label>
            <button
              disabled={isBusy || !selectedRaceId || predictionResults.length < 2}
              onClick={generateBetPreviews}
              type="button"
            >
              <BadgeCent size={15} aria-hidden="true" />
              候補を保存
            </button>
          </div>
          <div className="betSafetyNotice">
            <strong>自動投票は無効</strong>
            <span>ここで保存するのは買い目候補だけです。購入済み操作も外部購入後の手動記録です。</span>
            {betGenerationMessage && <span>{betGenerationMessage}</span>}
          </div>
          <div className="betList">
            {selectedRaceBets.length === 0 && (
              <div className="emptyRow">買い目候補はまだありません</div>
            )}
            {selectedRaceBets.map((bet) => (
              <div key={bet.id} className="betRow">
                <div className="betMain">
                  <span className={`rankBadge ${bet.rank.toLowerCase()}`}>{bet.rank}</span>
                  <span className={`sourceBadge ${bet.source_type}`}>{bet.source_type === "ai_integrated" ? "AI統合案" : bet.source_type === "python" ? "Python案" : "旧AI案"}</span>
                  <strong>{bet.bet_type}</strong>
                  <span>{bet.strategy}</span>
                  <span className={`pill ${bet.status}`}>{bet.status}</span>
                </div>
                <div className="betMeta">
                  <span>軸 {bet.axis_horse_nos?.join("-") || "-"}</span>
                  <span>相手 {bet.opponent_horse_nos?.join("-") || "-"}</span>
                  <span>{bet.points}点</span>
                  <span>{formatCurrency(bet.stake_per_point)} / 点</span>
                  <strong>{formatCurrency(bet.total_amount)}</strong>
                  <span>上限 {formatCurrency(bet.max_race_amount)} / 日 {formatCurrency(bet.max_day_amount)}</span>
                </div>
                <p>{bet.skip_reason || bet.reason || ""}</p>
                {bet.warning_codes.length > 0 && (
                  <p className="betWarnings">警告: {bet.warning_codes.join(" / ")}</p>
                )}
                <div className="betActions">
                  <button
                    disabled={isBusy || !["candidate", "review_required"].includes(bet.status)}
                    onClick={() => updateBetCandidateStatus(bet.id, "planned", "購入予定として確認")}
                    type="button"
                  >
                    <CheckCircle2 size={14} aria-hidden="true" />
                    {bet.status === "review_required" ? "確認して購入候補へ" : "購入候補として確認"}
                  </button>
                  <button
                    disabled={isBusy || bet.status !== "planned"}
                    onClick={() => updateBetCandidateStatus(bet.id, "purchased", "外部で購入した事実を手動記録")}
                    title="JRA等での購入処理は行わず、外部購入後の記録だけを更新します"
                    type="button"
                  >
                    <BadgeCent size={14} aria-hidden="true" />
                    手動購入を記録
                  </button>
                  <button
                    disabled={isBusy || bet.status === "skipped" || bet.status === "settled"}
                    onClick={() => updateBetCandidateStatus(bet.id, "skipped", "手動で見送り")}
                    type="button"
                  >
                    <CircleSlash size={14} aria-hidden="true" />
                    見送り
                  </button>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section id="performance">
          <div className="sectionHeader">
            <h2>成績分析</h2>
            <span>{selectedDate || "-"}</span>
          </div>
          <div className="analyticsControls">
            <label>
              <span>期間</span>
              <select value={analyticsScope} onChange={(event) => setAnalyticsScope(event.target.value as "day" | "all")}>
                <option value="day">選択日</option>
                <option value="all">全期間</option>
              </select>
            </label>
            <label>
              <span>source</span>
              <select value={analyticsSource} onChange={(event) => setAnalyticsSource(event.target.value)}>
                <option value="">すべて</option>
                <option value="python">Python案</option>
                <option value="ai_integrated">AI統合案</option>
                <option value="legacy_ai">旧AI案</option>
              </select>
            </label>
            <label>
              <span>券種</span>
              <select value={analyticsBetType} onChange={(event) => setAnalyticsBetType(event.target.value)}>
                <option value="">すべて</option>
                <option value="3連複">3連複</option>
                <option value="ワイド">ワイド</option>
              </select>
            </label>
            <label>
              <span>内訳</span>
              <select value={analyticsGroup} onChange={(event) => setAnalyticsGroup(event.target.value)}>
                <option value="source_type">source別</option>
                <option value="bet_type">券種別</option>
                <option value="venue">競馬場別</option>
                <option value="course">距離・course別</option>
                <option value="race_class">class別</option>
                <option value="prediction_model">Python model別</option>
                <option value="ai_model">AI model別</option>
              </select>
            </label>
            <button onClick={() => loadAnalytics(selectedDate)} type="button">
              <RefreshCw size={14} aria-hidden="true" />
              集計更新
            </button>
          </div>
          <div className="analyticsGrid">
            <div>
              <span>精算</span>
              <strong>{analytics?.settled_bets ?? 0}</strong>
            </div>
            <div>
              <span>的中</span>
              <strong>{analytics?.hits ?? 0}</strong>
            </div>
            <div>
              <span>投資</span>
              <strong>{formatCurrency(analytics?.stake_amount)}</strong>
            </div>
            <div>
              <span>払戻</span>
              <strong>{formatCurrency(analytics?.payout_amount)}</strong>
            </div>
            <div>
              <span>損益</span>
              <strong>{formatCurrency(analytics?.profit_loss)}</strong>
            </div>
            <div>
              <span>回収率</span>
              <strong>{formatPercent(analytics?.roi)}</strong>
            </div>
            <div>
              <span>的中率</span>
              <strong>{formatPercent(analytics?.hit_rate)}</strong>
            </div>
            <div>
              <span>最大連敗</span>
              <strong>{analytics?.max_consecutive_losses ?? 0}</strong>
            </div>
            <div>
              <span>最大DD</span>
              <strong>{formatCurrency(analytics?.max_drawdown)}</strong>
            </div>
          </div>
          <div className="analyticsBreakdown">
            {(analytics?.breakdown || []).map((item) => (
              <div key={`${item.dimension}-${item.value}`}>
                <strong>{item.value}</strong>
                <span>{item.settled_bets}件 / 的中 {formatPercent(item.hit_rate)}</span>
                <span>回収 {formatPercent(item.roi)} / 損益 {formatCurrency(item.profit_loss)}</span>
              </div>
            ))}
            {analytics?.breakdown?.length === 0 && <div className="emptyRow">集計対象はまだありません</div>}
          </div>
          <div className="resultStrip">
            <span>選択レース結果</span>
            <strong>{raceResult?.finish_order?.slice(0, 3).join("-") || "未登録"}</strong>
            <span>{raceResult ? `${raceResult.result_status} / 払戻 ${raceResult.payouts_json.length}件` : ""}</span>
          </div>
        </section>

        <section id="operations">
          <div className="sectionHeader">
            <h2>ジョブ</h2>
            <span>{jobs.length}</span>
          </div>
          <div className="logList">
            {jobs.slice(0, 8).map((job) => (
              <div key={job.id} className="logRow">
                <span className={`pill ${job.status}`}>{job.status}</span>
                <strong>{job.job_type}</strong>
                <small>{new Date(job.created_at).toLocaleString("ja-JP")}</small>
                {job.status === "failed" && job.message && (
                  <small title={job.message}>失敗理由: {job.message}</small>
                )}
              </div>
            ))}
          </div>
        </section>

        <section>
          <div className="sectionHeader">
            <h2>品質チェック</h2>
            <AlertTriangle size={16} aria-hidden="true" />
          </div>
          <div className="logList">
            {issues.slice(0, 8).map((issue) => (
              <div key={issue.id} className="issueRow">
                <span className={`pill ${issue.severity}`}>{issue.severity}</span>
                <strong>{issue.code}</strong>
                <small>{issue.message}</small>
              </div>
            ))}
          </div>
        </section>
      </div>
    </main>
  );
}
