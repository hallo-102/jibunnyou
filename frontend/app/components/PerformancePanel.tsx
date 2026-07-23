import { RefreshCw } from "lucide-react";

export type RaceResult = {
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

export type AnalyticsSummary = {
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

type PerformancePanelProps = {
  analytics: AnalyticsSummary | null;
  betType: string;
  formatCurrency: (value?: number | null) => string;
  formatPercent: (value?: number | null) => string;
  group: string;
  onBetTypeChange: (value: string) => void;
  onGroupChange: (value: string) => void;
  onRefresh: () => void;
  onScopeChange: (value: "day" | "all") => void;
  onSourceChange: (value: string) => void;
  raceResult: RaceResult | null;
  scope: "day" | "all";
  selectedDate: string;
  source: string;
};

export default function PerformancePanel({
  analytics,
  betType,
  formatCurrency,
  formatPercent,
  group,
  onBetTypeChange,
  onGroupChange,
  onRefresh,
  onScopeChange,
  onSourceChange,
  raceResult,
  scope,
  selectedDate,
  source
}: PerformancePanelProps) {
  return (
    <section data-route-section="performance" id="performance">
      <div className="sectionHeader">
        <h2>成績分析</h2>
        <span>{selectedDate || "-"}</span>
      </div>
      <div className="analyticsControls">
        <label>
          <span>期間</span>
          <select
            aria-label="成績集計期間"
            value={scope}
            onChange={(event) => onScopeChange(event.target.value as "day" | "all")}
          >
            <option value="day">選択日</option>
            <option value="all">全期間</option>
          </select>
        </label>
        <label>
          <span>source</span>
          <select
            aria-label="成績集計ソース"
            value={source}
            onChange={(event) => onSourceChange(event.target.value)}
          >
            <option value="">すべて</option>
            <option value="python">Python案</option>
            <option value="ai_integrated">AI統合案</option>
            <option value="legacy_ai">旧AI案</option>
          </select>
        </label>
        <label>
          <span>券種</span>
          <select
            aria-label="成績集計券種"
            value={betType}
            onChange={(event) => onBetTypeChange(event.target.value)}
          >
            <option value="">すべて</option>
            <option value="3連複">3連複</option>
            <option value="ワイド">ワイド</option>
          </select>
        </label>
        <label>
          <span>内訳</span>
          <select
            aria-label="成績集計内訳"
            value={group}
            onChange={(event) => onGroupChange(event.target.value)}
          >
            <option value="source_type">source別</option>
            <option value="bet_type">券種別</option>
            <option value="venue">競馬場別</option>
            <option value="course">距離・course別</option>
            <option value="race_class">class別</option>
            <option value="prediction_model">Python model別</option>
            <option value="ai_model">AI model別</option>
          </select>
        </label>
        <button onClick={onRefresh} type="button">
          <RefreshCw size={14} aria-hidden="true" />
          集計更新
        </button>
      </div>
      <div className="analyticsGrid">
        <div><span>精算</span><strong>{analytics?.settled_bets ?? 0}</strong></div>
        <div><span>的中</span><strong>{analytics?.hits ?? 0}</strong></div>
        <div><span>投資</span><strong>{formatCurrency(analytics?.stake_amount)}</strong></div>
        <div><span>払戻</span><strong>{formatCurrency(analytics?.payout_amount)}</strong></div>
        <div><span>損益</span><strong>{formatCurrency(analytics?.profit_loss)}</strong></div>
        <div><span>回収率</span><strong>{formatPercent(analytics?.roi)}</strong></div>
        <div><span>的中率</span><strong>{formatPercent(analytics?.hit_rate)}</strong></div>
        <div><span>最大連敗</span><strong>{analytics?.max_consecutive_losses ?? 0}</strong></div>
        <div><span>最大DD</span><strong>{formatCurrency(analytics?.max_drawdown)}</strong></div>
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
  );
}
