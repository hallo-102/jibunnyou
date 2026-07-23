import { BadgeCent, CheckCircle2, CircleSlash } from "lucide-react";

export type BetSourceMode = "python" | "ai_integrated" | "both";
export type BetType = "3連複" | "ワイド";
export type BetStrategyMode = "formation" | "box" | "wheel";

export type BetCandidate = {
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

export type BetGenerationResult = {
  generated: number;
  candidates: number;
  skipped: number;
  blocked: number;
  review_required: number;
  total_planned_amount: number;
  warnings: string[];
};

type BetPlanningPanelProps = {
  betGenerationMessage: string;
  betSourceMode: BetSourceMode;
  betStrategyMode: BetStrategyMode;
  betType: BetType;
  canUseIntegratedAi: boolean;
  canGenerate: boolean;
  formatCurrency: (value?: number | null) => string;
  isBusy: boolean;
  maxBetPoints: number;
  maxDayAmount: number;
  maxRaceAmount: number;
  onBetSourceModeChange: (value: BetSourceMode) => void;
  onBetStrategyModeChange: (value: BetStrategyMode) => void;
  onBetTypeChange: (value: BetType) => void;
  onGenerate: () => void;
  onMaxBetPointsChange: (value: number) => void;
  onMaxDayAmountChange: (value: number) => void;
  onMaxRaceAmountChange: (value: number) => void;
  onStakePerPointChange: (value: number) => void;
  onStatusChange: (betId: number, status: string, reason?: string) => void;
  selectedRaceBets: BetCandidate[];
  stakePerPoint: number;
};

export default function BetPlanningPanel({
  betGenerationMessage,
  betSourceMode,
  betStrategyMode,
  betType,
  canUseIntegratedAi,
  canGenerate,
  formatCurrency,
  isBusy,
  maxBetPoints,
  maxDayAmount,
  maxRaceAmount,
  onBetSourceModeChange,
  onBetStrategyModeChange,
  onBetTypeChange,
  onGenerate,
  onMaxBetPointsChange,
  onMaxDayAmountChange,
  onMaxRaceAmountChange,
  onStakePerPointChange,
  onStatusChange,
  selectedRaceBets,
  stakePerPoint
}: BetPlanningPanelProps) {
  return (
    <section data-route-section="bets" id="bet-planning">
      <div className="sectionHeader">
        <h2>買い目候補</h2>
        <span>{selectedRaceBets.length}</span>
      </div>
      <div className="betControlGrid">
        <label>
          <span>予想source</span>
          <select
            aria-label="買い目予想ソース"
            value={betSourceMode}
            onChange={(event) => onBetSourceModeChange(event.target.value as BetSourceMode)}
          >
            <option value="python">Python案</option>
            <option value="ai_integrated" disabled={!canUseIntegratedAi}>AI統合案</option>
            <option value="both" disabled={!canUseIntegratedAi}>両方を比較</option>
          </select>
        </label>
        <label>
          <span>券種</span>
          <select
            aria-label="買い目券種"
            value={betType}
            onChange={(event) => onBetTypeChange(event.target.value as BetType)}
          >
            <option value="3連複">3連複</option>
            <option value="ワイド">ワイド</option>
          </select>
        </label>
        <label>
          <span>方式</span>
          <select
            aria-label="買い目方式"
            value={betStrategyMode}
            onChange={(event) => onBetStrategyModeChange(event.target.value as BetStrategyMode)}
          >
            <option value="formation">フォーメーション</option>
            <option value="wheel">1頭軸流し</option>
            <option value="box">5頭BOX</option>
          </select>
        </label>
        <label>
          <span>1点</span>
          <input
            aria-label="買い目1点金額"
            min={100}
            step={100}
            type="number"
            value={stakePerPoint}
            onChange={(event) => onStakePerPointChange(Number(event.target.value))}
          />
        </label>
        <label>
          <span>レース上限</span>
          <input
            aria-label="買い目レース上限"
            min={100}
            step={100}
            type="number"
            value={maxRaceAmount}
            onChange={(event) => onMaxRaceAmountChange(Number(event.target.value))}
          />
        </label>
        <label>
          <span>1日上限</span>
          <input
            aria-label="買い目1日上限"
            min={100}
            step={100}
            type="number"
            value={maxDayAmount}
            onChange={(event) => onMaxDayAmountChange(Number(event.target.value))}
          />
        </label>
        <label>
          <span>最大点数</span>
          <input
            aria-label="買い目最大点数"
            max={500}
            min={1}
            type="number"
            value={maxBetPoints}
            onChange={(event) => onMaxBetPointsChange(Number(event.target.value))}
          />
        </label>
        <button disabled={isBusy || !canGenerate} onClick={onGenerate} type="button">
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
              <span className={`sourceBadge ${bet.source_type}`}>
                {bet.source_type === "ai_integrated"
                  ? "AI統合案"
                  : bet.source_type === "python"
                    ? "Python案"
                    : "旧AI案"}
              </span>
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
                onClick={() => onStatusChange(bet.id, "planned", "購入予定として確認")}
                type="button"
              >
                <CheckCircle2 size={14} aria-hidden="true" />
                {bet.status === "review_required" ? "確認して購入候補へ" : "購入候補として確認"}
              </button>
              <button
                disabled={isBusy || bet.status !== "planned"}
                onClick={() => onStatusChange(bet.id, "purchased", "外部で購入した事実を手動記録")}
                title="JRA等での購入処理は行わず、外部購入後の記録だけを更新します"
                type="button"
              >
                <BadgeCent size={14} aria-hidden="true" />
                手動購入を記録
              </button>
              <button
                disabled={isBusy || bet.status === "skipped" || bet.status === "settled"}
                onClick={() => onStatusChange(bet.id, "skipped", "手動で見送り")}
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
  );
}
