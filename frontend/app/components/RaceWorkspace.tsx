import { ArrowDown, ArrowUp, ArrowUpDown } from "lucide-react";
import type {
  AiEvaluation,
  AiIntegrationAnalysis,
  HorseComparison,
  IndependentAiAnalysis,
  IndependentAiRunner,
  IntegratedHorsePrediction
} from "./AiAnalysisPanels";
import type { BetCandidate } from "./BetPlanningPanel";

export type Race = {
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

export type Entry = {
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

export type PredictionRun = {
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

export type PredictionResult = {
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

export type PredictionStatus = {
  race_id: string;
  latest_run_id: string;
  status: string;
  result_count: number;
  predicted_at?: string | null;
  top_horse_no?: number | null;
  top_horse_name?: string | null;
  top_score?: number | null;
};

export type FinalPrediction = {
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

export type QualityStatus = {
  id: number;
  race_id: string;
  status: string;
  summary?: string | null;
  issue_count: number;
  red_count: number;
  yellow_count: number;
  checked_at: string;
};

export type EntrySortKey =
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

export type SortDirection = "asc" | "desc";

type RaceWorkspaceProps = {
  aiByHorseNo: Map<number, AiEvaluation>;
  betByRaceId: Map<string, BetCandidate>;
  comparisonByHorseNo: Map<number, HorseComparison>;
  displayEntries: Entry[];
  entrySort: { key: EntrySortKey; direction: SortDirection };
  finalByHorseNo: Map<number, FinalPrediction>;
  formatNumber: (value?: number | null, digits?: number) => string;
  formatPercent: (value?: number | null) => string;
  independentByHorseNo: Map<number, IndependentAiRunner>;
  independentByRaceId: Map<string, IndependentAiAnalysis>;
  integratedByHorseNo: Map<number, IntegratedHorsePrediction>;
  integrationByRaceId: Map<string, AiIntegrationAnalysis>;
  latestSelectedPredictionRun: PredictionRun | null;
  onEntrySort: (key: EntrySortKey) => void;
  onSelectedRaceChange: (raceId: string) => void;
  predictionByHorseNo: Map<number, PredictionResult>;
  predictionByRaceId: Map<string, PredictionStatus>;
  qualityByRaceId: Map<string, QualityStatus>;
  selectedDate: string;
  selectedRace?: Race;
  selectedRaceId: string;
  visibleRaces: Race[];
};

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

export default function RaceWorkspace({
  aiByHorseNo,
  betByRaceId,
  comparisonByHorseNo,
  displayEntries,
  entrySort,
  finalByHorseNo,
  formatNumber,
  formatPercent,
  independentByHorseNo,
  independentByRaceId,
  integratedByHorseNo,
  integrationByRaceId,
  latestSelectedPredictionRun,
  onEntrySort,
  onSelectedRaceChange,
  predictionByHorseNo,
  predictionByRaceId,
  qualityByRaceId,
  selectedDate,
  selectedRace,
  selectedRaceId,
  visibleRaces
}: RaceWorkspaceProps) {
  return (
    <>
      <span id="race-workspace" className="anchorTarget" data-route-section="races" aria-hidden="true" />
      <div className="workspace" data-route-section="races" id="primary-workspace">
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
                      onClick={() => onSelectedRaceChange(race.race_id)}
                    >
                      <td>{race.race_number}</td>
                      <td>{race.venue}</td>
                      <td>{race.start_time}</td>
                      <td>{race.name}</td>
                      <td>{race.course}</td>
                      <td>{race.headcount}</td>
                      <td>
                        {prediction ? (
                          <span className="predictionBadge done" title={prediction.top_horse_name || ""}>済</span>
                        ) : (
                          <span className="predictionBadge pending">未</span>
                        )}
                      </td>
                      <td>
                        {independentStatus?.status === "succeeded" ? (
                          <span className="predictionBadge done" title="Python予想を非表示にした独立分析が固定保存されています">済</span>
                        ) : independentStatus?.status === "failed" ? (
                          <span className="predictionBadge error" title={independentStatus.error_message || "独立AI分析失敗"}>失敗</span>
                        ) : (
                          <span className="predictionBadge pending">未</span>
                        )}
                      </td>
                      <td>
                        {integrationStatus?.status === "succeeded" ? (
                          <span className="predictionBadge done" title={integrationStatus.integration?.final_comment || "比較・統合結果を固定保存済み"}>済</span>
                        ) : integrationStatus?.status === "degraded" ? (
                          <span className="predictionBadge warn" title={integrationStatus.error_message || "比較のみ完了"}>一部</span>
                        ) : integrationStatus?.status === "failed" ? (
                          <span className="predictionBadge error" title={integrationStatus.error_message || "比較・統合失敗"}>失敗</span>
                        ) : (
                          <span className="predictionBadge pending">未</span>
                        )}
                      </td>
                      <td>
                        {bet ? (
                          <span className={`rankBadge ${bet.rank.toLowerCase()}`} title={bet.skip_reason || bet.reason || ""}>{bet.rank}</span>
                        ) : (
                          <span className="rankBadge none">-</span>
                        )}
                      </td>
                      <td>
                        {quality ? (
                          <span className={`qualityBadge ${quality.status.toLowerCase()}`} title={quality.summary || ""}>{quality.status}</span>
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
                  {([
                    ["馬番", "horse_no"],
                    ["馬名", "horse_name"],
                    ["騎手", "jockey"],
                    ["人気", "popularity"],
                    ["単勝", "win_odds"],
                    ["複勝", "place_odds"],
                    ["Python順位", "python_rank"],
                    ["Python score", "python_score"],
                    ["推定内率", "estimated_in3_rate"],
                    ["期待値", "expected_value"],
                    ["危険馬", "risk_flag"],
                    ["独立AI順位", "independent_ai_rank"],
                    ["独立AI自信度", "independent_ai_confidence"]
                  ] as Array<[string, EntrySortKey]>).map(([label, key]) => (
                    <th key={key}>
                      <EntrySortHeader label={label} columnKey={key} sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={onEntrySort} />
                    </th>
                  ))}
                  <th><span>独立AI根拠</span></th>
                  <th><EntrySortHeader label="順位差" columnKey="comparison_gap" sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={onEntrySort} /></th>
                  <th><span>反対材料</span></th>
                  {([
                    ["統合順位", "integrated_rank"],
                    ["統合score", "integrated_score"],
                    ["統合自信度", "integrated_confidence"]
                  ] as Array<[string, EntrySortKey]>).map(([label, key]) => (
                    <th key={key}>
                      <EntrySortHeader label={label} columnKey={key} sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={onEntrySort} />
                    </th>
                  ))}
                  <th><span>統合根拠</span></th>
                  {([
                    ["旧AI順位", "ai_rank"],
                    ["旧AI判断", "ai_action"],
                    ["旧AI補正", "ai_adjust_score"],
                    ["最終順位", "final_rank"],
                    ["馬券役割", "final_bet_role"],
                    ["評価理由", "reason"]
                  ] as Array<[string, EntrySortKey]>).map(([label, key]) => (
                    <th key={key}>
                      <EntrySortHeader label={label} columnKey={key} sortKey={entrySort.key} sortDirection={entrySort.direction} onSort={onEntrySort} />
                    </th>
                  ))}
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
                          <span className="riskBadge risk" title={prediction.risk_reason || ""}>あり</span>
                        ) : (
                          <span className="riskBadge safe">なし</span>
                        )}
                      </td>
                      <td>{independent?.ai_rank ?? ""}</td>
                      <td>{independent ? formatPercent(independent.confidence * 100) : ""}</td>
                      <td className="reasonCell">
                        {independent ? (
                          <span title={[...independent.positive_factors, ...independent.negative_factors, ...independent.uncertainties].join(" / ")}>{independent.rationale}</span>
                        ) : ""}
                      </td>
                      <td>{comparison?.rank_gap ?? ""}</td>
                      <td>
                        {comparison ? (
                          <span className={comparison.material_opposition ? "riskBadge risk" : "riskBadge safe"} title={comparison.counterpoints.join(" / ") || comparison.uncertainty}>
                            {comparison.material_opposition ? "あり" : "なし"}
                          </span>
                        ) : ""}
                      </td>
                      <td>{integrated?.integrated_rank ?? ""}</td>
                      <td>{formatNumber(integrated?.integrated_score, 2)}</td>
                      <td>{integrated ? formatPercent(integrated.confidence * 100) : ""}</td>
                      <td className="reasonCell">
                        {integrated ? (
                          <span title={integrated.risk_summary}>{integrated.decision_basis}: {integrated.reasons.join(" / ")}</span>
                        ) : ""}
                      </td>
                      <td>{ai?.ai_rank ?? ""}</td>
                      <td>{ai ? <span className={`aiActionBadge ${ai.ai_action}`}>{ai.ai_action}</span> : ""}</td>
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
    </>
  );
}
