export type IndependentAiRunner = {
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

export type IndependentAiOutput = {
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

export type IndependentAiAnalysis = {
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

export type HorseComparison = {
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

export type ComparisonOutput = {
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

export type IntegratedHorsePrediction = {
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

export type IntegrationOutput = {
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

export type AiIntegrationAnalysis = {
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

export type AiEvaluation = {
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

export type AiBetStrategy = {
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

type AiDiff = {
  upgrades: AiEvaluation[];
  downgrades: AiEvaluation[];
  dangerous: AiEvaluation[];
  values: AiEvaluation[];
};

type AiAnalysisPanelsProps = {
  aiBetStrategy: AiBetStrategy | null;
  aiDiff: AiDiff;
  aiEvaluations: AiEvaluation[];
  formatAiJobFailure: (message?: string | null) => string;
  formatNumber: (value?: number | null, digits?: number) => string;
  independentAnalysis: IndependentAiAnalysis | null;
  integrationAnalysis: AiIntegrationAnalysis | null;
};

export default function AiAnalysisPanels({
  aiBetStrategy,
  aiDiff,
  aiEvaluations,
  formatAiJobFailure,
  formatNumber,
  independentAnalysis,
  integrationAnalysis
}: AiAnalysisPanelsProps) {
  return (
    <>
      <section data-route-section="analysis" id="ai-analysis">
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
            <p>{formatAiJobFailure(independentAnalysis.error_message)}</p>
          </div>
        ) : (
          <div className="emptyState">選択レースの独立AI分析はまだありません</div>
        )}
      </section>

      <section data-route-section="analysis">
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
                <p>{integrationAnalysis.error_message
                  ? formatAiJobFailure(integrationAnalysis.error_message)
                  : "比較結果のみ固定保存されています"}</p>
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
            <p>{formatAiJobFailure(integrationAnalysis.error_message)}</p>
          </div>
        ) : (
          <div className="emptyState">固定済み独立AI結果とPython予想を比較すると表示されます</div>
        )}
      </section>

      <section data-route-section="analysis">
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
    </>
  );
}
