from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from statistics import mean
from typing import Protocol

from app.core.config import Settings
from app.schemas.ai_independent import (
    AiRaceAssessment,
    IndependentAnalysisInput,
    IndependentAnalysisResponse,
    IndependentHorseEvaluation,
)
from app.schemas.ai_integration import (
    ComparisonInput,
    ComparisonResponse,
    HorseComparison,
    IntegratedHorsePrediction,
    IntegrationInput,
    IntegrationResponse,
    OppositionSummary,
)


INDEPENDENT_PROMPT_VERSION = "ai-independent-v1.0.0"
INDEPENDENT_DEVELOPER_PROMPT = """あなたは競馬予想アプリの独立分析担当です。
入力として渡されたレース情報だけを使い、Python予想から独立した評価を作成してください。
入力にない事実を作らず、情報不足はunknownsとuncertaintiesへ明記してください。
順位を付ける根拠が足りない馬はai_rankをnullにできます。
馬番と馬名は入力どおりに全頭を一度ずつ返し、自動購入や購入指示は行わないでください。
内部の長い思考過程ではなく、ユーザーが検証できる短い根拠だけを返してください。
指定された構造化出力スキーマへ厳密に従ってください。"""
INDEPENDENT_PROMPT_SHA256 = sha256(INDEPENDENT_DEVELOPER_PROMPT.encode("utf-8")).hexdigest()
COMPARISON_PROMPT_VERSION = "ai-comparison-v1.0.0"
COMPARISON_DEVELOPER_PROMPT = """あなたは競馬予想アプリの比較分析担当です。
入力には、すでに固定された独立AI結果と、その固定後に初めて開示されたPython予想があります。
各馬について一致・不一致と理由の差を整理し、独立結果を後から改変しないでください。
反対材料がない場合は捏造せず、重大な反対材料なしと明記してください。
入力にない事実を追加せず、馬番・馬名・順位は入力どおりに全頭を一度ずつ返してください。
自動購入を指示せず、指定された構造化出力スキーマへ厳密に従ってください。"""
COMPARISON_PROMPT_SHA256 = sha256(COMPARISON_DEVELOPER_PROMPT.encode("utf-8")).hexdigest()
INTEGRATION_PROMPT_VERSION = "ai-integration-v1.1.0"
INTEGRATION_DEVELOPER_PROMPT = """あなたは競馬予想アプリの統合判断担当です。
固定済み独立AI結果、Python予想、検証済み比較結果を使い、全頭の統合順位案を作成してください。
0〜100の統合scoreを作り、統合順位の順に同点を許さず降順になるようにしてください。
通常のPython順位変更は2順位以内、例外でも4順位以内とし、3順位以上の変更はmanual_review_requiredをtrueにしてください。
どちらを優先したか、根拠、不確実性、リスクを短く検証可能な形で示してください。
入力にない事実を追加せず、自動購入を指示せず、指定された構造化出力スキーマへ厳密に従ってください。"""
INTEGRATION_PROMPT_SHA256 = sha256(INTEGRATION_DEVELOPER_PROMPT.encode("utf-8")).hexdigest()


class AiProviderUnavailable(RuntimeError):
    """Raised when the configured AI provider cannot be used safely."""


class AiProviderResponseError(RuntimeError):
    """Raised when a provider does not return a parsed structured response."""


@dataclass(frozen=True)
class AiProviderResult:
    output: IndependentAnalysisResponse
    provider_response_id: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


@dataclass(frozen=True)
class AiComparisonProviderResult:
    output: ComparisonResponse
    provider_response_id: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


@dataclass(frozen=True)
class AiIntegrationProviderResult:
    output: IntegrationResponse
    provider_response_id: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class IndependentAiProvider(Protocol):
    model_name: str

    def analyze(self, input_snapshot: IndependentAnalysisInput) -> AiProviderResult:
        """Return one schema-validated independent analysis."""


class AiPipelineProvider(IndependentAiProvider, Protocol):
    def compare(self, input_snapshot: ComparisonInput) -> AiComparisonProviderResult:
        """Compare a locked independent result with a Python prediction."""

    def integrate(self, input_snapshot: IntegrationInput) -> AiIntegrationProviderResult:
        """Return one guarded integration proposal."""


class DeterministicMockAiProvider:
    """Provide an input-only deterministic provider for automated tests."""

    model_name = "deterministic-independent-mock-v1"

    def analyze(self, input_snapshot: IndependentAnalysisInput) -> AiProviderResult:
        has_evidence = any(
            runner.past_performances or runner.market.popularity is not None
            for runner in input_snapshot.runners
        )
        ordered = sorted(input_snapshot.runners, key=self._ranking_key)
        rank_by_horse = {
            runner.horse_no: rank
            for rank, runner in enumerate(ordered, start=1)
        } if has_evidence else {}

        evaluations: list[IndependentHorseEvaluation] = []
        for runner in input_snapshot.runners:
            positives: list[str] = []
            negatives: list[str] = []
            uncertainties: list[str] = []
            finishes = [
                item.finish_position
                for item in runner.past_performances
                if item.finish_position is not None
            ]
            if finishes:
                positives.append(f"過去走{len(finishes)}件の平均着順は{mean(finishes):.1f}着")
            else:
                uncertainties.append("日付を確認できる過去走データがありません")
            if runner.market.popularity is not None:
                positives.append(f"当日市場評価は{runner.market.popularity}番人気")
            else:
                uncertainties.append("人気データがありません")
            if runner.qualitative.training_summary is None:
                uncertainties.append("調教要約がありません")

            confidence = min(0.85, 0.25 + len(finishes) * 0.1 + (0.1 if runner.market.popularity else 0))
            rank = rank_by_horse.get(runner.horse_no)
            evaluations.append(
                IndependentHorseEvaluation(
                    horse_no=runner.horse_no,
                    horse_name=runner.horse_name,
                    ai_rank=rank,
                    rank_range_low=rank,
                    rank_range_high=rank,
                    confidence=round(confidence if has_evidence else 0.1, 2),
                    risk_level="medium" if uncertainties else "low",
                    positive_factors=positives,
                    negative_factors=negatives,
                    uncertainties=uncertainties,
                    rationale=(
                        "入力された過去走と市場情報だけを用いた独立評価です"
                        if has_evidence
                        else "順位判断に必要な基礎情報が不足しています"
                    ),
                )
            )

        output = IndependentAnalysisResponse(
            race_id=input_snapshot.race.race_id,
            decision_status="completed" if has_evidence else "insufficient_data",
            data_confidence="medium" if has_evidence else "low",
            manual_review_required=not has_evidence or input_snapshot.data_quality.status != "GREEN",
            race_assessment=AiRaceAssessment(
                pace_assessment="uncertain",
                track_bias_assessment="入力情報だけでは馬場バイアスを確定できません",
                main_risks=["展開情報の不足"],
                summary="Python予想情報を使わず、取得済み基礎データだけで評価しました",
            ),
            runners=evaluations,
            unknowns=["展開と馬場バイアスは追加情報が必要です"],
            final_comment="この結果は独立分析であり、Python予想との比較前の固定結果です",
        )
        return AiProviderResult(output=output, provider_response_id="mock-response")

    def compare(self, input_snapshot: ComparisonInput) -> AiComparisonProviderResult:
        independent_by_horse = {
            runner.horse_no: runner for runner in input_snapshot.independent_result.runners
        }
        horses: list[HorseComparison] = []
        opposition_horse_nos: list[int] = []
        absolute_gaps: list[int] = []
        for python_result in input_snapshot.python_results:
            independent = independent_by_horse[python_result.horse_no]
            rank_gap = (
                python_result.python_rank - independent.ai_rank
                if independent.ai_rank is not None
                else None
            )
            absolute_gap = abs(rank_gap) if rank_gap is not None else 99
            if rank_gap is not None:
                absolute_gaps.append(absolute_gap)
            material = rank_gap is not None and absolute_gap >= 3
            if material:
                opposition_horse_nos.append(python_result.horse_no)
            agreement = (
                "unknown"
                if rank_gap is None
                else "exact"
                if absolute_gap == 0
                else "small_difference"
                if absolute_gap <= 2
                else "material_difference"
            )
            horses.append(
                HorseComparison(
                    horse_no=python_result.horse_no,
                    horse_name=python_result.horse_name,
                    python_rank=python_result.python_rank,
                    ai_rank=independent.ai_rank,
                    rank_gap=rank_gap,
                    agreement_level=agreement,
                    python_view=python_result.evaluation_reason or "Python順位とscoreによる評価です",
                    ai_view=independent.rationale,
                    counterpoints=(
                        [f"独立AI順位との差は{absolute_gap}順位です"]
                        if material
                        else []
                    ),
                    material_opposition=material,
                    uncertainty=(
                        "独立AI順位が不明です"
                        if independent.ai_rank is None
                        else "入力済み両評価の順位差だけを比較しています"
                    ),
                )
            )

        maximum_gap = max(absolute_gaps, default=99)
        alignment = "high" if maximum_gap <= 1 else "medium" if maximum_gap <= 2 else "low"
        opposition = OppositionSummary(
            has_material_opposition=bool(opposition_horse_nos),
            horse_nos=opposition_horse_nos,
            summary=(
                f"{len(opposition_horse_nos)}頭に3順位以上の差があります"
                if opposition_horse_nos
                else "重大な反対材料なし"
            ),
        )
        output = ComparisonResponse(
            race_id=input_snapshot.independent_result.race_id,
            independent_analysis_id=input_snapshot.independent_analysis_id,
            python_prediction_run_id=input_snapshot.python_prediction_run_id,
            overall_alignment=alignment,
            data_confidence=input_snapshot.independent_result.data_confidence,
            manual_review_required=bool(opposition_horse_nos),
            horses=horses,
            opposition=opposition,
            summary="固定済み独立AI順位とPython順位を馬ごとに比較しました",
        )
        return AiComparisonProviderResult(output=output, provider_response_id="mock-comparison")

    def integrate(self, input_snapshot: IntegrationInput) -> AiIntegrationProviderResult:
        independent_by_horse = {
            runner.horse_no: runner
            for runner in input_snapshot.comparison_input.independent_result.runners
        }
        comparison_by_horse = {
            horse.horse_no: horse for horse in input_snapshot.comparison_result.horses
        }
        horses: list[IntegratedHorsePrediction] = []
        for python_result in input_snapshot.comparison_input.python_results:
            independent = independent_by_horse[python_result.horse_no]
            comparison = comparison_by_horse[python_result.horse_no]
            horses.append(
                IntegratedHorsePrediction(
                    horse_no=python_result.horse_no,
                    horse_name=python_result.horse_name,
                    python_rank=python_result.python_rank,
                    ai_rank=independent.ai_rank,
                    # deterministic mockは安全側でPython順位を維持し、比較根拠だけを検証する。
                    integrated_rank=python_result.python_rank,
                    integrated_score=round(
                        100.0
                        - (python_result.python_rank - 1)
                        * (99.0 / max(1, len(input_snapshot.comparison_input.python_results) - 1)),
                        4,
                    ),
                    decision_basis=(
                        "balanced" if independent.ai_rank is not None else "python_priority"
                    ),
                    confidence=min(0.9, max(0.2, independent.confidence)),
                    uncertainty_level=(
                        "high"
                        if comparison.agreement_level == "unknown"
                        else "medium"
                        if comparison.material_opposition
                        else "low"
                    ),
                    reasons=[comparison.python_view, comparison.ai_view],
                    risk_summary=comparison.uncertainty,
                )
            )
        output = IntegrationResponse(
            race_id=input_snapshot.comparison_result.race_id,
            independent_analysis_id=input_snapshot.comparison_result.independent_analysis_id,
            python_prediction_run_id=input_snapshot.comparison_result.python_prediction_run_id,
            integration_strategy="balanced",
            data_confidence=input_snapshot.comparison_result.data_confidence,
            manual_review_required=input_snapshot.comparison_result.manual_review_required,
            horses=horses,
            key_disagreements=[input_snapshot.comparison_result.opposition.summary],
            uncertainties=["mockでは安全側でPython順位を維持しています"],
            final_comment="独立AIとPythonの両根拠を保持した統合判断です",
        )
        return AiIntegrationProviderResult(output=output, provider_response_id="mock-integration")

    @staticmethod
    def _ranking_key(runner) -> tuple[float, int, int]:
        finishes = [
            item.finish_position
            for item in runner.past_performances
            if item.finish_position is not None
        ]
        average_finish = mean(finishes) if finishes else 999.0
        popularity = runner.market.popularity or 999
        return average_finish, popularity, runner.horse_no


def create_independent_ai_provider(settings: Settings) -> IndependentAiProvider:
    """Build only an explicitly configured provider; never silently fabricate AI output."""

    if settings.ai_provider == "mock":
        if settings.environment not in {"test", "development", "local"}:
            raise AiProviderUnavailable("mock AI providerは本番環境では使用できません")
        return DeterministicMockAiProvider()
    raise AiProviderUnavailable(
        "APIによるAI予想は廃止されました。ChatGPT手動予想を使用してください"
    )
