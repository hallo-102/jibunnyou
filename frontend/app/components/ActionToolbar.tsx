import {
  AlertTriangle,
  BadgeCent,
  CheckCircle2,
  ListRestart,
  Play,
  RefreshCw,
  Search
} from "lucide-react";

export type RaceWorkbook = {
  file_name: string;
  race_date: string;
  size_bytes: number;
  modified_at: string;
  is_imported: boolean;
};

type ActionToolbarProps = {
  canGenerateBets: boolean;
  isBusy: boolean;
  onGenerateBets: () => void;
  onRunJob: (jobType: string) => void;
  onSearchTextChange: (value: string) => void;
  onWorkbookSelection: (fileName: string) => void;
  predictionJobActive: boolean;
  raceWorkbooks: RaceWorkbook[];
  searchText: string;
  selectedDate: string;
  selectedRaceId: string;
  selectedWorkbookFile: string;
  workbookSelectionMessage: string;
};

export default function ActionToolbar({
  canGenerateBets,
  isBusy,
  onGenerateBets,
  onRunJob,
  onSearchTextChange,
  onWorkbookSelection,
  predictionJobActive,
  raceWorkbooks,
  searchText,
  selectedDate,
  selectedRaceId,
  selectedWorkbookFile,
  workbookSelectionMessage
}: ActionToolbarProps) {
  return (
    <section className="toolbar">
      <label className="field workbookField">
        <span>開催日・Excel</span>
        <select
          aria-label="開催日Excelファイル"
          disabled={isBusy}
          value={selectedWorkbookFile}
          onChange={(event) => onWorkbookSelection(event.target.value)}
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
          aria-label="レース検索"
          value={searchText}
          onChange={(event) => onSearchTextChange(event.target.value)}
          placeholder="レース検索"
        />
      </label>
      <button disabled={isBusy} onClick={() => onRunJob("collection.race_info")} type="button">
        <Play size={16} aria-hidden="true" />
        レース取込
      </button>
      <button disabled={isBusy} onClick={() => onRunJob("collection.odds")} type="button">
        <Play size={16} aria-hidden="true" />
        オッズ取込
      </button>
      <button disabled={isBusy} onClick={() => onRunJob("prediction.feature_generation")} type="button">
        <ListRestart size={16} aria-hidden="true" />
        特徴量反映
      </button>
      <button
        disabled={isBusy || predictionJobActive || !selectedDate || !selectedRaceId}
        onClick={() => onRunJob("prediction.python")}
        title={!selectedDate || !selectedRaceId
          ? "開催日とレースを選択してください"
          : predictionJobActive
            ? "Python予想を実行中です"
            : "選択レースのPython予想を実行"}
        type="button"
      >
        {predictionJobActive ? <RefreshCw size={16} aria-hidden="true" /> : <Play size={16} aria-hidden="true" />}
        {predictionJobActive ? "Python予想 実行中" : "Python予想"}
      </button>
      <button
        disabled={isBusy || !canGenerateBets}
        onClick={onGenerateBets}
        title="下の買い目設定を使って候補を保存します。自動投票は行いません"
        type="button"
      >
        <BadgeCent size={16} aria-hidden="true" />
        買い目候補を保存
      </button>
      <button disabled={isBusy} onClick={() => onRunJob("maintenance.data_quality_check")} type="button">
        <AlertTriangle size={16} aria-hidden="true" />
        品質チェック
      </button>
      <button disabled={isBusy} onClick={() => onRunJob("collection.results")} type="button">
        <Play size={16} aria-hidden="true" />
        結果取得
      </button>
      <button disabled={isBusy} onClick={() => onRunJob("result.settlement")} type="button">
        <CheckCircle2 size={16} aria-hidden="true" />
        精算
      </button>
    </section>
  );
}
