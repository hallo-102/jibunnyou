import fs from "node:fs/promises";
import path from "node:path";
import { Workbook, SpreadsheetFile } from "@oai/artifact-tool";

const projectRoot = path.resolve("../..");
const evaluation = JSON.parse(
  await fs.readFile(path.join(projectRoot, "reports/evaluation_20260728_211611.json"), "utf8"),
);
const decision = JSON.parse(
  await fs.readFile(
    path.join(projectRoot, "data/output/bet_rule_validation/five_block_bet_rule_decision.json"),
    "utf8",
  ),
);
const candidateCsv = await fs.readFile(
  path.join(projectRoot, "data/output/bet_rule_validation/five_block_bet_rule_candidates.csv"),
  "utf8",
);

const headerFormat = {
  fill: "#1F4E78",
  font: { bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  verticalAlignment: "center",
};
const sectionFormat = {
  fill: "#D9EAF7",
  font: { bold: true, color: "#17365D" },
};

const baseline = Workbook.create();
const summarySheet = baseline.worksheets.add("Summary");
summarySheet.showGridLines = false;
summarySheet.getRange("A1:B1").values = [["時系列修正前ベースライン", ""]];
summarySheet.getRange("A1:B1").format = {
  fill: "#17365D",
  font: { bold: true, color: "#FFFFFF", size: 16 },
  horizontalAlignment: "center",
};
summarySheet.getRange("A3:B3").values = [["指標", "値"]];
summarySheet.getRange("A3:B3").format = headerFormat;
const summaryRows = Object.entries(evaluation.summary);
summarySheet.getRangeByIndexes(3, 0, summaryRows.length, 2).values = summaryRows;
summarySheet.getRange("B4:B7").format.numberFormat = "#,##0";
summarySheet.getRange("B8:B13").format.numberFormat = "0.00%";
summarySheet.getRange("B14:B17").format.numberFormat = "#,##0";
summarySheet.getRange("B18:B19").format.numberFormat = "0.0000";
summarySheet.getRange("B20:B21").format.numberFormat = "yyyy-mm-dd";
summarySheet.getRange("B20:B21").values = [
  [new Date(`${evaluation.summary.best_day.slice(0, 4)}-${evaluation.summary.best_day.slice(4, 6)}-${evaluation.summary.best_day.slice(6, 8)}T00:00:00`)],
  [new Date(`${evaluation.summary.worst_day.slice(0, 4)}-${evaluation.summary.worst_day.slice(4, 6)}-${evaluation.summary.worst_day.slice(6, 8)}T00:00:00`)],
];
summarySheet.getRange("A3:B3").format.borders = {
  preset: "outside",
  style: "medium",
  color: "#17365D",
};
summarySheet.getRange("A3:B25").format.autofitColumns();
summarySheet.getRange("A:A").format.columnWidth = 34;
summarySheet.getRange("B:B").format.columnWidth = 18;
summarySheet.freezePanes.freezeRows(3);

const settingsSheet = baseline.worksheets.add("Settings");
settingsSheet.showGridLines = false;
settingsSheet.getRange("A1:B1").values = [["設定", "修正前値"]];
settingsSheet.getRange("A1:B1").format = headerFormat;
settingsSheet.getRange("A2:B7").values = [
  ["best_feature_weights", "yosou_py/best_feature_weights_20260701.py"],
  ["SCORING_MODEL_VERSION", "five_block"],
  ["TRAIN期間", "20250524～20260228"],
  ["TEST期間（修正前）", "20260301～利用可能最新日"],
  ["評価作成日時", evaluation.metadata.created_at],
  ["予想ファイル数", evaluation.metadata.prediction_files.length],
];
settingsSheet.getRange("A1:B7").format.autofitColumns();
settingsSheet.getRange("A:A").format.columnWidth = 30;
settingsSheet.getRange("B:B").format.columnWidth = 55;

const monthlySheet = baseline.worksheets.add("Monthly");
monthlySheet.showGridLines = false;
const monthlyRows = evaluation.monthly;
const monthlyHeaders = monthlyRows.length ? Object.keys(monthlyRows[0]) : [];
if (monthlyHeaders.length) {
  monthlySheet.getRangeByIndexes(0, 0, 1, monthlyHeaders.length).values = [monthlyHeaders];
  monthlySheet.getRangeByIndexes(0, 0, 1, monthlyHeaders.length).format = headerFormat;
  monthlySheet.getRangeByIndexes(1, 0, monthlyRows.length, monthlyHeaders.length).values =
    monthlyRows.map((row) => monthlyHeaders.map((header) => row[header] ?? null));
  monthlySheet.getRangeByIndexes(0, 0, monthlyRows.length + 1, monthlyHeaders.length)
    .format.autofitColumns();
  monthlySheet.freezePanes.freezeRows(1);
}

const baselineOut = path.join(
  projectRoot,
  "data/output/validation/baseline_before_timeseries_fix.xlsx",
);
await fs.mkdir(path.dirname(baselineOut), { recursive: true });
const baselineBlob = await SpreadsheetFile.exportXlsx(baseline);
await baselineBlob.save(baselineOut);
const baselinePreview = await baseline.render({
  sheetName: "Summary",
  range: "A1:B25",
  scale: 1.5,
  format: "png",
});
await fs.writeFile(
  path.join(projectRoot, ".codex_tmp/keiba_timeseries_20260728/baseline_preview.png"),
  new Uint8Array(await baselinePreview.arrayBuffer()),
);
for (const [sheetName, range, fileName] of [
  ["Settings", "A1:B7", "settings_preview.png"],
  ["Monthly", "A1:Q12", "monthly_preview.png"],
]) {
  const preview = await baseline.render({ sheetName, range, scale: 1.2, format: "png" });
  await fs.writeFile(
    path.join(projectRoot, `.codex_tmp/keiba_timeseries_20260728/${fileName}`),
    new Uint8Array(await preview.arrayBuffer()),
  );
}
console.log(
  (
    await baseline.inspect({
      kind: "table",
      range: "Summary!A1:B25",
      include: "values,formulas",
      tableMaxRows: 25,
      tableMaxCols: 2,
    })
  ).ndjson,
);

const candidates = await Workbook.fromCSV(candidateCsv, { sheetName: "Candidates" });
const candidateSheet = candidates.worksheets.getItem("Candidates");
candidateSheet.showGridLines = false;
const candidateUsed = candidateSheet.getUsedRange();
candidateSheet.getRangeByIndexes(0, 0, 1, candidateUsed.columnCount).format = headerFormat;
candidateUsed.format.autofitColumns();
candidateSheet.getRange("A:A").format.columnWidth = 14;
candidateSheet.freezePanes.freezeRows(1);

const decisionSheet = candidates.worksheets.add("Decision");
decisionSheet.showGridLines = false;
decisionSheet.getRange("A1:B1").values = [["判定項目", "内容"]];
decisionSheet.getRange("A1:B1").format = headerFormat;
decisionSheet.getRange("A2:B9").values = [
  ["decision", decision.decision],
  ["reason", decision.reason.join(" / ")],
  ["candidate_count", decision.candidate_count],
  ["eligible_count", decision.eligible_count],
  ["TRAIN", `${decision.periods.train.start}～${decision.periods.train.end}`],
  ["VALID", `${decision.periods.valid.start}～${decision.periods.valid.end}`],
  ["TEST", `${decision.periods.test.start}～latest（不採用のため未評価）`],
  ["買い目", "3連複3点・馬連2点・ワイド2点、合計700円"],
];
decisionSheet.getRange("A2:B2").format = sectionFormat;
decisionSheet.getRange("A1:B9").format.autofitColumns();
decisionSheet.getRange("A:A").format.columnWidth = 24;
decisionSheet.getRange("B:B").format.columnWidth = 70;
decisionSheet.getRange("B2:B9").format.wrapText = true;

const candidatesOut = path.join(
  projectRoot,
  "data/output/bet_rule_validation/five_block_bet_rule_candidates.xlsx",
);
const candidatesBlob = await SpreadsheetFile.exportXlsx(candidates);
await candidatesBlob.save(candidatesOut);
const candidatesPreview = await candidates.render({
  sheetName: "Decision",
  range: "A1:B9",
  scale: 1.5,
  format: "png",
});
await fs.writeFile(
  path.join(projectRoot, ".codex_tmp/keiba_timeseries_20260728/candidates_preview.png"),
  new Uint8Array(await candidatesPreview.arrayBuffer()),
);
const candidateTablePreview = await candidates.render({
  sheetName: "Candidates",
  range: "A1:U20",
  scale: 0.8,
  format: "png",
});
await fs.writeFile(
  path.join(projectRoot, ".codex_tmp/keiba_timeseries_20260728/candidate_table_preview.png"),
  new Uint8Array(await candidateTablePreview.arrayBuffer()),
);
console.log(
  (
    await candidates.inspect({
      kind: "match",
      searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
      options: { useRegex: true, maxResults: 100 },
      summary: "formula error scan",
    })
  ).ndjson,
);
