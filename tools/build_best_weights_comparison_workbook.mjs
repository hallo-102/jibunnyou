import fs from "node:fs/promises";
import path from "node:path";

import {
  SpreadsheetFile,
  Workbook,
} from "file:///C:/Users/okino/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/@oai/artifact-tool/dist/artifact_tool.mjs";

const projectRoot = process.cwd();
const outputDir = path.join(
  projectRoot,
  "outputs",
  "best_weights_profit_comparison_20260817",
);
const outputPath = path.join(
  outputDir,
  "best_weights_profit_comparison_20260817.xlsx",
);

const sourceSheets = [
  ["予測指標", "prediction_summary.csv"],
  ["差の95%CI", "prediction_difference_ci.csv"],
  ["平均着順共通", "paired_average_finish.csv"],
  ["収支比較", "bet_summary.csv"],
  ["条件別収支", "condition_profit.csv"],
  ["予想1位人気別", "rank1_popularity_segment.csv"],
  ["月別・日別", "walk_forward_daily.csv"],
  ["TEST再利用", "test_reuse_audit.csv"],
  ["重み監査", "weight_audit.csv"],
  ["事前市場監査", "market_snapshot_audit.csv"],
];

const [firstSheetName, firstFileName] = sourceSheets[0];
const firstCsvText = await fs.readFile(
  path.join(outputDir, firstFileName),
  "utf8",
);
const workbook = await Workbook.fromCSV(firstCsvText, {
  sheetName: firstSheetName,
});
for (const [sheetName, fileName] of sourceSheets.slice(1)) {
  const csvText = await fs.readFile(path.join(outputDir, fileName), "utf8");
  await workbook.fromCSV(csvText, { sheetName });
}
const summary = workbook.worksheets.add("比較サマリー");
const audit = workbook.worksheets.add("データフロー監査");

const auditJson = JSON.parse(
  await fs.readFile(path.join(outputDir, "comparison_audit.json"), "utf8"),
);

const navy = "#1F4E78";
const blue = "#D9EAF7";
const paleBlue = "#EAF3F8";
const green = "#E2F0D9";
const red = "#FCE4D6";
const orange = "#FFF2CC";
const gray = "#E7E6E6";
const white = "#FFFFFF";

function setTitle(sheet, range, text) {
  const titleRange = sheet.getRange(range);
  titleRange.merge();
  titleRange.values = [[text]];
  titleRange.format = {
    fill: navy,
    font: { bold: true, color: white, size: 16 },
    verticalAlignment: "center",
  };
  titleRange.format.rowHeight = 30;
}

function styleHeader(range) {
  range.format = {
    fill: navy,
    font: { bold: true, color: white },
    horizontalAlignment: "center",
    verticalAlignment: "center",
    wrapText: true,
  };
  range.format.rowHeight = 28;
}

function styleSection(range) {
  range.format = {
    fill: blue,
    font: { bold: true, color: "#17365D" },
    verticalAlignment: "center",
  };
}

function applySourceSheetStyle(sheetName) {
  const sheet = workbook.worksheets.getItem(sheetName);
  const used = sheet.getUsedRange();
  sheet.showGridLines = false;
  sheet.freezePanes.freezeRows(1);
  used.format.font = { name: "Aptos", size: 10 };
  used.format.verticalAlignment = "center";
  used.format.autofitColumns();
  used.getRow(0).format = {
    fill: navy,
    font: { bold: true, color: white, size: 10 },
    horizontalAlignment: "center",
    verticalAlignment: "center",
    wrapText: true,
  };
  used.getRow(0).format.rowHeight = 34;
}

summary.showGridLines = false;
summary.freezePanes.freezeRows(2);
setTitle(summary, "A1:H1", "旧best 20260730 vs 新best 20260817 公平比較");

summary.getRange("A3:B3").values = [["採用判断", "前向き検証まで保留"]];
summary.getRange("A3").format = { fill: gray, font: { bold: true } };
summary.getRange("B3").format = {
  fill: orange,
  font: { bold: true, color: "#9C6500" },
};
summary.getRange("A4:B7").values = [
  ["現在の本番runner", "best_feature_weights_20260817.py"],
  ["予測精度の優位", "旧best（全体・TEST平均着順、全体TOP5/複勝率）"],
  ["回収率の優位", "旧best（全体単勝・複勝、現行3連複の事前市場4日）"],
  ["実運用の判断", "未使用HOLDOUTなし。現設定を変更せず前向き固定期間で再判定"],
];
summary.getRange("A4:A7").format = { fill: paleBlue, font: { bold: true } };
summary.getRange("B4:B7").format.wrapText = true;

summary.getRange("A9:H9").merge();
summary.getRange("A9").values = [["比較条件・監査結果"]];
styleSection(summary.getRange("A9:H9"));
summary.getRange("A10:D15").values = [
  ["入力ファイル", 130, "結果Excelシート", 134],
  ["評価レース", 3697, "評価馬", 50963],
  ["TRAIN", 2076, "VALID", 550],
  ["TEST", 1071, "TEST欠損結果", 17],
  ["明示TEST利用回数", 4, "追加の集約利用可能性", 1],
  ["未使用HOLDOUT", "なし", "未来情報混入", "なし"],
];
summary.getRange("A10:A15").format = { fill: paleBlue, font: { bold: true } };
summary.getRange("C10:C15").format = { fill: paleBlue, font: { bold: true } };
summary.getRange("E10:H15").merge(true);
summary.getRange("E10:H15").values = [
  ["同一の特徴量DataFrameを両モデルへ渡し、差し替えたのは実効重みだけ。"],
  ["結果・払戻・確定人気はスコア／順位確定後に評価用として結合。"],
  ["現在入力に確定オッズ列なし。事前OZZUは買い目フィルタだけに使用。"],
  ["DLは結果Excelを読むがblend/bonus=0、両重みのdl_rank_score=0。順位影響なし。"],
  ["TESTは複数の採用判断で再利用済み。完全な未使用データとは扱わない。"],
  ["20260822以降を固定し、最低300レースまたは8開催日まで条件を凍結。"],
];
summary.getRange("E10:H15").format = {
  fill: orange,
  wrapText: true,
  verticalAlignment: "center",
};

summary.getRange("A17:H17").merge();
summary.getRange("A17").values = [["TEST絶対指標（同一1071レース・14552頭）"]];
styleSection(summary.getRange("A17:H17"));
summary.getRange("A18:H18").values = [[
  "モデル",
  "レース数",
  "馬数",
  "TOP5点率",
  "TOP3完全捕捉率",
  "予想1位勝率",
  "予想1位複勝率",
  "予想1位平均着順（共通1064）",
]];
styleHeader(summary.getRange("A18:H18"));
summary.getRange("A19:A21").values = [["旧best"], ["新best"], ["新-旧"]];
summary.getRange("B19:H19").formulas = [[
  "='予測指標'!C4",
  "='予測指標'!D4",
  "='予測指標'!G4",
  "='予測指標'!I4",
  "='予測指標'!K4",
  "='予測指標'!M4",
  "='平均着順共通'!C4",
]];
summary.getRange("B20:H20").formulas = [[
  "='予測指標'!C12",
  "='予測指標'!D12",
  "='予測指標'!G12",
  "='予測指標'!I12",
  "='予測指標'!K12",
  "='予測指標'!M12",
  "='平均着順共通'!D4",
]];
summary.getRange("B21:H21").formulas = [[
  "=B20-B19",
  "=C20-C19",
  "=D20-D19",
  "=E20-E19",
  "=F20-F19",
  "=G20-G19",
  "=H20-H19",
]];
summary.getRange("A21:H21").format = { fill: orange, font: { bold: true } };
summary.getRange("D19:G21").format.numberFormat = "0.00%";
summary.getRange("H19:H21").format.numberFormat = "0.000";

summary.getRange("A23:F23").merge();
summary.getRange("A23").values = [["指定されたTEST比率の件数内訳"]];
styleSection(summary.getRange("A23:F23"));
summary.getRange("A24:F24").values = [[
  "指標",
  "旧best",
  "新best",
  "差",
  "新/旧比",
  "分母",
]];
styleHeader(summary.getRange("A24:F24"));
summary.getRange("A25:A27").values = [["予想1位複勝"], ["TOP5点"], ["TOP3完全捕捉"]];
summary.getRange("B25:F27").formulas = [
  ["='予測指標'!L4", "='予測指標'!L12", "=C25-B25", "=C25/B25", "='予測指標'!C4"],
  ["='予測指標'!E4", "='予測指標'!E12", "=C26-B26", "=C26/B26", "='予測指標'!F4"],
  ["='予測指標'!H4", "='予測指標'!H12", "=C27-B27", "=C27/B27", "='予測指標'!C4"],
];
summary.getRange("E25:E27").format.numberFormat = "0.0000";

summary.getRange("A29:H29").merge();
summary.getRange("A29").values = [["TEST買い目別収支（100円/点・現行条件をモデルごとに適用）"]];
styleSection(summary.getRange("A29:H29"));
summary.getRange("A30:H30").values = [[
  "戦略",
  "モデル",
  "購入件数",
  "的中件数",
  "総購入額",
  "総払戻額",
  "収支",
  "回収率",
]];
styleHeader(summary.getRange("A30:H30"));
const strategies = [
  "予想1位単勝",
  "予想1位複勝",
  "Sランク_3連複3点",
  "Aランク_3連複3点",
  "Bランク_3連複3点",
  "現行回収率重視_3連複3点",
];
const summaryModels = ["旧best_20260730", "新best_20260817"];
let summaryRow = 31;
for (const strategy of strategies) {
  for (const model of summaryModels) {
    summary.getRange(`A${summaryRow}:B${summaryRow}`).values = [[strategy, model]];
    const criteria = `'収支比較'!$A$2:$A$49,$B${summaryRow},'収支比較'!$B$2:$B$49,$A${summaryRow},'収支比較'!$C$2:$C$49,"TEST"`;
    summary.getRange(`C${summaryRow}:H${summaryRow}`).formulas = [[
      `=SUMIFS('収支比較'!$D$2:$D$49,${criteria})`,
      `=SUMIFS('収支比較'!$F$2:$F$49,${criteria})`,
      `=SUMIFS('収支比較'!$H$2:$H$49,${criteria})`,
      `=SUMIFS('収支比較'!$I$2:$I$49,${criteria})`,
      `=SUMIFS('収支比較'!$J$2:$J$49,${criteria})`,
      `=SUMIFS('収支比較'!$K$2:$K$49,${criteria})`,
    ]];
    summaryRow += 1;
  }
}
summary.getRange("E31:G42").format.numberFormat = "¥#,##0;[Red]-¥#,##0";
summary.getRange("H31:H42").format.numberFormat = "0.00%";
summary.getRange("A31:H42").conditionalFormats.addCustom("=$G31>0", {
  fill: green,
});
summary.getRange("A31:H42").conditionalFormats.addCustom("=$G31<0", {
  fill: red,
});

summary.getRange("J17:M17").values = [["月", "TOP5差", "TOP3差", "1位複勝差"]];
styleHeader(summary.getRange("J17:M17"));
summary.getRange("J18:J21").values = [["2026-05"], ["2026-06"], ["2026-07"], ["2026-08"]];
for (let index = 0; index < 4; index += 1) {
  const targetRow = 18 + index;
  const oldRow = 6 + index;
  const newRow = 14 + index;
  summary.getRange(`K${targetRow}:M${targetRow}`).formulas = [[
    `='予測指標'!G${newRow}-'予測指標'!G${oldRow}`,
    `='予測指標'!I${newRow}-'予測指標'!I${oldRow}`,
    `='予測指標'!M${newRow}-'予測指標'!M${oldRow}`,
  ]];
}
summary.getRange("K18:M21").format.numberFormat = "0.00%";
const chart = summary.charts.add("line", summary.getRange("J17:M21"));
chart.title = "TEST月別 新best－旧best";
chart.hasLegend = true;
chart.xAxis = { axisType: "textAxis" };
chart.yAxis = { numberFormatCode: "0.0%" };
chart.setPosition("J2", "Q15");

summary.getRange("A44:H46").merge(true);
summary.getRange("A44:H46").values = [
  ["注1: 現行回収率重視3連複は、予測入力に確定人気を使わず、開催前保存済みOZZUが存在する20260808/09/15/16だけを両モデルへ対称適用。"],
  ["注2: TESTは過去の採用判断に少なくとも4回明示利用され、追加で1回は事前ゲート集約に含まれた可能性がある。独立HOLDOUTではない。"],
  ["注3: モデルの優劣判定では、平均着順は値が小さいほど良い。他の率・回収率は値が大きいほど良い。"],
];
summary.getRange("A44:H46").format = { fill: gray, wrapText: true };

summary.getRange("A1:Q46").format.font = { name: "Aptos", size: 10 };
summary.getRange("A1:Q46").format.verticalAlignment = "center";
summary.getRange("A1:A46").format.columnWidth = 25;
summary.getRange("B1:B46").format.columnWidth = 25;
summary.getRange("C1:H46").format.columnWidth = 15;
summary.getRange("J1:M46").format.columnWidth = 13;
summary.getRange("A44:H46").format.rowHeight = 34;

for (const [sheetName] of sourceSheets) {
  applySourceSheetStyle(sheetName);
}

workbook.worksheets.getItem("予測指標").getRange("G2:Q17").format.numberFormat = "0.0000";
workbook.worksheets.getItem("差の95%CI").getRange("D2:F21").format.numberFormat = "0.0000";
workbook.worksheets.getItem("収支比較").getRange("G2:G49").format.numberFormat = "0.00%";
workbook.worksheets.getItem("収支比較").getRange("H2:J49").format.numberFormat = "¥#,##0;[Red]-¥#,##0";
workbook.worksheets.getItem("収支比較").getRange("K2:K49").format.numberFormat = "0.00%";
workbook.worksheets.getItem("収支比較").getRange("L2:N49").format.numberFormat = "0.00";
workbook.worksheets.getItem("収支比較").getRange("Q2:Q49").format.numberFormat = "0.00%";
workbook.worksheets.getItem("条件別収支").freezePanes.freezeColumns(5);
workbook.worksheets.getItem("月別・日別").freezePanes.freezeColumns(3);

audit.showGridLines = false;
audit.freezePanes.freezeRows(2);
setTitle(audit, "A1:F1", "モデル選択・データフロー・未来情報監査");
audit.getRange("A3:B3").values = [["項目", "確認結果"]];
styleHeader(audit.getRange("A3:B3"));
audit.getRange("A4:B17").values = [
  ["本番選択ファイル", auditJson.selector.production_selected_file],
  ["本番選択規則", auditJson.selector.production_selection_rule],
  ["本番が採用JSONを使うか", auditJson.selector.production_uses_adoption_json],
  ["optimizer baseline", auditJson.selector.optimizer_baseline_selected_file],
  ["比較対象DataFrame同一", auditJson.feature_input.same_dataframe_object_used_for_both_models],
  ["後続DataFrame列数", auditJson.feature_input.column_count],
  ["スコア特徴量列数", auditJson.feature_input.scoring_feature_columns.length],
  ["結果・払戻をスコア入力に使うか", auditJson.results_data_flow.prediction_score_uses_results_or_payout],
  ["結果を結合する時点", "順位確定後の評価時"],
  ["確定人気を使う時点", "順位確定後の事後セグメントのみ"],
  ["確定オッズをスコア／順位に使うか", auditJson.leakage_audit.final_odds_used_for_score_or_rank],
  ["同日・未来の過去走を除外", auditJson.leakage_audit.future_or_same_date_past_races_excluded],
  ["同日・未来のratingを除外", auditJson.leakage_audit.future_or_same_date_ratings_excluded],
  ["DL結果学習の順位影響", auditJson.leakage_audit.dl_result_training_has_ranking_effect],
];

audit.getRange("A19:F19").merge();
audit.getRange("A19").values = [["列一覧"]];
styleSection(audit.getRange("A19:F19"));
audit.getRange("A20:D20").values = [[
  "後続DataFrame列",
  "スコア参照特徴量",
  "結果entry列",
  "払戻列",
]];
styleHeader(audit.getRange("A20:D20"));
const frameColumns = auditJson.feature_input.columns;
const scoreColumns = auditJson.feature_input.scoring_feature_columns;
const resultColumns = auditJson.results_data_flow.result_entry_columns;
const payoutColumns = auditJson.results_data_flow.payout_columns;
const maxColumnRows = Math.max(
  frameColumns.length,
  scoreColumns.length,
  resultColumns.length,
  payoutColumns.length,
);
const columnRows = [];
for (let index = 0; index < maxColumnRows; index += 1) {
  columnRows.push([
    frameColumns[index] ?? null,
    scoreColumns[index] ?? null,
    resultColumns[index] ?? null,
    payoutColumns[index] ?? null,
  ]);
}
audit.getRangeByIndexes(20, 0, columnRows.length, 4).values = columnRows;
audit.getRange("A1:F140").format.font = { name: "Aptos", size: 10 };
audit.getRange("A1:F140").format.verticalAlignment = "center";
audit.getRange("A1:B17").format.wrapText = true;
audit.getRange("A1:A140").format.columnWidth = 34;
audit.getRange("B1:B140").format.columnWidth = 45;
audit.getRange("C1:D140").format.columnWidth = 24;

await fs.mkdir(outputDir, { recursive: true });
const workbookInspect = await workbook.inspect({
  kind: "workbook,sheet,formula",
  maxChars: 10000,
  tableMaxRows: 8,
  tableMaxCols: 10,
  options: { maxResults: 100 },
});
await fs.writeFile(
  path.join(outputDir, "workbook_inspect.ndjson"),
  workbookInspect.ndjson,
  "utf8",
);

const previewDir = path.join(outputDir, "workbook_previews");
await fs.mkdir(previewDir, { recursive: true });
for (const sheetName of ["比較サマリー", ...sourceSheets.map(([name]) => name), "データフロー監査"]) {
  const preview = await workbook.render({
    sheetName,
    autoCrop: "all",
    scale: sheetName === "比較サマリー" ? 1 : 0.65,
    format: "png",
  });
  await fs.writeFile(
    path.join(previewDir, `${sheetName}.png`),
    new Uint8Array(await preview.arrayBuffer()),
  );
}

const exported = await SpreadsheetFile.exportXlsx(workbook);
await exported.save(outputPath);
console.log(JSON.stringify({ outputPath, sheetCount: 12 }, null, 2));
