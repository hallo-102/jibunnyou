import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const projectRoot = process.cwd();
const timestamp = process.argv[2] ?? "20260605_190000";
const reportsDir = path.join(projectRoot, "reports");
const inputJson = path.join(reportsDir, `racedata_deep_analysis_tables_${timestamp}.json`);
const outputXlsx = path.join(reportsDir, `racedata_deep_analysis_summary_${timestamp}.xlsx`);

function cleanSheetName(name, used) {
  const base = String(name)
    .replace(/[\\/?*:[\]]/g, "_")
    .slice(0, 31) || "Sheet";
  let candidate = base;
  let i = 2;
  while (used.has(candidate)) {
    const suffix = `_${i}`;
    candidate = `${base.slice(0, 31 - suffix.length)}${suffix}`;
    i += 1;
  }
  used.add(candidate);
  return candidate;
}

function toMatrix(table) {
  const headers = table.headers ?? [];
  const rows = table.rows ?? [];
  return [headers, ...rows].map((row) => row.map((cell) => (cell == null ? "" : String(cell))));
}

function setColumnWidths(sheet, colCount) {
  for (let col = 0; col < colCount; col += 1) {
    const width = col === 0 ? 150 : 105;
    sheet.getRangeByIndexes(0, col, 1, 1).format.columnWidthPx = width;
  }
}

const raw = await fs.readFile(inputJson, "utf8");
const tables = JSON.parse(raw);

const workbook = Workbook.create();
const usedNames = new Set();
const sheetNames = [];

for (const table of tables) {
  const sheetName = cleanSheetName(table.name, usedNames);
  sheetNames.push(sheetName);
  const sheet = workbook.worksheets.add(sheetName);
  sheet.showGridLines = false;

  const matrix = toMatrix(table);
  const rowCount = Math.max(matrix.length, 1);
  const colCount = Math.max(matrix[0]?.length ?? 1, 1);

  sheet.getRange("A1").values = [[table.name]];
  sheet.getRange("A1").format = {
    font: { bold: true, color: "#111827", size: 14 },
    fill: "#F3F4F6",
  };

  const tableRange = sheet.getRangeByIndexes(2, 0, rowCount, colCount);
  tableRange.values = matrix;
  sheet.getRangeByIndexes(2, 0, 1, colCount).format = {
    fill: "#1F2937",
    font: { bold: true, color: "#FFFFFF" },
  };
  tableRange.format.borders = { preset: "all", style: "thin", color: "#D1D5DB" };
  tableRange.format.wrapText = true;
  setColumnWidths(sheet, colCount);
  sheet.freezePanes.freezeRows(3);
}

const overview = workbook.worksheets.getItem("概要");
if (overview) {
  overview.getRange("D1").values = [["読み方"]];
  overview.getRange("D2").values = [[
    "各シートはMarkdownレポートと同じ集計表です。率は100円均等買いベースの単勝・複勝回収率を含みます。",
  ]];
  overview.getRange("D1:D2").format = {
    fill: "#E0F2FE",
    font: { color: "#0F172A" },
  };
  overview.getRange("D:D").format.columnWidthPx = 520;
}

const inspect = await workbook.inspect({
  kind: "sheet,table",
  maxChars: 4000,
  tableMaxRows: 5,
  tableMaxCols: 8,
});
console.log(inspect.ndjson);

for (const sheetName of sheetNames) {
  const preview = await workbook.render({
    sheetName,
    autoCrop: "all",
    scale: 1,
    format: "png",
  });
  const safeName = sheetName.replace(/[\\/?*:[\]]/g, "_");
  await fs.writeFile(
    path.join(reportsDir, `racedata_deep_analysis_summary_${timestamp}_${safeName}.png`),
    new Uint8Array(await preview.arrayBuffer()),
  );
}

const xlsx = await SpreadsheetFile.exportXlsx(workbook);
await xlsx.save(outputXlsx);
console.log(`output=${outputXlsx}`);
