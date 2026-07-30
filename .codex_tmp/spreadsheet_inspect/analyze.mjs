import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const inputPath = process.argv[2];
if (!inputPath) {
  throw new Error("解析対象xlsxパスを指定してください。");
}

const input = await FileBlob.load(inputPath);
const workbook = await SpreadsheetFile.importXlsx(input);
const requestedSheet = process.argv[3];
if (requestedSheet === "__sheets__") {
  for (const sheet of workbook.worksheets) {
    console.log(JSON.stringify({
      name: sheet.name,
      range: sheet.getUsedRange().address,
    }));
  }
  process.exit(0);
}
if (requestedSheet) {
  const sheet = workbook.worksheets.getItem(requestedSheet);
  const usedRange = sheet.getUsedRange();
  const detail = await workbook.inspect({
    kind: "region",
    sheetId: requestedSheet,
    range: usedRange.address,
    maxChars: 50000,
    tableMaxRows: 300,
    tableMaxCols: 30,
    tableMaxCellChars: 160,
  });
  console.log(detail.ndjson);
  process.exit(0);
}
const summary = await workbook.inspect({
  kind: "workbook,sheet,table",
  maxChars: 12000,
  tableMaxRows: 8,
  tableMaxCols: 12,
  tableMaxCellChars: 120,
});
console.log(summary.ndjson);
