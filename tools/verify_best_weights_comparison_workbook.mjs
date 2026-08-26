import {
  FileBlob,
  SpreadsheetFile,
} from "file:///C:/Users/okino/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/@oai/artifact-tool/dist/artifact_tool.mjs";
import { fileURLToPath } from "node:url";

const workbookPath = new URL(
  "../outputs/best_weights_profit_comparison_20260817/best_weights_profit_comparison_20260817.xlsx",
  import.meta.url,
);
const workbook = await SpreadsheetFile.importXlsx(
  await FileBlob.load(fileURLToPath(workbookPath)),
);

const sheets = await workbook.inspect({
  kind: "sheet",
  include: "id,name",
  maxChars: 5000,
});
const keyRanges = await workbook.inspect({
  kind: "region",
  sheetId: "比較サマリー",
  range: "A17:H42",
  maxChars: 18000,
  tableMaxRows: 30,
  tableMaxCols: 8,
});
const formulaErrors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: {
    useRegex: true,
    matchCase: false,
    maxResults: 100,
  },
  maxChars: 10000,
});

console.log("SHEETS");
console.log(sheets.ndjson);
console.log("KEY_RANGES");
console.log(keyRanges.ndjson);
console.log("FORMULA_ERRORS");
console.log(formulaErrors.ndjson);
