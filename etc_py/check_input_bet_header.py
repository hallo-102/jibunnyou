# etc_py/print_bet_headers_output.py
import pandas as pd

OUT_EXCEL = r"C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026\data\output\馬の競走成績_with_feat_20260207.xlsx"
SHEET = "買い目_レース別1行"

df = pd.read_excel(OUT_EXCEL, sheet_name=SHEET, engine="openpyxl")
print(list(df.columns))
