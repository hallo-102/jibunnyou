"""保存結果をJRA公式全レースと照合する、一度限りの復旧監査。"""
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from continuous_improvement.archive import sha256, write_json
from continuous_improvement.review import key, load_results

base = Path(sys.argv[1])
folder = base / "official_verification"
folder.mkdir(exist_ok=True)
horses, payouts = load_results(base / "work/racedata_results.xlsx", "20261114")
entries = pd.read_excel(base / "pre/input.xlsx", sheet_name="今走レース情報")
expected = set(entries["レースID"].map(key))
seed = "https://app.jra.jp/JRADB/accessS.html?CNAME=sw01sde0105202605030120261114/35"
queue, visited, reports = [seed], set(), []
while queue:
    url = queue.pop(0)
    match = re.search(r"sw01sde01(\d{2})(2026)(\d{2})(\d{2})(\d{2})20261114", url)
    if not match:
        continue
    place, year, meeting, day, race = match.groups()
    rid = year + place + meeting + day + race
    if rid in visited or rid not in expected:
        continue
    visited.add(rid)
    try:
        cache = folder / (rid + ".html")
        if not cache.exists():
            # 公式へのアクセスは逐次・低頻度とし、取得済みページから再開できる。
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            cache.write_bytes(response.content)
            time.sleep(0.6)
        soup = BeautifulSoup(cache.read_bytes(), "html.parser")
        for link in soup.select("a[href]"):
            href = urljoin(url, link["href"])
            if "accessS.html?CNAME=sw01sde01" in href and "20261114" in href:
                queue.append(href)
        errors = []
        if "2026年11月14日" not in soup.get_text():
            errors.append("公式開催日不一致")
        official_horses = {}
        for row in soup.select("table tr"):
            horse, finish = row.select_one(".ubanTd"), row.select_one(".tyakuTd")
            if horse and finish:
                official_horses[int(horse.get_text(strip=True))] = finish.get_text(strip=True)
        saved = horses.loc[horses["rid_str"].eq(rid)]
        if set(official_horses) != set(saved["馬番"]):
            errors.append("公式馬番集合・頭数不一致")
        for row in saved.to_dict("records"):
            finish = official_horses.get(row["馬番"])
            if finish != key(row["着順"]):
                errors.append(f"着順不一致: {row['馬番']} saved={row['着順']} official={finish}")
        official_payouts, kind = {}, None
        for row in soup.select("table tr"):
            combo, amount = row.select_one(".horseNum"), row.select_one(".dividend")
            if not combo or not amount:
                continue
            heading = row.find("th")
            if heading:
                kind = heading.get_text(strip=True)
            # 少頭数の非発売枠連は公式表に空欄行だけが残る。
            if not combo.get_text(strip=True) and not amount.get_text(strip=True):
                continue
            combination = "-".join(str(int(v)) for v in re.findall(r"\d+", combo.get_text()))
            official_payouts[(rid, kind, combination)] = int(re.sub(r"\D", "", amount.get_text()))
        saved_payouts = {k: v for k, v in payouts.items() if k[0] == rid}
        if not official_payouts or saved_payouts != official_payouts:
            errors.append("払戻キー・金額不一致")
        reports.append({"race_id": rid, "url": url, "html_sha256": sha256(cache),
                        "horses": len(official_horses), "payouts": len(official_payouts), "errors": errors})
    except Exception as exc:
        reports.append({"race_id": rid, "url": url, "errors": [str(exc)]})
    write_json(folder / "verification.json", {"day": "20261114", "expected_races": len(expected),
        "verified_races": sum(not r["errors"] for r in reports), "missing_races": sorted(expected - visited), "races": reports})
result = json.loads((folder / "verification.json").read_text(encoding="utf-8"))
print(json.dumps(result, ensure_ascii=False))
assert result["verified_races"] == len(expected), "公式照合未完了。verification.jsonから再開"
