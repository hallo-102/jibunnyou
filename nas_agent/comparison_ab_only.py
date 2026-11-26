"""Run the single-day A/B evaluation with the fixed PR #34 implementation."""
from collections import Counter
from pathlib import Path
import sys


def run(config_path, output):
    # This entry point is in the installed agent. All comparison/model imports
    # come from the independently verified PR #34 checkout (the working directory).
    source = Path.cwd()
    sys.path.insert(0, str(source))
    from tools import compare_ab_models as comparison

    sys.path.append(str(source / "race_purchase_filter"))
    from race_purchase_filter import assessment

    config = comparison.read_json(config_path)
    bounds = config["periods"]["validation"]
    periods = {"validation": tuple(comparison.date.fromisoformat(day) for day in bounds)}
    unit = comparison.integer_setting(config, "unit_yen", 100, 100)
    box_size = comparison.integer_setting(config, "box_size", 5, 3)
    high = comparison.integer_setting(config, "top_payout_races", 3, 0)
    if unit % 100 or box_size > 5:
        raise ValueError("Invalid unit_yen or box_size")
    snapshots_path = config_path.parent / config["snapshots"]
    results_path = config_path.parent / config["results"]
    snapshots = comparison.read_json(snapshots_path)
    result_rows = comparison.read_json(results_path)
    builders, diagnostics = comparison.diagnostic_builders(source)
    baselines = comparison.baselines_from(config, config_path.parent)
    races, counts, missing, excluded = comparison.load_races(config_path.parent, snapshots, periods, builders)
    results = comparison.read_results(result_rows, races)
    coverage = comparison.coverage_counts(races, results, periods, missing, excluded)
    methods = {method: comparison.METHODS[method] for method in config["methods"]}
    scored, diagnostic_rows = comparison.evaluate(
        races, results, baselines, {}, methods, (unit, box_size), diagnostics, assessment,
    )
    summary, breakdown = comparison.grouped_summary(scored, unit, high, tuple(periods))
    parity = Counter(row["status"] for row in diagnostic_rows if row["comparison"] == "main_parity")
    if any(output.iterdir()):
        raise ValueError("Comparison output directory must be empty")
    for name, rows in (
        ("comparison", summary),
        ("breakdown", breakdown),
        ("races", [row for method_rows in scored.values() for row in method_rows]),
        ("diagnostics", diagnostic_rows),
        ("exclusions", excluded),
    ):
        comparison.save_json(output / f"{name}.json", rows)
        comparison.save_csv(output / f"{name}.csv", rows)
    manifest = {
        "schema_version": 1,
        "mode": config["mode"],
        "code_sha": comparison.git(source, "rev-parse", "HEAD").decode().strip(),
        "runner_sha256": comparison.sha256_file(Path(__file__)),
        "methods": {label: {method: model for method in methods}
                    for label, (_, model) in baselines.items()},
        "periods": config["periods"],
        "training": "not_run",
        "inputs": {
            "config_sha256": comparison.sha256_file(config_path),
            "snapshots_sha256": comparison.sha256_file(snapshots_path),
            "results_sha256": comparison.sha256_file(results_path),
            "fixture_manifest_sha256": comparison.sha256_file(config_path.parent / "manifest.json"),
            "snapshot_counts": dict(counts),
        },
        "coverage": coverage,
        "purchase": {"filter_policy": assessment.policy_version(assessment.load_policy()),
                     "cohort": "intersection_of_buy_candidates_across_A_and_B",
                     "unit_yen": unit, "box_size": box_size, "top_payout_races": high},
        "diagnostics": {"variants": diagnostics, "main_parity_counts": dict(parity)},
        "output_sha256": {path.name: comparison.sha256_file(path) for path in sorted(output.iterdir())},
    }
    comparison.save_json(output / "manifest.json", manifest)
    return 0 if parity and set(parity) == {"equal"} else 2


if __name__ == "__main__":
    sys.exit(run(Path(sys.argv[1]), Path(sys.argv[2])))
