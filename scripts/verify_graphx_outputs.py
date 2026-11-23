from pathlib import Path
import csv, json, sys
root = Path(__file__).resolve().parents[1]
out = root / "output"
required = [out / "pagerank_results.csv", out / "communities_results.csv", out / "graph_stats.json"]
missing = [str(p) for p in required if not p.is_file()]
if missing:
    print("MISSING OUTPUTS:", *missing, sep="\n- "); sys.exit(1)
stats = json.loads(required[2].read_text(encoding="utf-8"))
with required[0].open(newline="", encoding="utf-8") as f: ranks = list(csv.DictReader(f))
with required[1].open(newline="", encoding="utf-8") as f: communities = list(csv.DictReader(f))
assert stats.get("engine") == "Apache Spark GraphX 3.5.3", "Unexpected engine label"
assert len(ranks) == stats["vertices"], "PageRank row count does not match vertex count"
assert len(communities) == stats["vertices"], "Community row count does not match vertex count"
assert all(float(row["pagerank"]) >= 0 for row in ranks), "Negative PageRank value"
assert len({row["user_id"] for row in communities}) == stats["vertices"], "Duplicate/missing community user IDs"
print("OUTPUTS_VALID")
print(f"Engine: {stats['engine']} | vertices={stats['vertices']} | edges={stats['edges']} | components={stats['components']}")
