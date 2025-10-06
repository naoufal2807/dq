import argparse, os, json
from sql_parser import diff_columns
from lineage import Lineage
from risk_engine import assess_risk, md_report


def read(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def infer_node_id_from_filename(manifest_path: str, after_sql_path: str) -> str | None:
    """
    OPTIONAL convenience: look up a node whose compiled path or name hints at the file.
    For production, keep a mapping file or pass --node-id explicitly.
    """
    name_guess = os.path.splittext(os.path.basename(after_sql_path))[0]
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    for node_id, node in manifest.get("nodes", {}).items():
        if node_id.endswith(f".{name_guess}"):
            return node_id
    return None


def main():
    ap = argparse.ArgumentParser(description="Schema Change Risk ( Don't let your dbt model break your warehouse )")
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--before", required=True)
    ap.add_argument("--after", required=True)
    ap.add_argument("--dialect", default=None, help="snowflake|bigquery|postgres|spark ect..")
    ap.add_argument("--node-id", default=None, help="dbt node id (e.g., model.myproj.users)")
    args = ap.parse_args()

    before_sql = read(args.before)
    after_sql = read(args.after)

    diff = diff_columns(before_sql, after_sql, dialect=args.dialect)

    node_id = args.node_id or infer_node_id_from_filename(args.manifest, args.after)
    if not node_id:
        # fallback: use filename model
        node_name = os.path.splitext(os.path.basename(args.after))[0]
        node_id = f"model.UNKNOWNPROJ.{node_name}"

    lineage = Lineage.from_dbt_manifest(args.manifest)
    impact = assess_risk(lineage, node_id, diff["removed"], diff["renames"])

    report = md_report(node_id, diff, impact, dashboards_hint=["Monthly Revenue", "Customer Retention"])
    print(report)


if __name__ == "__main__":
    main()
    