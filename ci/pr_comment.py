import argparse, os, subprocess, tempfile, json, textwrap
from pathlib import Path
import requests


# Import your CLI pieces
from ..sql_parser import diff_columns
from ..lineage import Lineage
from ..risk_engine import assess_risk, md_report

def sh(cmd: str) -> str:
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\n{res.stderr}")
    return res.stdout.strip()

def git_show(path: str, ref: str) -> str:
    return sh(f'git show {ref}:"{path}"')

def file_exists_in_ref(path: str, ref: str) -> bool:
    try:
        sh(f'git cat-file -e {ref}:"{path}"')
        return True
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--head", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--dialect", default=None)
    ap.add_argument("--pr", required=True, type=int)
    args = ap.parse_args()

    # Find changed SQL files under models/ (adjust pattern if needed)
    diff_files = sh(f'git diff --name-only {args.base} {args.head}').splitlines()
    changed_sql = [p for p in diff_files if p.lower().endswith(".sql")]

    if not changed_sql:
        print("No .sql changes; skipping.")
        return

    # Load lineage (dbt manifest). Fall back gracefully if missing.
    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"[warn] manifest not found at {args.manifest}. Downstream impact may be empty.")
        lineage = Lineage(children={})
    else:
        lineage = Lineage.from_dbt_manifest(str(manifest_path))

    # Build a combined markdown report
    sections = []
    for path in changed_sql:
        # Skip non-models if you want (e.g., macros). Customize as needed:
        if "/macros/" in path or "\\macros\\" in path:
            continue

        # Compute before/after content; if file didn’t exist in base, treat before as empty
        before_sql = ""
        if file_exists_in_ref(path, args.base):
            before_sql = git_show(path, args.base)
        # After is from the workspace (checked out head)
        try:
            after_sql = Path(path).read_text(encoding="utf-8")
        except Exception:
            # If file was deleted in head, treat after as empty
            after_sql = ""

        diff = diff_columns(before_sql, after_sql, dialect=args.dialect)

        # Infer a node id by filename suffix; better if you map via manifest nodes
        node_guess = f"model.UNKNOWN.{Path(path).stem}"
        try:
            with open(args.manifest, "r", encoding="utf-8") as f:
                m = json.load(f)
            for nid in m.get("nodes", {}):
                if nid.endswith(f".{Path(path).stem}"):
                    node_guess = nid
                    break
        except Exception:
            pass

        impact = assess_risk(lineage, node_guess, diff["removed"], diff["renames"])
        report = md_report(node_guess, diff, impact, dashboards_hint=None)
        header = f"### File: `{path}`"
        sections.append(header + "\n\n" + report)

    if not sections:
        print("No relevant SQL model changes found.")
        return

    body = "## 🔍 Schema Change Risk — PR Summary\n\n" + "\n---\n".join(sections)

    # Post comment to GitHub PR
    repo = os.getenv("GITHUB_REPOSITORY")  # e.g., owner/repo
    token = os.getenv("GITHUB_TOKEN")
    if not repo or not token:
        print("[warn] Missing GITHUB_REPOSITORY or GITHUB_TOKEN; printing output:")
        print(body)
        return

    url = f"https://api.github.com/repos/{repo}/issues/{args.pr}/comments"
    resp = requests.post(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }, json={"body": body})
    if resp.status_code >= 300:
        raise RuntimeError(f"Failed to post comment: {resp.status_code} {resp.text}")
    print("Comment posted.")
    return

if __name__ == "__main__":
    main()
