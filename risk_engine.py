from __future__ import annotations
from typing import Dict, List, Tuple
from lineage import Lineage


def assess_risk(lineage: Lineage,
                model_node_id: str,
                removed: List[str],
                renames: List[Tuple[str, str]]) -> Dict:
    
    impacted = lineage.downstream(model_node_id)
    severity = "LOW"
    if removed or renames:
        severity = "HIGH" if impacted else "MEDIUM"
        
    return {
        "root": model_node_id,
        "impacted": impacted,
        "severity": severity
    }
    

def md_report(
        model_node_id: str,
        diff: Dict,
        impact: Dict,
        dashboards_hint: List[str] | None = None) -> str:
    
    added = diff.get("added", [])
    removed = diff.get("removed", [])
    renames = diff.get("renames", [])
    impacted = impact.get("impacted", [])
    severity = impact.get("severity", "LOW")
    sev_emoji = {"HIGH":"🔴","MEDIUM":"🟠","LOW":"🟢"}.get(severity, "🟢")
    
    lines = []
    lines.append("## 🔍 Schema Change Risk Report\n")
    lines.append(f"**Model:** `{model_node_id}`\n")
    lines.append(f"**Risk Level:** {sev_emoji} **{severity}**\n")

    lines.append("\n**Detected Changes:**")
    if added: lines.append(f"- Added columns: `{', '.join(added)}`")
    if removed: lines.append(f"- Removed columns: `{', '.join(removed)}`")
    if renames:
        for a,b in renames:
            lines.append(f"- Renamed: `{a}` -> `{b}`")
    
    if not (added or removed or renames):
        lines.append("- No changes detected.")
        
    lines.append("\n**Impact Analysis:**")
    if impacted:
        lines.append(f"- Downstream nodes likely affected ({len(impacted)})")
        for m in impacted:
            lines.append(f"  - `{m}`")
    else:
        lines.append("- No downstream nodes detected.")
    
    if renames or removed:
        lines.append("\n**Recommended Actions:**")
        if renames:
            lines.append("Consider **aliasing** the new column to the old name** in this release to avoid breaking dependents.")
            lines.append("Example:")
            lines.append("```sql")
            for a,b in renames:
                lines.append(f"SELECT ..., {b} AS {a}, ...")
            lines.append("```")
        elif removed:
            lines.append("If a removed column is still referenced downstream, re-introduce it temporarily via alias until dependents are updated.")

    if dashboards_hint:
        lines.append("\n**Dashboards referencing downstream models (hint):**")
        for d in dashboards_hint:
            lines.append(f"- {d}")

    return "\n".join(lines) + "\n"
            