from __future__ import annotations
from typing import List, Optional
import sqlglot
from sqlglot import exp


def extract_output_columns(sql: str, dialect: Optional[str] = None)-> List[str]:
    """
    Returns a list of column names for the outermost SELECT statement
    
    Works with complex SQL: CTEs, experessions, aliases, functions, qualify, etc..
    Args:
        sql (str): The SQL query to analyze.
        dialect (Optional[str]): The SQL dialect to use for parsing. Defaults to None.
    """
    
    try: 
        parsed = sqlglot.parse_one(sql, read=dialect) if dialect else sqlglot.parse_one(sql)
    except Exception as e:
        
        # dialect might be wrong try without it
        parsed = sqlglot.parse_one(sql)
    
    
    # Find the top-level SELECT statement (skip CTE definitions; we want the final projection)
    select_node = parsed.find(exp.Select)
    if not select_node:
        return []
    
    out_names: List[str] = []
    for proj in select_node.expressions:
        # If it has an explicit alias (e.g., SUM(x) AS total), use that
        alias = proj.alias
        if alias:
            out_names.append(alias.lower())
            continue
        
        # Otherwise, derive a name : try to use the column's own name if it's a naked column
        # SELECT email FROM users -> email             
        if isinstance(proj, exp.Column):
            out_names.append(proj.name.lower())
            continue
        
        # sqlglot foten annotates experssion outputs with .alias or .output_name
        # trying best-effort
        output_name = proj.alias_or_name
        if output_name:
            out_names.append(str(output_name).lower())
            continue
        
        # As a fallback, use sqlgplot's natural name heuristic
        derived = proj.output_name
        if derived:
            out_names.append(str(derived).lower())
            continue
    
    return out_names


def diff_columns(before_sql: str, after_sql: str, dialect: Optional[str] = None):
    """
    Determines adde/removed columnns and infer renames.
    We use set diffs + a small heuristic for 1-to-1 rename.
    """
    before_cols = extract_output_columns(before_sql, dialect=dialect)
    after_cols = extract_output_columns(after_sql, dialect=dialect)
    
    bset, aset = set(before_cols), set(after_cols)
    added = sorted(aset - bset)
    removed = sorted(bset - aset)
    renames = []
    
    # simple rename heuristic: if counts match 1-to-1, pair them
    if len(removed) == 1 and len(added) == 1:
        renames.append((removed[0], added[0]))
    
    return {
        "before": before_cols,
        "after": after_cols,
        "added": added,
        "removed": removed,
        "renames": renames,
    }

# Example usage 

# tests (quick run in a REPL)
bq_sql_before = """
WITH x AS (
  SELECT SAFE_CAST(id AS INT64) AS user_id,
         email AS user_email,
         country
  FROM raw.users
)
SELECT user_id, user_email, country FROM x
"""

bq_sql_after = """
WITH x AS (
  SELECT SAFE_CAST(id AS INT64) AS user_id,
         email,
         country
  FROM raw.users
)
SELECT user_id, email, country FROM x QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY user_id) = 1
"""

print(extract_output_columns(bq_sql_before, dialect="bigquery"))  # ['user_id','user_email','country']
print(extract_output_columns(bq_sql_after,  dialect="bigquery"))  # ['user_id','email','country']
print(diff_columns(bq_sql_before, bq_sql_after, dialect="bigquery"))
# expect removed=['user_email'], added=['email'], renames=[('user_email','email')]
