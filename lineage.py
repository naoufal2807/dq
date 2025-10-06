from __future__ import annotations
from typing import Dict, List, Set
import json

class Lineage:
    def __init__(self, children: Dict[str, List[str]]):
        # children: parent_node -> [direct_children]
        self.children = children
        
    @classmethod
    def from_dbt_manifest(cls, path:str) -> Lineage:
        """
        Build forward edges from dbt manifest:
        - For each node, for each dependency (depends_on.nodes), add node as child of dependency.
        
        """
        with open(path, 'r') as f:
            manifest = json.load(f)
        
        nodes = manifest.get("nodes", {})
        children: Dict[str, List[str]] = {}
        
        for node_id, node in nodes.items():
            deps = node.get("depends_on", {}).get("nodes", [])
            for parent in deps:
                children.setdefault(parent, []).append(node_id)
        
        return cls(children)
    
    
    def downstream(self, node:str) -> List[str]:
        """BFS downstream from a node id (e.g., 'model.projects.users')."""
        out: List[str] = []
        seen: Set[str] = set([node])
        queue = [node]
        while queue:
            cur = queue.pop(0)
            for child in self.children.get(cur, []):
                if child not in seen:
                    seen.add(child)
                    out.append(child)
                    queue.append(child)
                    
        return out
    
        