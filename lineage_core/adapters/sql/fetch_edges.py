FETCH_EDGES = """
select 
    source_id as from_node_id
    , target_id as to_node_id
from 
    graph.dependencies
where 
    removed_commit_id is null
   and (
   source_id = any(%(node_ids)s) 
   or 
   target_id = any(%(node_ids)s))
"""
