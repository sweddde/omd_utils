FETCH_NODES_ADDITIONAL= """
select 
    n.id 
    , n.name
    , n.description
    , n.namespace_id
    , ns.name as db_schema
    , n.updated
from
    graphdb_graph.nodes n
join 
    graphdb_graph.namespaces ns 
on 
    n.namespace_id = ns.id
where 
    n.id = any(%(node_ids)s)
    and n.state = %(state)s
    and n.operator_id != %(operator_id)s
"""