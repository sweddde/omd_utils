FETCH_NODES = """
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
    graphdb_graph.nodes_tags nt 
on
    n.id = nt.node_id
join 
    graphdb_graph.namespaces ns 
on
    n.namespace_id = ns.id
where 
    nt.tag_id = %(tag_id)s
    and ns.name = any(%(schemas)s)
    and n.state = %(state)s
    and n.operator_id != %(operator_id)s
    {extra_filter}
"""
