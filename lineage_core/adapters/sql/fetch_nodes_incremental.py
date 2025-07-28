FETCH_NODES_INCREMENTAL = """
select distinct on (n.name) 
    n.id 
    , n.name
    , n.namespace_id
    , ns.name as db_schema
    , coalesce(n.updated, n.created) as updated
    , n.state 
from
    graph.nodes n
join
    graph.nodes_tags nt 
on 
    n.id = nt.node_id
join 
    graph.namespaces ns 
on 
    n.namespace_id = ns.id
where 
    nt.tag_id = %(tag_id)s
    and ns.name = any(%(schemas)s)
    and operator_id != %(operator_id)s
    and coalesce(n.updated, n.created) > (%(last_executed)s - interval '%(safety_window_hours)s hours')
order by n.name, coalesce(n.updated, n.created) desc, n.id desc
"""