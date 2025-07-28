FETCH_TABLE_DESCRIPTION = """
select 
    description
from 
    graph.nodes
where 
    id = %(node_id)s;
"""

FETCH_COLUMNS_DESCRIPTIONS = """
select
    "name" as column_name
    , description
from 
    graph."columns"
where 
    node_id = %(node_id)s
    and description is not null;
"""