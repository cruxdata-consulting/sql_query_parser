import sys
import os
import json

# Get the path to folder2 and add it to sys.path
script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'query_parser')
sys.path.append(script_path)

from query_parser import Query

def main():
    
    q_text = '''
    with subq_a as (
    select sum(1) as col1
    from y left outer join foo on y.a = foo.b
    where i = 0
    )
    , subq_b as (
    select 1 as col1
    from x inner join z on x.a = z.b
    )
    , bar as (
    select 'something' as bar
    )
    , baz as (
    select a, b
    from x inner join z on x.a = z.b
    )
    select 
        a.col1 as a_col1
    from 
        subq_a a cross join subq_b b 
        on a.col1 = b.col1
    '''
    q = Query(q_text, 'bigquery')

    # populate a dictionary of features
    feature_dict = {}

    feature_dict['query_string'] = q.query_string
    feature_dict['table_list'] = q.get_table_list()
    feature_dict['has_select_star'] = q.has_select_star()
    feature_dict['ctes_without_aggs'] = q.ctes_without_aggs()
    feature_dict['selects_wo_where_clauses'] = q.selects_without_where_clauses()
    feature_dict['basetables_and_where_clauses'] = q.generate_basetable_where_dict()
    feature_dict['join_types_used'] = q.get_join_types_used()
    feature_dict['count_of_scope_types'] = q.get_scope_types_count()
    
    print(json.dumps(feature_dict, indent=4))

if __name__ == "__main__":
    main()