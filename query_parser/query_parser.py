from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import build_scope
from sqlglot.optimizer import optimize
from collections import Counter

## Helper methods
def _remove_duplicate_dicts(list_of_dicts):
    # Convert dictionaries to tuples (with sorted lists as values) and add them to a set
    unique_set = set(tuple((k, tuple(sorted(v))) for k, v in d.items()) for d in list_of_dicts)
    
    # Convert tuples back to dictionaries and add them to a list
    unique_list = [dict(t) for t in unique_set]
    
    return unique_list

def _scope_has_base_table(scope):
    '''
    Helper method that takes in scope and outputs whether it references a base table
    '''
    has_base_table = False
    for alias, (node, source) in scope.selected_sources.items():
        if isinstance(source, exp.Table):
            has_base_table = True
    return has_base_table
## Helper methods

class Query():

    def __init__(self, query_text, dialect=None) -> None:
        self.dialect = dialect
        self.query_string = query_text

        def is_select_statement() -> bool:
            '''
            Check if root query text is a select statement and return true if it is
            '''
            for select in parse_one(self.query_string, dialect=self.dialect).find_all(exp.Select):
                if select:
                    return True
            return False
        
        self.is_valid = is_select_statement() and self.query_string 
        
        if self.is_valid:
            self.ast = parse_one(query_text, dialect=dialect)
        else:
            self.ast = None

    def get_table_list(self, unique=True) -> list[str]:
        '''
        Gets the list of "source" tables from a query string
        '''
        table_list = []
        root = build_scope(self.ast)
        
        if self.is_valid:
            for table in self.ast.find_all(exp.Table):
                if table.db: # Works better than build_scope in most cases
                    table_list.append(table.name)
        
        if unique:
            return list(set(table_list))
        return table_list

    def get_join_types_used(self) -> dict:
        '''
        Get a dictionary of join types and the number of times
        that each join type is used across the query
        '''
        joins = []
        for join in self.ast.find_all(exp.Join):
            join_name = join.side.strip() + " " + join.kind.strip()
            joins.append(join_name.strip())

        return dict(Counter(joins))
    
    def has_select_star(self) -> bool:
        '''
        Returns Boolean for if any component of the query uses a SELECT * expression
        '''
        has_select = False
        for select in parse_one(self.query_string, dialect=self.dialect).find_all(exp.Select):
            for projection in select.expressions:
                if projection.alias_or_name == '*':
                    has_select = True
        
        return has_select
    
    def selects_without_where_clauses(self) -> list[str]:
        '''
        Returns a list of CTEs SQL text strings that don't have a where clause
        '''
        select_without_where_clauses = []
        if self.is_valid:
            
            root = build_scope(self.ast)
            for scope in root.traverse():

                # Check if query contains any source table in from clause
                has_base_table = _scope_has_base_table(scope)

                if has_base_table:
                    has_where_clause = False
                    for where in scope.find_all(exp.Where):
                        if where:
                            has_where_clause = True
                    if not has_where_clause:
                        select_without_where_clauses.append(scope.expression.sql())
            return select_without_where_clauses

    def generate_basetable_where_dict(self) -> list[str]:
        '''
        Iterate through all scopes with base tables
        and populate the where clauses as values.

        This helps map to whether partition or clustering columns are being used within where clauses
        '''
        basetable_where_list = []
        unique_list = []
        if self.is_valid:
            root = build_scope(self.ast)
            for scope in root.traverse():

                # check if base table is referenced
                has_base_table = _scope_has_base_table(scope)

                if has_base_table:
                    for alias, (node, source) in scope.selected_sources.items():
                        kv = {}
                        # check if base table involved
                        if isinstance(source, exp.Table):
                            # add all where clause column names to dict
                            where_clause_columns = []
                            for where in scope.find_all(exp.Where):
                                for col in where.find_all(exp.Column):
                                    where_clause_columns.append(col.sql())
                            kv[source.sql()] = where_clause_columns
                            # append dict
                            basetable_where_list.append(kv)
            unique_list = _remove_duplicate_dicts(basetable_where_list)
        return unique_list

    def ctes_without_aggs(self) -> list:
        '''
        Returns a list of CTEs SQL text strings that don't have db tables as sources 
        and don't have aggregations either (basically "staging" tables that re-scan the same bytes)
        '''
        ctes_without_agg_expressions = []

        if self.is_valid:
            root = build_scope(self.ast)
            for scope in root.traverse():
                if scope.is_cte:
                    
                    # Check if query contains any source table in from clause
                    has_base_table = _scope_has_base_table
                    
                    # If no base table, check for aggregations
                    has_agg_func = False
                    if not has_base_table:
                        for aggfunc in scope.find_all(exp.AggFunc):
                            if aggfunc:
                                has_agg_func = True
                        if not has_agg_func:
                            ctes_without_agg_expressions.append(scope.expression.sql())
        
        return ctes_without_agg_expressions

    def optimize_query(self) -> None:
        '''
        Uses the SQLGlot optimizer to re-write the query in an optimized form
        See: https://github.com/tobymao/sqlglot/tree/main?tab=readme-ov-file#sql-optimizer
        NOTE: This could have an impact on how your query engine behaves. Proceed with caution!
        '''
        optimized_sql = optimize(parse_one(self.query_string), dialect=self.dialect).sql(pretty=True)
        self.query_string = optimized_sql
        print('--- Query String Optimized! ---')

    def __str__(self):
        return f"{self.query_string}"
    
    def get_scope_types_count(self) -> dict:
        '''
        Generates a dictionary of scope types and number of times they are used in a query.
        Scope types can be CTE, ROOT, UNION, DERIVED_TABLE, CORRELATED_SUBQUERY and UDFs.
        These are helpful in figuring out query complexity.
        '''
        scope_occurences = dict()
        if self.is_valid:
            scope_list = []
            root = build_scope(self.ast)
            for scope in root.traverse():
                if scope.is_cte:
                    scope_list.append('CTE')
                if scope.is_root:
                    scope_list.append('ROOT')
                if scope.is_union:
                    scope_list.append('UNION')
                if scope.is_derived_table:
                    scope_list.append('DERIVED_TABLE')
                if scope.is_correlated_subquery:
                    scope_list.append('CORRELATED_SUBQUERY')
                if scope.is_udtf:
                    scope_list.append('USER_DEFINED_FUNCTION')
            
            scope_occurences = dict(Counter(scope_list))
        return scope_occurences