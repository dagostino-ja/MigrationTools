import sqlparse
from sqlparse.sql import Identifier

def extract_subquery(temp_table_sql):
    start_idx = temp_table_sql.lower().find('select')
    return f"({temp_table_sql[start_idx:].strip()})" if start_idx != -1 else None

def replace_temp_table_with_subquery(sql_code, temp_tables):
    parsed = sqlparse.parse(sql_code)
    modified_sql = ''
    
    for statement in parsed:
        tokens = statement.tokens
        for token in tokens:
            if isinstance(token, Identifier):
                temp_table_name = token.get_real_name()
                if temp_table_name in temp_tables:
                    modified_sql += f"{temp_tables[temp_table_name]} AS {temp_table_name}"
                else:
                    modified_sql += token.value
            else:
                modified_sql += token.value
    return modified_sql

def resolve_temp_table_references(temp_table_name, temp_tables, resolved=None):
    if resolved is None:
        resolved = set()
    
    # Prevent infinite recursion by skipping already resolved tables
    if temp_table_name in resolved:
        return temp_tables[temp_table_name]
    
    subquery = temp_tables.get(temp_table_name)
    if not subquery:
        return None
    
    resolved.add(temp_table_name)  # Mark this temp table as resolved
    
    # Check if the subquery references any other temp tables and replace them
    for ref_table in temp_tables:
        if ref_table in subquery and ref_table not in resolved:
            subquery = subquery.replace(ref_table, resolve_temp_table_references(ref_table, temp_tables, resolved))
    
    return subquery

def convert_temp_tables_to_subqueries(sql_code):
    temp_tables = {}
    parsed = sqlparse.parse(sql_code)
    result_sql = sql_code
    
    for statement in parsed:
        if 'CREATE' in statement.value.upper() or 'INSERT INTO' in statement.value.upper():
            for token in statement.tokens:
                if isinstance(token, Identifier) and '#' in token.get_real_name():
                    temp_table_name = token.get_real_name()
                    subquery = extract_subquery(statement.value)
                    if subquery:
                        temp_tables[temp_table_name] = subquery
    
    for temp_table_name in temp_tables:
        temp_tables[temp_table_name] = resolve_temp_table_references(temp_table_name, temp_tables)
    
    result_sql = replace_temp_table_with_subquery(result_sql, temp_tables)
    return result_sql

sql_code = """
CREATE TEMPORARY TABLE #temp_table1 AS 
SELECT col1, col2 FROM some_table WHERE col3 = 'value';

CREATE TEMPORARY TABLE #temp_table2 AS 
SELECT col1 FROM #temp_table1 WHERE col2 = 'another_value';

CREATE TEMPORARY TABLE #temp_table3 AS 
SELECT col1 FROM #temp_table2 WHERE col1 = 'final_value';

SELECT col1 FROM #temp_table3 WHERE col1 = 'result_value';
"""

converted_sql = convert_temp_tables_to_subqueries(sql_code)
print(converted_sql)
