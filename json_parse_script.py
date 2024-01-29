
import duckdb
import json
import sys

PROFILING_AS_JSON_COMMAND = "PRAGMA enable_profiling = 'json'"
EXPLAIN_ANALYZE_PREFIX = "EXPLAIN ANALYZE "
DATABASE_NAME = "imdb.duckdb"
OUT_FILE = "statistics.txt"
OUT_FILE_ROW_FORMAT = "{:<30} {:<10} {:<8} {:<8} {:<12} {:<12}\n"
FLOAT_FORMAT = ".2f"

CARDINALITY = "cardinality"
CHILDREN = "children"
NAME = "name"

def get_sql_query(query_file_name):
    query = ""
    with open(query_file_name, 'r') as f:
        for line in f:
            query += " " + line.strip()
    return query

def get_sql_query_w_explain_analyze(query_file_name):
    return EXPLAIN_ANALYZE_PREFIX + get_sql_query(query_file_name)

def is_bushy(curr_plan_node):
    children = curr_plan_node[CHILDREN]
    return len(children) == 2 and \
           "JOIN" in curr_plan_node[NAME] and \
           "JOIN" in children[0][NAME] and \
           "JOIN" in children[1][NAME]

def is_join(curr_plan_node):
    children = curr_plan_node[CHILDREN]
    return len(children) == 2 and \
           "JOIN" in curr_plan_node[NAME]

def get_children_leaf_size(curr_plan_node, query_file, out_file, depth=0):
    # BC: we hit a leaf node
    children = curr_plan_node[CHILDREN]
    if not children:
        return curr_plan_node[CARDINALITY]
    
    # IS: get size of children
    if is_join(curr_plan_node):
        x = get_children_leaf_size(children[0], query_file, out_file, depth + 1)
        y = get_children_leaf_size(children[1], query_file, out_file, depth + 1)
        z = curr_plan_node[CARDINALITY]
        
        node_name = curr_plan_node[NAME]
        bushy = "Y" if is_bushy(curr_plan_node) else ""
        children_to_parent_ratio = "----" if not z else \
                                          format((x + y) / z, FLOAT_FORMAT)
        children_ratio = "----" if not min(x, y) else \
                                format(max(x, y) / min(x, y), FLOAT_FORMAT)
        row = OUT_FILE_ROW_FORMAT.format(
            query_file, 
            node_name,
            depth,
            bushy,
            children_to_parent_ratio,
            children_ratio,
        )
        out_file.write(row)
        return x + y

    total_children = 0
    for child in children:
        total_children += get_children_leaf_size(
                child, 
                query_file, 
                out_file, 
                depth + 1
        )
    
    return total_children

def main():
    con = duckdb.connect(database=DATABASE_NAME, read_only=True)
    con.execute(PROFILING_AS_JSON_COMMAND)
    
    query_file = sys.argv[1]
    print("*" * 20, query_file, "", "*" * 20)

    explain_analyze_query = get_sql_query_w_explain_analyze(query_file)
    query_plan_json_str = con.sql(explain_analyze_query).fetchall()[0][1]
    query_plan_json = json.loads(query_plan_json_str)
    
    with open(OUT_FILE, 'a+') as out_file:
        if len(out_file.read()) == 0:
            title_row = OUT_FILE_ROW_FORMAT.format(
                "Query",
                "Node Name",
                "Depth",
                "Bushy?",
                "[(x+y)/z]",
                "[x/y]",
            )
            out_file.write(title_row)
        
        get_children_leaf_size(query_plan_json, query_file, out_file)

        out_file.write("\n")

if __name__ == '__main__':
    main() 
