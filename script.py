import duckdb
import json
import os
import pandas as pd
import sys

DATABASE_NAME = "imdb.duckdb"
DIRECTORY = "join-order-benchmark/"
EXPLAIN_ANALYZE_PREFIX = "EXPLAIN ANALYZE "
OUT_FILE = "statistics.csv"
PROFILING_AS_JSON_COMMAND = "PRAGMA enable_profiling = 'json'"

CARDINALITY = "cardinality"
CHILDREN = "children"
NAME = "name"

QUERY_NAME = "query_name"
NODE_NAME = "node_name"
DEPTH = "depth"
IS_BUSHY = "is_bushy"
LEAF_TO_PARENT_RATIO = "(x+y)/z"
LEAF_RATIO = "x/y"
LOCAL_LEAF_TO_PARENT_RATIO = "(x'+y')/z"
LOCAL_LEAF_RATIO = "x'/y'"

'''
Takes in a .sql file + database connection.
Applies EXPLAIN ANALYZE to grab the JSON query plan.
'''
def get_json_from_file(file, con):
    query = EXPLAIN_ANALYZE_PREFIX
    for line in file:
        query += " " + line.strip()
    query_plan = con.sql(query).fetchall()[0][1]
    return json.loads(query_plan)

'''
Takes in a curr_plan_node as root.
Finds the highest join node in this subtree.
Returns a node, the depth of the node, and whether it is a JOIN node.
'''
def get_first_join_node(curr_plan_node, depth):
    # Iterate until we hit a node that is JOIN
    while "JOIN" not in curr_plan_node[NAME]:
        children = curr_plan_node[CHILDREN]
        # If at a leaf node, return it anyways
        if not children:
            return curr_plan_node, depth, False
        curr_plan_node = children[0]
        depth += 1
    return curr_plan_node, depth, True

'''
Assume curr_plan_node is a JOIN node. 
'''
def get_local_children_size(curr_plan_node, depth, data, file_name):
    assert("JOIN" in curr_plan_node[NAME])
    children = curr_plan_node[CHILDREN]
    if len(children) != 2:
        return

    L_child, L_depth, L_is_join = get_first_join_node(children[0], depth + 1)
    R_child, R_depth, R_is_join = get_first_join_node(children[1], depth + 1)
    
    L_size = L_child[CARDINALITY]
    R_size = R_child[CARDINALITY]
    curr_size = curr_plan_node[CARDINALITY]

    data[QUERY_NAME].append(file_name)
    data[NODE_NAME].append(curr_plan_node[NAME])
    data[DEPTH].append(depth)
    data[IS_BUSHY].append(False)
    data[LEAF_TO_PARENT_RATIO].append(0)
    data[LEAF_RATIO].append(0)
    data[LOCAL_LEAF_TO_PARENT_RATIO].append(pd.NA if not curr_size else (L_size + R_size) / curr_size) 
    data[LOCAL_LEAF_RATIO].append(pd.NA if not min(L_size, R_size) else max(L_size, R_size) / min(L_size, R_size))

    if L_is_join:
        get_local_children_size(L_child, L_depth, data, file_name)
    if R_is_join:
        get_local_children_size(R_child, R_depth, data, file_name)


'''
Takes in a .sql file + database connection.
Applies EXPLAIN ANALYZE to grab the JSON query plan.
Based on the query plan, we gather desired data to be placed in data.
'''
def get_data(file, data, con):
    file_name = file.name
    query_plan = get_json_from_file(file, con)
    top_join_node, top_join_depth, is_join = get_first_join_node(query_plan, 0)
    if is_join:
        get_local_children_size(top_join_node, top_join_depth, data, file_name)


def main():
    # Connect to our database
    con = duckdb.connect(database=DATABASE_NAME, read_only=True)
    con.execute(PROFILING_AS_JSON_COMMAND)
    
    # This dictionary will be used to create a pandas DataFrame later
    data = {
        QUERY_NAME: [],
        NODE_NAME: [],
        DEPTH: [],
        IS_BUSHY: [],
        LEAF_TO_PARENT_RATIO: [],
        LEAF_RATIO: [],
        LOCAL_LEAF_TO_PARENT_RATIO: [],
        LOCAL_LEAF_RATIO: [],
    }

    # Iterate over all files in DIRECTORY
    for file_name in sorted(os.listdir(DIRECTORY)):
        file_path = os.path.join(DIRECTORY, file_name)
            
        # Only open and operate on .sql files
        if os.path.isfile(file_path) and \
           file_name.endswith(".sql") and \
           file_name[0].isdigit():
            with open(file_path, 'r') as file:
                print("Processing file...", file_name)
                get_data(file, data, con)
            print("Done!")
    
    # Write out our data to file on disk
    df = pd.DataFrame(data)
    df.to_csv(OUT_FILE)

if __name__ == '__main__':
    main()
