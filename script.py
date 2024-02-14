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
EXTRA_INFO = "extra_info"

QUERY_NAME = "query_name"
NODE_INFO = "node_info"
DEPTH = "depth"
IS_BUSHY = "is_bushy"

LEFT_LEAF_SIZE = "x"
RIGHT_LEAF_SIZE = "y"
LEFT_LOCAL_SIZE = "x'"
RIGHT_LOCAL_SIZE = "y'"
CURR_SIZE = "z"

LEAF_SIZE = "leaf_size"


'''
Checks if a plan node is JOIN type.
Explicitly ignores SEMI JOIN.
'''
def is_join_node(curr_node):
    return len(curr_node[CHILDREN]) == 2 and \
           "JOIN" in curr_node[NAME] and \
           "SEMI" not in curr_node[EXTRA_INFO]


'''
Checks if a plan node is a BUSHY JOIN.
'''
def is_bushy_join_node(curr_node):
    children = curr_node[CHILDREN]
    return is_join_node(curr_node) and \
           is_join_node(children[0]) and \
           is_join_node(children[1])


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
def get_first_join_node(curr_plan_node):
    while "JOIN" not in curr_plan_node[NAME]:
        children = curr_plan_node[CHILDREN]
        if not children:
            return curr_plan_node, False
        curr_plan_node = children[0]
    return curr_plan_node, True


'''
Assume curr_plan_node is a JOIN node.
Chases left and right branches to get immediate/local children sizes.
'''
def get_local_children_size(curr_plan_node, data, file_name):
    assert("JOIN" in curr_plan_node[NAME])
    children = curr_plan_node[CHILDREN]
    if len(children) != 2:
        return

    L_child, L_is_join = get_first_join_node(children[0])
    R_child, R_is_join = get_first_join_node(children[1])
    
    L_size = L_child[CARDINALITY]
    R_size = R_child[CARDINALITY]
    curr_size = curr_plan_node[CARDINALITY]
    
    data[QUERY_NAME].append(file_name)
    data[NODE_INFO].append(curr_plan_node[NAME])
    data[DEPTH].append(curr_plan_node[DEPTH])
    data[IS_BUSHY].append(is_bushy_join_node(curr_plan_node))
    data[LEFT_LEAF_SIZE].append(children[0][LEAF_SIZE])
    data[RIGHT_LEAF_SIZE].append(children[1][LEAF_SIZE])
    data[LEFT_LOCAL_SIZE].append(L_size)
    data[RIGHT_LOCAL_SIZE].append(R_size)
    data[CURR_SIZE].append(curr_size)
    data[EXTRA_INFO].append(curr_plan_node[EXTRA_INFO])

    if L_is_join:
        get_local_children_size(L_child, data, file_name)
    if R_is_join:
        get_local_children_size(R_child, data, file_name)


'''
Modify the JSON object for current plan node to add depth calculation.
This depth calculation is specific for JOINs in the plan.
'''
def set_join_node_depths(curr_plan_node):
    children = curr_plan_node[CHILDREN]
    if not children:
        curr_plan_node[DEPTH] = 0
        return 0
    
    biggest_depth = set_join_node_depths(children[0])
    if len(children) == 2:
        biggest_depth = max(biggest_depth, set_join_node_depths(children[1]))

    if is_join_node(curr_plan_node):
        biggest_depth += 1
    
    curr_plan_node[DEPTH] = biggest_depth

    return biggest_depth


'''
Modify the JSON object for current plan node to add leaf children size.
This leaf children size is the sum of output cardinalities of leaves in subtree.
'''
def set_leaf_children_size(curr_plan_node):
    children = curr_plan_node[CHILDREN]
    if not children:
        leaf_size = curr_plan_node[CARDINALITY]
        curr_plan_node[LEAF_SIZE] = leaf_size
        return leaf_size

    leaf_children_size = 0
    for child in children:
        leaf_children_size += set_leaf_children_size(child)

    curr_plan_node[LEAF_SIZE] = leaf_children_size
    return leaf_children_size


'''
Takes in a .sql file + database connection.
Applies EXPLAIN ANALYZE to grab the JSON query plan.
Based on the query plan, we gather desired data to be placed in data.
'''
def get_data(file, data, con):
    file_name = file.name
    query_plan = get_json_from_file(file, con)
    set_join_node_depths(query_plan)
    set_leaf_children_size(query_plan)
    top_join_node, is_join = get_first_join_node(query_plan)
    if is_join:
        get_local_children_size(top_join_node, data, file_name)


def main():
    # Connect to our database
    con = duckdb.connect(database=DATABASE_NAME, read_only=True)
    con.execute(PROFILING_AS_JSON_COMMAND)
    
    # This dictionary will be used to create a pandas DataFrame later
    data = {
        QUERY_NAME: [],
        NODE_INFO: [],
        DEPTH: [],
        IS_BUSHY: [],
        LEFT_LEAF_SIZE: [],
        RIGHT_LEAF_SIZE: [],
        LEFT_LOCAL_SIZE: [],
        RIGHT_LOCAL_SIZE: [],
        CURR_SIZE: [],
        EXTRA_INFO: [],
    }

    # Iterate over all files in DIRECTORY
    for file_name in sorted(os.listdir(DIRECTORY)):
        file_path = os.path.join(DIRECTORY, file_name)
            
        # Only open and operate on .sql files
        if os.path.isfile(file_path) and \
           file_name.endswith(".sql") and \
           file_name[0].isdigit():
            with open(file_path, 'r') as file:
                print("Processing file...", file_name, end=" ")
                get_data(file, data, con)
            print("Done!")
    
    # Write out our data to file on disk
    df = pd.DataFrame(data)
    df.to_csv(OUT_FILE)

if __name__ == '__main__':
    main()
