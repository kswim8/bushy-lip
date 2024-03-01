import argparse
from pty import CHILD
import duckdb
import json
import os
import pandas as pd
import sys

EXPLAIN_ANALYZE_PREFIX = "EXPLAIN ANALYZE "
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
Checks if a plan node is UNION type.
'''
def is_union_node(curr_node):
    return len(curr_node[CHILDREN]) == 2 and "UNION" == curr_node[NAME]


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
Finds the highest binary node in this subtree.
Returns a node and whether it is binary (has 2 children).
'''
def get_first_binary_node(curr_plan_node):
    children = curr_plan_node[CHILDREN]
    while len(children) < 2:
        if not children:
            return curr_plan_node, False
        curr_plan_node = children[0]
        children = curr_plan_node[CHILDREN]
    return curr_plan_node, True


'''
Assume curr_plan_node is a binary node (has 2 children).
Chases left and right branches to get immediate/local children sizes.
'''
def get_local_children_size(curr_plan_node, data, file_name):
    children = curr_plan_node[CHILDREN]
    if len(children) != 2:
        return

    L_child, L_is_binary = get_first_binary_node(children[0])
    R_child, R_is_binary = get_first_binary_node(children[1])
    
    L_size = L_child[CARDINALITY]
    R_size = R_child[CARDINALITY]
    curr_size = curr_plan_node[CARDINALITY]
    
    if is_join_node(curr_plan_node):
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

    if L_is_binary:
        get_local_children_size(L_child, data, file_name)
    if R_is_binary:
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
    
    biggest_depth = 0
    for child in children:
        biggest_depth = max(biggest_depth, set_join_node_depths(child))

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
Modify the JSON object for current plan node if it is a UNION.
As of writing this, DuckDB has output cardinality as 0 for UNION nodes.
This makes it difficult to get accurate calculations.
'''
def set_union_cardinality(curr_plan_node):
    children = curr_plan_node[CHILDREN]
    if not children:
        return

    for child in children:
        set_union_cardinality(child)

    if is_union_node(curr_plan_node):
        total_size = 0
        for child in children:
            total_size += child[CARDINALITY]

        curr_plan_node[CARDINALITY] = total_size


'''
Search for other plan nodes with 2 children to assess edge cases.
Not meant to be used for actual script runs, only 1 time use.
'''
def find_other_2_children_nodes(curr_plan_node):
    children = curr_plan_node[CHILDREN]
    if not children:
        return
    
    if len(children) > 1 and not is_join_node(curr_plan_node):
        print(curr_plan_node[NAME], len(children), curr_plan_node[CARDINALITY])

    for child in children:
        find_other_2_children_nodes(child)

    return


'''
Takes in a .sql file + database connection.
Applies EXPLAIN ANALYZE to grab the JSON query plan.
Based on the query plan, we gather desired data to be placed in data.
'''
def get_data(file, data, con):
    file_name = file.name
    query_plan = get_json_from_file(file, con)
    set_union_cardinality(query_plan)
    set_join_node_depths(query_plan)
    set_leaf_children_size(query_plan)
    top_binary_node, is_binary = get_first_binary_node(query_plan)
    if is_binary:
        get_local_children_size(top_binary_node, data, file_name)


def main():
    parser = argparse.ArgumentParser(description='Analyzes DuckDB query plans')

    parser.add_argument('--db', type=str, help='Name of database file')
    parser.add_argument('--dir', type=str, help='Directory with sql queries')
    parser.add_argument('--out', type=str, help='Name of data output file')
    
    args = parser.parse_args()
    database_name = args.db
    directory = args.dir
    out_file = args.out

    # Connect to our database
    con = duckdb.connect(database=database_name, read_only=True)
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
    directory = os.path.expanduser(directory)
    for file_name in sorted(os.listdir(directory)):
        file_path = os.path.join(directory, file_name)
            
        # Only open and operate on .sql files
        if os.path.isfile(file_path) and \
           file_name.endswith(".sql"):
            with open(file_path, 'r') as file:
                print("Processing file...", file_name, end=" ")
                get_data(file, data, con)
            print("Done!")
    
    # Write out our data to file on disk
    df = pd.DataFrame(data)
    df.to_csv(out_file, index=False)

if __name__ == '__main__':
    main()
