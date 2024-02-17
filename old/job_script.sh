#!/bin/bash

directory_path="join-order-benchmark"
py_script_name="json_parse_script.py"

for file in "$directory_path"/*; do
	if [[ -f "$file" && $file == *.sql ]]; then
		python $py_script_name "$file"
	fi
done
