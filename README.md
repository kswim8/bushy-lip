# DuckDB Benchmark Query Analysis

As a part of ongoing research to find new [LIP](https://jigneshpatel.org/publ/LIP.pdf) techniques, particularly for arbitrary query plans, this repo serves to analyze query plans in popular benchmarks for OLAP database systems, such as DuckDB.

## Join-Order Benchmark (Setup)
This has been (mostly) tested on Intel MacOS and Ubuntu.

1. download the tgz here: http://homepages.cwi.nl/~boncz/job/imdb.tgz
2. untar it, to get a folder called `imdb`
3. there is a file in the folder called `schematext.sql`, which will create the tables for us. run `cat schematext.sql | duckdb imdb.duckdb` where `imdb.duckdb` is the name of the persistent database for this data (it doesn't have to exist at time of running this command, duckdb will create it for you)... verify the tables exist when you run `duckdb` and `.tables` in duckdb
4. run `cat copyfromcsv.sql | duckdb imdb.duckdb` to copy the data from the csv to the duckdb tables we just created
5. verify by running `select count(*) from aka_name;` to make sure there is a nonzero number of rows
6. now that we have the database used for the join-order-benchmark, we can examine query plans for these queries
7. clone the join-order-benchmark repo here: https://github.com/gregrahn/join-order-benchmark
8. here is an example of how you can run the script for all 99 join-order-benchmark queries:

```
python script.py --db=~/imdb.duckdb --dir=~/join-order-benchmark/ --out=statistics_job.csv
```

## LDBC Social Network Benchmark, Business Intelligence (Setup)
This has been (mostly) tested on Intel MacOS.

1. follow the instructions here: https://github.com/duckdb/duckdb/tree/main/benchmark/ldbc
2. the queries exist in the `queries/` directory at the link above. to properly run the queries, you need to get rid of the comments in the sql files. i did this manually and put them in this repo in the folder `ldbc-snb-bi/` for ease of use
3. here is an example of how you can run the script for the existing ldbc-snb-bi queries:

```
python script.py --db=~/ldbc.duckdb --dir=~/ldbc-snb-bi/ --out=statistics_ldbc.csv
```
