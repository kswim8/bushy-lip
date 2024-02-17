# bushy-lip
adapting lookahead info passing (LIP) to bushy query plans

## phase 1
we want to examine some query plans in the join-order-benchmark (https://github.com/gregrahn/join-order-benchmark)
we are specifically looking for:
 - bushy query plans (join node with 2 join nodes as children)
 - large left child, large right child

### setup instructions
this has been (mostly) tested on Intel MacOS and Ubuntu
1. download the tgz here: http://homepages.cwi.nl/~boncz/job/imdb.tgz
2. untar it, to get a folder called `imdb`
3. there is a file in the folder called `schematext.sql`, which will create the tables for us. run `cat schematext.sql | duckdb imdb.duckdb` where `imdb.duckdb` is the name of the persistent database for this data (it doesn't have to exist at time of running this command, duckdb will create it for you)... verify the tables exist when you run `duckdb` and `.tables` in duckdb
4. run `cat copyfromcsv.sql | duckdb imdb.duckdb` to copy the data from the csv to the duckdb tables we just created
5. verify by running `select count(*) from aka_name;` to make sure there is a nonzero number of rows
6. now that we have the database used for the join-order-benchmark, we can examine query plans for these queries
7. clone the join-order-benchmark repo here: https://github.com/gregrahn/join-order-benchmark
8. you can run the script for all 99 join-order-benchmark queries by running `python script.py`
