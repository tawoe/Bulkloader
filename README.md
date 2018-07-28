# bulkloader

Recommended method to import the data is the bulkloader now. It is gazillion
times faster than the old loader, importing all the data within a few hours,
not days!

Just call it by:

`./bulkloader.py`

Or if you want to measure the execution time:

`time ./bulkloader.py`

Each data file is put into its own index, so we can wipe and load them
individually. It is rather difficult to delete types. Data in each index is
stored in type 'default'.

Logfiles will be created in `logs/` in the format
`<index>.<datetime>.bulkloader.log`.

See the constants in the script file for configurable values, like `LOG_DIR`,
`DATA_DIR`, `INDEX_PREFIX` or `BULK_SIZE`.

You can control which data files to import by editing RUNNER_CONFIG



## stats

A little script to calculate statistics which are difficult to obtain using SQL only. You need to configure the database settings and dates. You probably also want to remove the comments of the different functions to be run in the __main__ section. To run, simple issue:

`./stats.py`