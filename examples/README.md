# okhbase examples
Simple examples to illustrate using the api to do basic work. 

## Usage

### Create table and Write Data

First we create a table, and put a bunch of data in it.
This command will crate a table named 'ok_hbase_test', withs a single column family: 'd'.
It then sequentially creates rows with row keys a through zzz, 18278 rows in total.
That may take a few minutes, as we are not batching the writes.

```bash
$ ./table_write.rb  --host localhost --port 9090 --table ok_hbase_test
```

### Read Data
Now that we have daa in the table, we want to read it.
The following script will scan the table we just made for any rows that begin with 'hba'

```bash
$ ./table_scan.rb  --host localhost --table ok_hbase_test --prefix hba
```

The output should look like:
```bash
Nathans-MacBook-Pro-2:ok_hbase nkeyes$ ./examples/table_scan.rb  --host localhost --table ok_hbase_test --prefix hba
2013-06-11 08:36:13 -0700 DEBUG: Setting up connection
2013-06-11 08:36:13 -0700 DEBUG: Connecting to localhost
2013-06-11 08:36:13 -0700 DEBUG: Get instance for table ok_hbase_test
{
    "hba" => {
           "d:message" => "this is row number 5461",
        "d:row_number" => "5461"
    }
}
Nathans-MacBook-Pro-2:ok_hbase nkeyes$
```

Experiment with shorter prefixes to see more rows returned.
An empty prefix will return all rows:

```bash
./table_scan.rb  --host localhost --table ok_hbase_test --prefix ''
```

