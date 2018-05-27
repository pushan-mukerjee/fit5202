#!/bin/bash
echo "Pushan Mukerjee"
echo "29052971"

echo "# 1. Complete this to create a table called <student_number> using HBase Shell commands.
# write your code here:

#checks if table exists
echo "exists '29052971'" | hbase shell

#create table 29052971
echo "create '29052971', 'cf1'" | hbase shell

#describe table 
echo "describe '29052971'" | hbase shell

# add comments when necessary (in HBase start comment line using hash sign)
#" | hbase shell

echo "2. Complete the following to import the file from HDFS and populate your table."
# write your code here:
# hbase org.apache.hadoop.hbase.mapreduce.ImportTsv ...
# add comments when necessary (use an echo command)

#variable representing column family, table_name and file path
cf1="cf1"
table_name="29052971"
file_path="/tmp/flights100.tsv - flights100.tsv.csv"

#load table from csv file
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,$cf1:Statecode,$cf1:county,$cf1:point_latitude,$cf1:point_longitude,$cf2:eq_site_limit,$cf2:hu_site_limit,$cf2:fl_site_limit,$cf2:fr_site_limit,$cf2:tiv_2011,$cf2:tiv_2012,$cf2:eq_site_deductible,$cf2:hu_site_deductible,$cf2:fl_site_deductible,$cf2:fr_site_deductible,$cf2:line,$cf2:construction,$cf2:point_granularity '-Dimporttsv.separator=,' $table_name $file_path

echo "# Complete this to (remove the hash symbols for command lines but include them for comment lines):
# 3. Print the number of rows in the table.
# count rows in the 29052971 table...
echo "count '$table_name'" | hbase shell

# 4. Add a flight from Melbourne to Sydney with 1 hour departure delay.
echo "put '$table_name', '101', '$cf1:dep_delay', '1'" | hbase shell
echo "put '$table_name', '101', '$cf1:origin', 'MEL'" | hbase shell
echo "put '$table_name', '101', '$cf1:dest', 'SYD1'" | hbase shell

# 5. Print the added flight.
echo "get '$table_name', '101'" | hbase shell

# 6. Print all flights with exactly 1 hour departure delay.
filter="SingleColumnValueFilter('$cf1','dep_delay',=,'binary:1')"
echo "scan '$table_name', {COLUMNS => ['$cf1:dep_delay'], FILTER => \"$filter\"}" | hbase shell

# scan  ...
# 7. Remove the flight between Melbourne and Sydney.
echo "deleteall '$table_name', '101'" | hbase shell

# 8. Print 10 flights which departed before the scheduled time.
# scan ...
# 9. Print 10 flights between JFK and LAX airports.
filter="SingleColumnValueFilter('$cf1','dep_delay',=,'binary:1')"
echo "scan '$table_name', {COLUMNS => ['$cf1:dep_delay'], FILTER => \"$filter\"}" | hbase shell

# 10. Find the number of flights between JFK and LAX airports in Jan and July 2013 were delayed for at least half an hour but arrived to their destination(s) before the scheduled time?
# ...
# 11. Drop the created table(s).
echo "disable '$table_name'" | hbase shell
echo "drop '$table_name'" | hbase shell
# ...
# exists '<student_number>'
quit
" | hbase shell
