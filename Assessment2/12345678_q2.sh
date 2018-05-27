#!/bin/bash
echo "Student Full Name"
echo "Student Number"

echo "# 1. Complete this to create a table called <student_number> using HBase Shell commands.
# write your code here:
exists '<student_number>'
create '<student_number>',...
describe '<student_number>'
# add comments when necessary (in HBase start comment line using hash sign)
" | hbase shell

echo "2. Complete the following to import the file from HDFS and populate your table."
# write your code here:
# hbase org.apache.hadoop.hbase.mapreduce.ImportTsv ...
# add comments when necessary (use an echo command)

echo "# Complete this to (remove the hash symbols for command lines but include them for comment lines):
# 3. Print the number of rows in the table.
# ...
# 4. Add a flight from Melbourne to Sydney with 1 hour departure delay.
# put ...
# 5. Print the added flight.
# get ...
# 6. Print all flights with exactly 1 hour departure delay.
# scan ...
# 7. Remove the flight between Melbourne and Sydney.
# ...
# 8. Print 10 flights which departed before the scheduled time.
# scan ...
# 9. Print 10 flights between JFK and LAX airports.
# scan ...
# 10. Find the number of flights between JFK and LAX airports in Jan and July 2013 were delayed for at least half an hour but arrived to their destination(s) before the scheduled time?
# ...
# 11. Drop the created table(s).
# ...
# exists '<student_number>'
quit
" | hbase shell