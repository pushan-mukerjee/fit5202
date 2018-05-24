#!/bin/bash

echo 'Pushan Mukerjee'
echo 'Student Number: 29052971'

#table_name="testtab"
#file_path="week3/testtab.csv"
#colFamily="cfam"

table_name="sales"
file_path="week3/SalesJan2009.csv"
colFamily="sales_cf"

echo "create '$table_name', '$colFamily'" | hbase shell

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,$colFamily:Product,$colFamily:Price,$colFamily:Payment_Type,$colFamily:Name,$colFamily:City,$colFamily:State,$colFamily:Country,$colFamily:Account_Created,$colFamily:Last_Login,$colFamily:Latitude,$colFamily:Longitude '-Dimporttsv.separator=,' $table_name $file_path

echo "scan '$table_name'" | hbase shell
