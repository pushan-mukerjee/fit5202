#!/bin/bash

echo 'Pushan Mukerjee'
echo 'Student Number: 29052971'

#table_name="testtab"
#file_path="week3/testtab.csv"
#colFamily="cfam"

table_name="insurance"
file_path="week3/FL_insurance_sample.csv"
cf1="area_cf"
cf2="policy_cf"

#create table
echo "create '$table_name', '$cf1', '$cf2'" | hbase shell

#hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,$colFamily:Product,$colFamily:Price,$colFamily:Payment_Type,$colFamily:Name,$colFamily:City,$colFamily:State,$colFamily:Country,$colFamily:Account_Created,$colFamily:Last_Login,$colFamily:Latitude,$colFamily:Longitude '-Dimporttsv.separator=,' $table_name $file_path

#load table from csv file
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,$cf1:Statecode,$cf1:county,$cf1:point_latitude,$cf1:point_longitude,$cf2:eq_site_limit,$cf2:hu_site_limit,$cf2:fl_site_limit,$cf2:fr_site_limit,$cf2:tiv_2011,$cf2:tiv_2012,$cf2:eq_site_deductible,$cf2:hu_site_deductible,$cf2:fl_site_deductible,$cf2:fr_site_deductible,$cf2:line,$cf2:construction,$cf2:point_granularity '-Dimporttsv.separator=,' $table_name $file_path

#Count rows in table
echo "count '$table_name'" | hbase shell

