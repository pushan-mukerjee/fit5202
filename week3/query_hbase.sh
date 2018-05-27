#!/bin/bash

echo 'Pushan Mukerjee'
echo 'Student Number: 29052971'

table="insurance"
file_path="week3/FL_insurance_sample.csv"
cf1="policy_cf"
cf2="area_cf"

#imported packages
#echo "import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter" | hbase shell
#echo "import org.apache.hadoop.hbase.filter.SingleColumnValueFilter" | hbase shell

#scan 'sales', {FILTER => "PrefixFilter('1/20/09') AND ValueFilter(=, 'binary:Alberta')"}

#counting rows
echo "count 'insurance'" | hbase shell

#Adding rows
echo "put 'insurance', '100200', '$cf2:statecode', 'NJ'" | hbase shell
echo "put 'insurance', '100200', '$cf2:county', 'Bergen'" | hbase shell
echo "put 'insurance', '100200', '$cf1:eq_site_limit', '500000'" | hbase shell

#echo "put '$table_name', '23/05/2018 12:29', '$colFamily:Last_Login', '20/12/2017 12:29'" | hbase shell
#echo "put '$table_name', '23/05/2018 12:29', '$colFamily:Latitude', '50.12'" | hbase shell
#echo "put '$table_name', '23/05/2018 12:29', '$colFamily:Longitude', '100.12'" | hbase shell

#viewing rows
echo "get 'insurance', '100200'" | hbase shell

#Updating rows
echo "put 'insurance', '100200', 'area_cf:county', 'Paramus'" | hbase shell

#viewing updates
echo "get 'insurance', '100200'" | hbase shell

#deleting rows
#echo "delete '$table_name', '23/05/2018 12:29', '$colFamily:Longitude'" | hbase shell
echo "delete 'insurance', '100200', 'policy_cf:eq_site_limit'" | hbase shell
echo "deleteall 'insurance', '100200'" | hbase shell

#Filters 
filter1="PrefixFilter('1/20/09')"
filter2="ValueFilter(=, 'binary:705600')"
filter3="ValueFilter(>=, 'binary:14000')"
filter4="org.apache.hadoop.hbase.filter.SingleColumnValueFilter(Bytes.toBytes('area_cf'),Bytes.toBytes('statecode'),CompareFilter::CompareOp.valueOf('EQUAL'),Bytes.toBytes('FL'))"
filter5="ValueFilter(!=, 'binary:Sicilia')"
filter6="ValueFilter(=, 'binary:FL')"
filter7="RowFilter(>=, '1/14/09')"
filter8="RowFilter(<=, '1/18/09')"
filter9="QualifierFilter(=, 'statecode')"
filter10="SingleColumnValueFilter('$cf1','point_granularity',>,'binary:1')"
filter11="SingleColumnValueFilter('$cf1','construction',=,'substring:Masonry')"
filter12="SingleColumnValueFilter('$cf1','construction',=,'substring:Wood')"

#Filter Queries
#echo "scan '$table', {COLUMNS => ['$cf:Price', '$cf:State'], STARTROW => '1/14/09', ENDROW => '1/18/09', FILTER => \"$filter4 AND $filter5\"}" | hbase shell
echo "scan '$table', {COLUMNS => ['$cf1:point_granularity','$cf1:construction'], FILTER => \"$filter10 AND ($filter11 OR $filter12)\"}" | hbase shell
