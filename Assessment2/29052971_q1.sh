#!/bin/bash
echo "Student Full Name"
echo "Student Number"

echo "1. Create a folder on the HDFS and name it after your student number: /tmp/<student_number>"
hadoop fs -mkdir /tmp/29052971

#write your code here (uncomment the line!). For example:

# add comments using echo command: 
echo "Listing the contents of /tmp/ directory showing created 29052971 folder!"
hadoop fs -ls -R /tmp/

echo "2. Listing all the files in the /tmp/29052971/ folder."
hadoop fs -ls -R /tmp/29052971/

echo "3. Transfer the above file to the HDFS folder that you just created (use absolute paths)."
hadoop fs -put /tmp/29052971.txt /tmp/29052971/

#...
echo "4. Print First 10 lines of the the transferred file 29052971.txt which is now stored on HDFS."
hadoop fs -cat /tmp/29052971/29052971.txt | head -10

echo "
The End!
"
