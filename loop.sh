#!/bin/bash
test=$1
file=$2
total=$3
echo "execute $test for $total times, write log to file $file"

cd raft
echo "" &> $file

for ((i=1; i<=total; i++))
do
echo "loops: $i/$total" >> $file.shout

go test -race -run $test &> $file
if [[ $? -ne 0 ]]; then
  echo "Error, check file $file for details." && exit 1
fi
echo "Success" >> $file.shout

done
