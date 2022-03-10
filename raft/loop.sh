#!/bin/bash
test=$1
file=$2
total=100
echo "execute $test for $total times, write log to file $file"

for ((i=1; i<=total; i++))
do
echo "loops: $i/$total"

go test -race -run $test &> $file
if [[ $? -ne 0 ]]; then
  echo "Error, check log file for detail." && exit 1
fi
echo "Success"

done
