#!/usr/bin/bash

D="2019-12-29"

lengths=(4 4 5 4 4 5 4 4 5 4 4 5);
m=1;
echo "insert into uk_rsi_standard_reporting_period values "
for i in ${lengths[*]}; do
    if [ $m -lt 12 ]; then
	printf "(\'2020-%02d-01\', \'$D\')," $m
    else
	printf "(\'2020-%02d-01\', \'$D\');" $m
    fi
    D=`date -d "$D + $i weeks" +"%Y-%m-%d"`
    let "m = m + 1"
done
