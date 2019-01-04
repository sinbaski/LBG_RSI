#!/usr/bin/bash

# rm *_column.txt

# for what in Tmax Tmin Tmean Sunshine Rainfall Raindays1mm AirFrost; do
#     tail -n +9 $what.txt | head -n -1 | while read line; do
# 	str=`echo "$line" | sed -E 's/ +/\t/g'`;
# 	year=`echo "$str" | cut -f 1`;
# 	values=(`echo "$str" | cut -f 2,3,4,5,6,7,8,9,10,11,12,13`);
# 	for ((i=0; i<12; i++)); do
# 	    ## printf "%s\t%d\t%s\n" "$year" "$(( $i + 1 ))" "${values[$i]}"
# 	    printf "%s-%d-1\t%s\n" "$year" "$(( $i + 1 ))" "${values[$i]}"
# 	done
#     done > ${what}_column.txt
# done

# for what in Tmin Tmean Sunshine Rainfall Raindays1mm AirFrost; do
#     echo "drop table if exists UK_$what;"
#     echo "create table UK_$what (\
#     	  D date not null primary key, \
#           val float\
#        	  );"
#     echo "load data local infile '${what}_column.txt' into table UK_$what columns terminated by '\\t';"
# done | mysql -u sinbaski -pq1w2e3r4 LBG

# for what in Tmax Tmin Tmean Sunshine Rainfall Raindays1mm AirFrost; do
#     values=(`tail -n 1 $what.txt | sed -E 's/ +/\t/g' | cut -f 2,3,4,5,6,7,8,9,10,11,12`);
#     for ((i=0; i<=10; i++)); do
# 	printf "2018-%d-1\t%s\n" "$(( $i + 1 ))" "${values[$i]}"
#     done > ${what}_column.txt;
#     echo "load data local infile '${what}_column.txt' into table UK_$what columns terminated by '\\t';"
# done | mysql -u sinbaski -pq1w2e3r4 LBG

