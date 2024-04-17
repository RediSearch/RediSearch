#!/bin/bash

# for fieldtype in tag txt num geo geoshape;
for fieldtype in tagwithsuffixtrie txtwithsuffixtrie;
do 
    echo $fieldtype;

    # running tests
    let i=1;
    for testsize in 10000 30000 50000 70000 90000 110000 130000 150000 170000 190000 210000 230000 250000;
    do
        # echo $testsize;
        python3 test_idx.py $fieldtype $testsize 0 > /tmp/file_${fieldtype}_${i}.txt
        let i++
    done

    
    # generating csv
    cat /tmp/file_${fieldtype}_1.txt | tr ':' '\t' > /tmp/file_${fieldtype}_1_col1_col2.txt

    # generate column 1-2
    let max=$i-1
    for ((j=2; j<=max; j++));
    do
        awk -F ':' '{print $2}' /tmp/file_${fieldtype}_${j}.txt > /tmp/file_${fieldtype}_${j}_col2.txt
    done
    
    # generate column 3
    let j=2;
    paste \
        /tmp/file_${fieldtype}_1_col1_col2.txt \
        /tmp/file_${fieldtype}_${j}_col2.txt | tr '\t' ',' \
        > /tmp/out_${fieldtype}.csv

    # generate columns > 3
    for ((j=3; j<=$max; j++));
    do
        paste /tmp/out_${fieldtype}.csv /tmp/file_${fieldtype}_${j}_col2.txt \
        | tr '\t' ',' > /tmp/out.csv
        mv /tmp/out.csv /tmp/out_${fieldtype}.csv
    done

    # clean up
    rm /tmp/file_${fieldtype}_*.txt

    # print results
    tail /tmp/out_${fieldtype}.csv | grep \/delta_used
done
