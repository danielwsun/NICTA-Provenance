declare -i n=1;
for i in $(seq 50 5 100)
do
    cat WifiStatusTotal.csv | head --lines=${i}K | tail --lines=50K > WifiStatus50K_${n}.csv
    n=$((n+1));
    echo $n Done.
done
