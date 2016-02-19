for i in $(seq 1 100)
do
    head --lines=${i}K WifiStatusTotal.csv > Lines_${i}K.csv;
done
