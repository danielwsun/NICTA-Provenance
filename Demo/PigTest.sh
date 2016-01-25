PIGLATIN="GeelongWifiDiggerProv.pig";

for i in $(seq 1 1)
do
    replace='s/GeelongWifiStats.*[.]csv/GeelongWifiStatus_'${i}'.csv/g';
    sed -i $replace $PIGLATIN;
    pig -x local $PIGLATIN;
done
