PIGLATIN="GeelongWifiDiggerProv.pig";
LOGFILE="PigTestLog.log";
TIMEFILE="PigTestTime.log";

for i in $(seq 1 5)
do
    #replace='s/GeelongWifiStatus.*[.]csv/WifiStatus_'${i}'.csv/g';
    replace='s/WifiStatus.*[.]csv/WifiStatusTotal.csv/g';
    sed -i $replace $PIGLATIN;
    time pig -x local $PIGLATIN &> $LOGFILE;
    echo Test \#${i} Done.
done
