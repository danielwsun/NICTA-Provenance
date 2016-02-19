PIGLATIN="GeelongWifiDiggerProv.pig";
LOGFILE="PigTestLog.log";
TIMEFILE="PigTestTime.log";

for i in $(seq 1 3)
do
    sed -i 's/Lines_.*K[.]csv/Lines_'${i}K'.csv/g' $PIGLATIN;
    echo Test \#${i}: >> $TIMEFILE;
    pig -x local $PIGLATIN &> $LOGFILE;
    cat $LOGFILE | grep '(.*ms)' >> $TIMEFILE;
    echo Test \#${i} Done.
done

PIGLATIN="GeelongWifiDigger.pig";
