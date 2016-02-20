NUM=100;
PIGLATIN="GeelongWifiDiggerProv.pig";
LOGFILE="PigTestLog.log";
TIMEFILE="PigTestTimeProv.log";

for i in $(seq 1 ${NUM})
do
    sed -i 's/Lines_.*K[.]csv/Lines_'${i}K'.csv/g' $PIGLATIN;
    echo TestProv \#${i}: >> $TIMEFILE;
    pig -x local $PIGLATIN &> $LOGFILE;
    cat $LOGFILE | grep '(.*ms)' >> $TIMEFILE;
    echo TestProv \#${i} Done.
done

PIGLATIN="GeelongWifiDigger.pig";
TIMEFILE="PigTestTime.log";

for i in $(seq 1 ${NUM})
do
    sed -i 's/Lines_.*K[.]csv/Lines_'${i}K'.csv/g' $PIGLATIN;
    echo Test \#${i}: >> $TIMEFILE;
    pig -x local $PIGLATIN &> $LOGFILE;
    cat $LOGFILE | grep '(.*ms)' >> $TIMEFILE;
    echo Test \#${i} Done.
done
