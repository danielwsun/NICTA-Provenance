NUM=2;
PIGLATIN="RankingProv.pig";
LOGFILE="PigTestLog.txt";
TIMEFILE="PigTestTimeProv.txt";
SCOREFILE="PigTestScoreProv.txt";

echo "Start" > $TIMEFILE;
echo "Start" > $SCOREFILE;

for i in $(seq 1 ${NUM})
do
    sed -i 's/WifiStatus.*[.]csv/WifiStatus_'${i}'.csv/g' $PIGLATIN;
    echo TestProv \#${i}: >> $TIMEFILE;
    pig -x local $PIGLATIN &> $LOGFILE;
    cat $LOGFILE | grep '(.*ms)' >> $TIMEFILE;
    echo TestProv \#${i}: >> $SCOREFILE;
    curl -XGET localhost:8888/_semantics >> $SCOREFILE;
    echo TestProv \#${i} Done.
done

PIGLATIN="GeelongWifiDigger.pig";
TIMEFILE="PigTestTime.log";

#for i in $(seq 1 ${NUM})
#do
#    sed -i 's/Lines_.*K[.]csv/Lines_'${i}K'.csv/g' $PIGLATIN;
#    echo Test \#${i}: >> $TIMEFILE;
#    pig -x local $PIGLATIN &> $LOGFILE;
#    cat $LOGFILE | grep '(.*ms)' >> $TIMEFILE;
#    echo Test \#${i} Done.
#done
