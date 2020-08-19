#!/usr/bin/env bash

for p in {1..100} ; do
    for d in {1..100} ; do
        for r in {1..100} ; do
            export PARENTS=${p}
            export DEPENDANTS=${d}
            export REPEAT=${r}
            echo "[INFO]: STARTING parents ${p} dependants ${d} repeats ${r}"
            beginTime=$(($(date +%s%N)/1000))
            LOGFILE="test_parents_${p}_dependants${d}_repeat_${r}.log"
            echo "[INFO]: START,${p},${d},${r},${beginTime}" > runlog.log
            mvn test -Dtest=com.tomtom.aktools.CommitingTest#massiveCommitInOrderTest > ${LOGFILE} 2>&1
            endTime=$(($(date +%s%N)/1000))
            echo "[INFO]: $(grep AVG_COMMIT_TIME_MS ${LOGFILE} | less)"
            echo "[INFO]: $(grep TOTALL_COMMIT_TIME_MS ${LOGFILE} | less)"
            echo "[INFO]: END,${p},${d},${r},${endTime}, $((endTime-beginTime)), ERRORS: $(grep ERROR ${LOGFILE} | wc)"
            echo "[INFO]: END,${p},${d},${r},${endTime}, $((endTime-beginTime)), ERRORS: $(grep ERROR ${LOGFILE} | wc)"
            echo "[INFO]: END,${p},${d},${r},${endTime}, $((endTime-beginTime))"  > runlog.log
        done
    done
done

