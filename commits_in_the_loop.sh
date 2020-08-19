#!/usr/bin/env bash

for p in {1..22} ; do
    for d in {1..22} ; do
            export PARENTS=$((p*5-4))
            export DEPENDANTS=$((d*5-4))
            export REPEAT=5
            echo "[INFO]: STARTING parents ${PARENTS} dependants ${DEPENDANTS} repeats ${REPEAT}"
            beginTime=$(($(date +%s%N)/1000))
            LOGFILE="test_parents_${PARENTS}_dependants${DEPENDANTS}_repeat_${REPEAT}.log"
            echo "[INFO]: START,${PARENTS},${DEPENDANTS},${REPEAT},${beginTime}" > runlog.log
            mvn test -Dtest=com.tomtom.aktools.CommitingTest#massiveCommitInOrderTest > ${LOGFILE} 2>&1
            endTime=$(($(date +%s%N)/1000))
            echo "[INFO]: $(grep AVG_COMMIT_TIME_MS ${LOGFILE} | less)"
            echo "[INFO]: $(grep TOTALL_COMMIT_TIME_MS ${LOGFILE} | less)"
            errors=$(grep ERROR ${LOGFILE} | wc | awk '{print $2}')
            echo "[INFO]: END,${PARENTS},${DEPENDANTS},${REPEAT},${endTime}, $((endTime-beginTime)), ${errors}"
            echo "[INFO]: END,${PARENTS},${DEPENDANTS},${REPEAT},${endTime}, $((endTime-beginTime)), ${errors}" > runlog.log
    done
done

