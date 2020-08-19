#!/usr/bin/env bash

for p in {1..100} ; do
    for d in {1..100} ; do
        for r in {1..100} ; do
            export PARENTS=${p}
            export DEPENDANTS=${d}
            export REPEAT=${r}
            echo "[INFO]: STARTING parents ${p} dependants ${d} repeats ${r}"
            beginTime=$(($(date +%s%N)/1000))
            echo "START,${p},${d},${r},${beginTime}" > runlog.log
            mvn test -Dtest=com.tomtom.aktools.CommitingTest#massiveCommitInOrderTest > test_parents_${p}_dependants${d}_repeat_${r}.log 2>&1
            endTime=$(($(date +%s%N)/1000))
            echo "END,${p},${d},${r},${endTime}, $((endTime-beginTime))"
            echo "END,${p},${d},${r},${endTime}, $((endTime-beginTime))"  > runlog.log
        done
    done
done

