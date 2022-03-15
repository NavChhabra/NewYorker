#!/bin/bash

spark-submit \
    --name "NewYorker" \
    --master local[*] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/jovyan/work/log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/jovyan/work/log4j.properties" \
    --deploy-mode client \
    --driver-memory 8G \
    --num-executors 4 \
    --executor-memory 5G \
    --executor-cores 8 \
    --files "/home/jovyan/work/log4j.properties" \
    main.py "$@"