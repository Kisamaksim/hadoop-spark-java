#!/bin/bash
docker cp /Users/maksimakidovic/IdeaProjects/victoria/results/firstTaskResult.txt hdfs:/
docker cp /Users/maksimakidovic/IdeaProjects/victoria/results/secondTaskResult.txt hdfs:/
docker cp /Users/maksimakidovic/IdeaProjects/victoria/results/thirdTaskResult.txt hdfs:/
docker exec hdfs bash -c '/opt/hadoop/bin/hadoop fs -put /firstTaskResult.txt /result'
docker exec hdfs bash -c '/opt/hadoop/bin/hadoop fs -put /secondTaskResult.txt /result'
docker exec hdfs bash -c '/opt/hadoop/bin/hadoop fs -put /thirdTaskResult.txt /result'
