deploy dockers
1. HDFS
	cd docker-hdfs
	docker build -t hdfs:custom .
	docker run -d -p 8088:8088 -p 9000:9000 -p 50070:50070 -p 50075:50075 -p 50030:50030 -p 50060:50060 --name hdfs hdfs:custom
	docker exec -it hdfs bash
		docker cp <file_on_host> <file_on_docker>
		hadoop fs -mkdir /nasa
		hadoop fs -put <file name> /nasa/<file name>
2. Spark
2.0 Spark base
	cd docker-spark/base
	docker build -t spark-base:custom
2.1 Spark master
	cd docker-spark/master
	docker build -t spark-master:custom .
	docker run -d -p 8080:8080 -p 7077:7077 --name spark-master spark-master:custom
2.2 Spark worker
	docker build -t spark-worker:custom .
	docker run -d -p 8081:8081 --name spark-worker spark-worker:custom
3. Java Application
3.1 mvn clean package
3.2 run local maksim.ia.Main
    (в классе 3 статических метода task<One,Two,Three> - представляющие решение тасок из лабы)
    (в /results результаты по каждой из тасок)