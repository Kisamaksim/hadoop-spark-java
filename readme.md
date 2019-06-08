# Структура проекта
- /docker - директория с докер файлами для hdfs и spark(master/worker)
- /results - текстовые файлы с результатом выполнения тасок из лабораторной работы
- /src - исходный код, в классе Main реализовано 3 статических метода, каждый представляет
решение одной из таски, сопровождены комментариями, какой именно

# Запуск
1. Сбилдить и запустить докер контейнеры hadoop-hdfs/spark-master/spark-worker
    - hadoop-hdfs
        - 	docker build -t hdfs:mult .
        - 	docker run -d -p 8088:8088 -p 9000:9000 -p 50070:50070 -p 50075:50075 -p 50030:50030 -p 50060:50060 --name hdfs hdfs:mult
    - spark-main
        - docker build -t spark-main:mult
    - spark-master
        - docker build -t spark-master:mult .
        - docker run -d -p 8080:8080 -p 7077:7077 --name spark-master spark-master:mult
    - spark-worker (в Dockerfile нужно установить IP машины, где стартован spark-master, для их коннекта)
        - docker build -t spark-worker:mult .
        - docker run -d -p 8081:8081 --name spark-worker spark-worker:mult
2. Собрать проект mvn clean package
3. Запустить из Idea maksim.ia.Main
