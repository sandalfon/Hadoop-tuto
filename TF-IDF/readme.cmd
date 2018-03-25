cd D:\workspace\Hadoop-tuto\TF-IDF
hadoop fs -rm -r /input
hadoop fs -mkdir /input
cd data
hadoop fs -put * /input/
hadoop fs -rm -r /output
cd ..\jar
hadoop jar tf-idf.jar AppDriver /input/ /output/
cd ..\output
del /Q *
hadoop fs -get /output/*
COPY /Y part-r-00000 result.txt
cd D:\workspace\Hadoop-tuto\TF-IDF


