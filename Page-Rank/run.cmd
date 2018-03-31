cd D:\workspace\Hadoop-tuto\Page-Rank
cd data\_movies\graph
hadoop fs -rm -r input
hadoop fs -rm -r output
hadoop fs -mkdir input
hadoop fs -mkdir output
hadoop fs -copyFromLocal adj_list input
hadoop fs -copyFromLocal nodes input
cd ..\..\..\jar
hadoop jar page-rank.jar ooc.ex01.pr.AppDriver input output/
cd ..\
rmdir /Q /S output
mkdir output
cd output
hadoop fs -get output/*
cd D:\workspace\Hadoop-tuto\Page-Rank


