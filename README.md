# HadoopRankingServer
HadoopRankingServer


Sum->Sort->Rank


1.Sum  : (id : point) -> (sum(point) : list(id))

2.Sort : (sum(point) : list(id)) -> TotalOrderPartitioner -> (rank : id, sum(point) )


javac -cp $(bin/hadoop classpath) -d HadoopRankingServer/class/ HadoopRankingServer/src/*.java

jar -cvf HadoopRankingServer/Rank.jar -C HadoopRankingServer/class .

bin/hadoop jar HadoopRankingServer/Rank.jar my.hadoop.test.Rank input out1 out2 10

bin/hadoop fs -cat ./out2/*

