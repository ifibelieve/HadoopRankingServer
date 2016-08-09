HadoopRankingServer

Sum->Sort

Sum 

1. Map     - (key:line, value:"id    point") -> (key:id, value:point)

2. Combine - (key:id, value:point) -> (key:id, value:sum(point))

3. Reduce  - (key:id, value:sum(point)) -> (key:sum(point) , value:key)

[[https://github.com/ifibelieve/HadoopRankingServer/blob/master/wiki/sum-result.png]]

Sort

1. Map     - Indentical

2. Shuffle - TotalOrderPartitioner 

3. Reduce  - (key:sum(point), value:list(id))-> (key:"rank    ID:id    PT:sum(point)", value:null)

[[https://github.com/ifibelieve/HadoopRankingServer/blob/master/wiki/partition.png]]
[[https://github.com/ifibelieve/HadoopRankingServer/blob/master/wiki/sort-result.png]]
