//package ca.uwaterloo.cs451.index;
//
//import com.datastax.driver.core.Cluster;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.core.fs.FileSystem;
//import java.util.UUID;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
//import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
//
//public class AvgDocumentLengthJob
//{
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        String filePath = "/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/Shakespeare.txt";
//
//        DataStream<String> indexStream = env.readTextFile(filePath);
//
////        DataSource<String> text = env.("/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/Shakespeare.txt");
//
//        DataStream<Tuple4<String, Integer, Integer, String>> lineCount = indexStream
////                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
////                    @Override
////                    public Tuple3<String, Integer, Integer> map(String s) throws Exception {
////                        return Tuple3.of("Shakespeare1", 1, s.split("\\s+").length);
////                    }
////                })
////                .groupBy(0)
////                        .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
////                            @Override
////                            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t1, Tuple3<String, Integer, Integer> t2) throws Exception {
////                                return Tuple3.of(t1.f0, t1.f1+t2.f1, t1.f2+t2.f2);
////                            }
////                        })
////                                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple4<String, Integer, Integer, String>>() {
////                                    @Override
////                                    public Tuple4<String, Integer, Integer, String> map(Tuple3<String, Integer, Integer> z) throws Exception {
////                                        String timeuuid = com.datastax.driver.core.utils.UUIDs.timeBased().toString();
////                                        return Tuple4.of(z.f0, z.f1, (int) z.f2/z.f1, timeuuid);
////                                    }
////                                });
//
//                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
//                    @Override
//                    public Tuple3<String, Integer, Integer> map(String s) throws Exception {
//                        return Tuple3.of("Shakespeare1", 1, s.split("\\s+").length);
//                    }
//                })
//                .keyBy(v->v.f0)
//                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
//                    @Override
//                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t1, Tuple3<String, Integer, Integer> t2) throws Exception {
//                        return Tuple3.of(t1.f0, t1.f1+t2.f1, t1.f2+t2.f2);
//                    }
//                })
//                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple4<String, Integer, Integer, String>>() {
//                    @Override
//                    public Tuple4<String, Integer, Integer, String> map(Tuple3<String, Integer, Integer> z) throws Exception {
//                        String timeuuid = com.datastax.driver.core.utils.UUIDs.timeBased().toString();
//                        return Tuple4.of(z.f0, z.f1, (int) z.f2/z.f1, timeuuid);
//                    }
//                });
//
//        //int lineCount.collect().get(0)
//
//        lineCount.print();
//        //lineCount.writeAsText("/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/AvgDL.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
////        CassandraSink.addSink(lineCount)
////                .setQuery("INSERT INTO indexer_space.collection_info_by_name (name,avgdl,total_docs,uuid_time) values (?,?,?,?);")
////                .setHost("127.0.0.1")
////                .build();
//
//
////        CassandraSink<Tuple4<String, Integer, Integer, String>> sink = CassandraSink.addSink(lineCount)
////                .setQuery("INSERT INTO indexer_space.collection_info_by_name (name,avgdl,total_docs,uuid_time) values (?,?,?,?);")
////                .enableWriteAheadLog()
////                .setClusterBuilder(new ClusterBuilder() {
////
////                    @Override
////                    protected Cluster buildCluster(Cluster.Builder builder) {
////                        return builder.addContactPoint("127.0.0.1").build();
////                    }
////
////                })
////                .build();
//
//        env.execute("Avg Document Length Job");
//
//
//
//    }
//}


package ca.uwaterloo.cs451.index;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

public class AvgDocumentLengthJob
{
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("/Users/shakti/Desktop/University_of_Waterloo/Fall2023/CS651/Project/Information-Retrieval-System/BigData-Retrieval-Project/data/Shakespeare.txt").setParallelism(1);

        DataSet<Tuple3<String, Integer, Integer>> lineCount = text
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String s) throws Exception {
                        return Tuple3.of("DocInfo", 1, s.split("\\s+").length);
                    }
                }).setParallelism(1)
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t1, Tuple3<String, Integer, Integer> t2) throws Exception {
                        return Tuple3.of(t1.f0, t1.f1+t2.f1, t1.f2+t2.f2);
                    }
                }).setParallelism(1)
                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Tuple3<String, Integer, Integer> z) throws Exception {
                        return Tuple3.of(z.f0, z.f1, (int) z.f2/z.f1);
                    }
                }).setParallelism(1);

        //lineCount.print();
        lineCount.writeAsText("/Users/shakti/Desktop/University_of_Waterloo/Fall2023/CS651/Project/Information-Retrieval-System/BigData-Retrieval-Project/data/Avg-dl.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Avg Document Length Job");



    }
}
