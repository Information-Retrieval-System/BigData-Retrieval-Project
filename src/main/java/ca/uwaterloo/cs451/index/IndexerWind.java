package ca.uwaterloo.cs451.index;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class IndexerWind {

    static double totalDocLength = 0.0;
    static int count = 0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ConsumerConfig consumerConfig = new ConsumerConfig(prop);

        // Kafka properties
//		KafkaSource<String> source = KafkaSource.<String>builder()
//				.setBootstrapServers("localhost:9092")
//				.setTopics("indexer-topic")
//				.setGroupId("my-group")
//				.setStartingOffsets(OffsetsInitializer.earliest())
//				.setValueOnlyDeserializer(new SimpleStringSchema())
//                .setBounded(OffsetsInitializer.latest())
//				.build();
//
//        //        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//
//		DataStream<Document> indexStream = env
//				.fromSource(
//						source,
//						WatermarkStrategy.forMonotonousTimestamps() ,"kafka Stream"
//				).map(new MapFunction<String, ca.uwaterloo.cs451.index.Document>() {
//            @Override
//            public ca.uwaterloo.cs451.index.Document map(String value) {
//                String[] words = value.split("\\s+");
//
//                List<String> myList = new ArrayList();
//                //String[] myArray = new String[] {"Java", "Util", "List"};
//
//                Collections.addAll(myList, words);
//                String docID = myList.get(0);
//                myList.remove(0);
//                String ss =
//                        myList.stream()
//                                // .map(Person::getFirstName)
//                                .collect(Collectors.joining(" "));
//                List<String> tokens = BetterTokenizer.tokenize(ss);
//                return new ca.uwaterloo.cs451.index.Document(docID, tokens);
//            }
//        });

        // Kafka properties
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("indexer-topic-two")
                .setGroupId("my-group-two")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");

        DataStream<Document> indexStream = env
                .fromSource(
                        source,
                        WatermarkStrategy.forMonotonousTimestamps(), "kafka Stream"
                ).setParallelism(1).map(new MapFunction<String, ca.uwaterloo.cs451.index.Document>() {
                    @Override
                    public ca.uwaterloo.cs451.index.Document map(String value) {
                        List<String> tokens = BetterTokenizer.tokenize(value.toString()); // Add kafka source
                        System.out.println("Docid Map: " +tokens.get(0));
                        String docId = tokens.get(0);
                        tokens.remove(0);
                        return new ca.uwaterloo.cs451.index.Document(docId, tokens);
                    }
                });

//        String filePath = "/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/ShakespeareID1.txt";
//
//        DataStream<Document> indexStream = env.readTextFile(filePath)
//                .map(new MapFunction<String, ca.uwaterloo.cs451.index.Document>() {
//                    @Override
//                    public ca.uwaterloo.cs451.index.Document map(String value) {
//                        String[] words = value.split("\\s+");
//
//                        List<String> myList = new ArrayList();
//                        Collections.addAll(myList, words);
//                        String docID = myList.get(0);
//                        myList.remove(0);
//                        String ss =
//                                myList.stream()
//                                        .collect(Collectors.joining(" "));
//                        List<String> tokens = BetterTokenizer.tokenize(ss);
//                        return new ca.uwaterloo.cs451.index.Document(docID, tokens);
//                    }
//                });

        HashMap<String, Integer> COUNTS = new HashMap<>();

        DataStream<Tuple2<String, List<List<String>>>> indexedStream = indexStream.flatMap(new FlatMapFunction<Document, Tuple4<String, String, Integer, Integer>>() {
            @Override
            public void flatMap(Document doc, Collector<Tuple4<String, String, Integer, Integer>> collector) throws Exception {
//				public Tuple3<String, Integer, Double> flatMap(ca.uwaterloo.cs451.index.Document doc) {
                List<String> words = doc.content;
                int docLength = words.size();
                String docId = doc.docid;
//					String index = createIndex(doc.content);
//					String compressedIndex = compressIndex(index);
//					String gapEncodedLength = gapEncode(docLength);
//					ca.uwaterloo.cs451.index.DocumentInfo docInfo = new ca.uwaterloo.cs451.index.DocumentInfo(doc.docid, index.split(",").length, docLength);
//					List<ca.uwaterloo.cs451.index.DocumentInfo> docInfoList = new ArrayList<>();
//					docInfoList.add(docInfo);


                COUNTS.clear();
                for (String token : words) {
                    if(COUNTS.containsKey(token)){
                        COUNTS.put(token, COUNTS.get(token)+1);
                    }else{
                        COUNTS.put(token, 1);
                    }
                }

                for (String word : words) {
                    //(term, docfreq(maybe??), docID, tf, doclen)
                    collector.collect(Tuple4.of(word,docId, COUNTS.get(word), docLength));
                }
            }

            private String createIndex(String content) {
                return String.join(",", content.split(" "));
            }

            private String compressIndex(String index) {
                return index.replace(",", "");
            }

            private String gapEncode(int length) {
                String encodedLength;
                if (count == 0) {
                    encodedLength = Integer.toString(length);
                } else {
                    int gap = length - (int) totalDocLength;
                    encodedLength = Integer.toString(gap);
                }
                totalDocLength = length;
                count++;
                return encodedLength;
            }
        }).map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple2<String, List<List<String>>>>() {
            @Override
            public Tuple2<String, List<List<String>>> map(Tuple4<String, String, Integer, Integer> s) throws Exception {
                List<List<String>> list = new ArrayList<>();
                List<String> l1 = new ArrayList<>();
                l1.add(s.f1);
                l1.add(s.f2.toString());
                l1.add(s.f3.toString());
                list.add(l1);
                return Tuple2.of(s.f0, list);
            }
        }).keyBy(v->v.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
        .window(EndOfStreamWindow.INSTANCE)
                .reduce(new ReduceFunction<Tuple2<String, List<List<String>>>>() {
                    @Override
                    public Tuple2<String, List<List<String>>> reduce(Tuple2<String, List<List<String>>> t2, Tuple2<String, List<List<String>>> t1) throws Exception {
                        List<List<String>> f = new ArrayList<>();
                        f.addAll(t2.f1);
                        f.addAll(t1.f1);
                        return Tuple2.of(t2.f0, f);
                    }
                })
                .map(new MapFunction<Tuple2<String, List<List<String>>>, Tuple2<String, List<List<String>>>>() {
                    @Override
                    public Tuple2<String, List<List<String>>> map(Tuple2<String, List<List<String>>> termTuple) throws Exception {
                        List<List<String>> newPostList = new ArrayList<>();
                        long prevDocId = 0;
//                        long prevDocLen = 0;
                        termTuple.f1.sort(Comparator.comparing(x -> x.get(0)));

                        for (List<String> post: termTuple.f1) {
                            List<String> newPost = new ArrayList<>();
                            long docId = Integer.parseInt(post.get(0));
                            long delta = docId - prevDocId;
                            newPost.add(String.valueOf(delta));
                            newPost.add(post.get(1));
                            newPost.add(String.valueOf(post.get(2)));
                            prevDocId = docId;
                            newPostList.add(newPost);
                        }
                        return Tuple2.of(termTuple.f0, newPostList);
                    }
                });


        //avgDocLengthStream.print("Running Average Document Length");
        indexedStream.writeAsText("/Users/shakti/Desktop/University_of_Waterloo/Fall2023/CS651/Project/Information-Retrieval-System/BigData-Retrieval-Project/data/PostingsListsWind.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Stream Indexing 2");
    }
}






