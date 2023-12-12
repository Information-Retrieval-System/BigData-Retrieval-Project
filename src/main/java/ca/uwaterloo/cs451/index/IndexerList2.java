package ca.uwaterloo.cs451.index;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import ca.uwaterloo.cs451.index.Document;
import ca.uwaterloo.cs451.index.DocumentInfo;
import ca.uwaterloo.cs451.index.IndexEntry;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IndexerList2 {

    static double totalDocLength = 0.0;
    static int count = 0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ConsumerConfig consumerConfig = new ConsumerConfig(prop);

//		WatermarkStrategy<Tuple3<String, Integer, Long>> strategy = WatermarkStrategy
//				.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(30))
//				.withTimestampAssigner((event, timestamp) -> event.f2);

        // Kafka properties
//		KafkaSource<String> source = KafkaSource.<String>builder()
//				.setBootstrapServers("localhost:9092")
//				.setTopics("indexer-topic")
//				.setGroupId("my-group")
//				.setStartingOffsets(OffsetsInitializer.latest())
//				.setValueOnlyDeserializer(new SimpleStringSchema())
//				.build();

        //        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

//		DataStream<Document> indexStream = env
//				.fromSource(
//						source,
//						WatermarkStrategy.noWatermarks(), "kafka Stream"
//				).setParallelism(1).map(new MapFunction<String, ca.uwaterloo.cs451.index.Document>() {
//					@Override
//					public ca.uwaterloo.cs451.index.Document map(String value) {
//						List<String> tokens = Tokenizer.tokenize(value.toString()); // Add kafka source
//						return new ca.uwaterloo.cs451.index.Document(tokens.get(0), value);
//					}
//				});

        String filePath = "/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/ShakespeareID.txt";

        DataStream<Document> indexStream = env.readTextFile(filePath)
                .map(new MapFunction<String, ca.uwaterloo.cs451.index.Document>() {
                    @Override
                    public ca.uwaterloo.cs451.index.Document map(String value) {
                        String[] words = value.split("\\s+");

                        List<String> myList = new ArrayList();
                        //String[] myArray = new String[] {"Java", "Util", "List"};

                        Collections.addAll(myList, words);
                        String docID = myList.get(0);
                        myList.remove(0);
                        String ss =
                                myList.stream()
                                        // .map(Person::getFirstName)
                                        .collect(Collectors.joining(" "));
                        List<String> tokens = BetterTokenizer.tokenize(ss);
//						if(!tokens.isEmpty())
//						System.out.println(tokens.get(0));
                        return new ca.uwaterloo.cs451.index.Document(docID, tokens);
//						return new ca.uwaterloo.cs451.index.Document("", value);
                    }
                });

//		DataStream<Document> indexFiltered = indexStream.filter(new FilterFunction<Document>() {
//			@Override
//			public boolean filter(Document doc) throws Exception {
//				return (!doc.docid.isEmpty());
//			}
//		});
        HashMap<String, Integer> COUNTS = new HashMap<>();

        DataStream<Tuple2<String, List<List<String>>>> indexedStream = indexStream.flatMap(new FlatMapFunction<Document, Tuple2<String, List<List<String>>>>() {
            @Override
            public void flatMap(Document doc, Collector<Tuple2<String, List<List<String>>>> collector) throws Exception {
//				public Tuple2<String, List<List<String>>> flatMap(ca.uwaterloo.cs451.index.Document doc) {
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

                List<List<String>> list = new ArrayList<>();
                List<String> l1 = new ArrayList<>();

                for (String word : words) {
                    l1.add(docId);
                    l1.add(COUNTS.get(word).toString());
                    l1.add(String.valueOf(docLength));
                    list.add(l1);
                    collector.collect(Tuple2.of(word,list));
                }

//					return Tuple3.of("", 0, 0.0);
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
        });

        KeyedStream<Tuple2<String, List<List<String>>>,String> keyedStream = indexedStream.keyBy(new KeySelector<Tuple2<String, List<List<String>>>, String>() {
            @Override
            public String getKey(Tuple2<String, List<List<String>>> value) throws Exception {
                return value.f0;
            }
        });

        DataStream<Tuple2<String, List<List<String>>>> reducedStream = keyedStream
//                .window(GlobalWindows.create())
//                .trigger(CountTrigger.of(2))
                .reduce(new ReduceFunction<Tuple2<String, List<List<String>>>>() {
            @Override
            public Tuple2<String, List<List<String>>> reduce(Tuple2<String, List<List<String>>> t1, Tuple2<String, List<List<String>>> t2) throws Exception {
                List<List<String>> f = new ArrayList<>(t1.f1);
                f.addAll(t2.f1);
                    return new Tuple2<>(t2.f0, f);
            }
        });





         reducedStream.print().setParallelism(1);

//        DataStream<List<Tuple4<String, String, Integer, Double>>> resultStream = keyedStream
//				.window(TumblingEventTimeWindows.of(Time.seconds(30)))
//                //.window(GlobalWindows.create())
//                //.trigger(CountTrigger.of(2))
//                .process(new MyProcessWindowFunction());




//				.reduce(new ReduceFunction<Tuple4<String, String, Integer, Double>>() {
//					public Tuple4<String, String, Integer, Double> reduce(Tuple4<String, String, Integer, Double> v1, Tuple4<String, String, Integer, Double> v2) {
//						return new Tuple4<>(v1.f0, v1.f1 + v2.f1);

//		ReduceFunction<Tuple3<String, Integer, Double>> reduced = keyedStream.reduce(new ReduceFunction<Tuple3<String, Integer, Double>>() {
//			@Override
//			public Tuple3<String, Integer, Double> reduce(Tuple3<String, Integer, Double> t1, Tuple3<String, Integer, Double> t2) throws Exception {
//				return Tuple3(t1.f0, t1.f1 + t2.f1);
//			}
//		})

		/*DataStream<Double> avgDocLengthStream = indexStream
				.map(doc -> (double) doc.content.split(" ").length)
				.timeWindowAll(Time.seconds(60))
				.reduce((a, b) -> a + b)
				.map(avg -> avg / 60.0);*/

//		resultStream.print();
        //resultStream.writeAsText("indexer_1.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        //avgDocLengthStream.print("Running Average Document Length");
//        keyedStream.writeAsText("/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/PostingsTuples.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Stream Indexing1");
    }
}






