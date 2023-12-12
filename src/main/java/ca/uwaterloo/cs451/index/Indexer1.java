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
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

public class Indexer1 {

    static double totalDocLength = 0.0;
    static int count = 0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ConsumerConfig consumerConfig = new ConsumerConfig(prop);

        // Kafka properties
		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("indexer-topic")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		DataStream<Document> indexStream = env
				.fromSource(
						source,
						WatermarkStrategy.noWatermarks(), "kafka Stream"
				).setParallelism(1).map(new MapFunction<String, ca.uwaterloo.cs451.index.Document>() {
					@Override
					public ca.uwaterloo.cs451.index.Document map(String value) {
						List<String> tokens = BetterTokenizer.tokenize(value.toString()); // Add kafka source
						return new ca.uwaterloo.cs451.index.Document(tokens.get(0), tokens);
					}
				});

//        String filePath = "/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/ShakespeareID1.txt";
//
//        DataStream<Document> indexStream = env.readTextFile(filePath)
//                .map(new MapFunction<String, ca.uwaterloo.cs451.index.Document>() {
//                    @Override
//                    public ca.uwaterloo.cs451.index.Document map(String value) {
//                        String[] words = value.split("\\s+");
//                        List<String> myList = new ArrayList();
//                        Collections.addAll(myList, words);
//                        String docID = myList.get(0);
//                        myList.remove(0);
//                        String ss =
//                        myList.stream()
//                                .collect(Collectors.joining(" "));
//                        List<String> tokens = BetterTokenizer.tokenize(ss);
//                        return new ca.uwaterloo.cs451.index.Document(docID, tokens);
//                    }
//                });

        HashMap<String, Integer> COUNTS = new HashMap<>();

        DataStream<Tuple4<String, String, Integer, Integer>> indexedStream = indexStream.flatMap(new FlatMapFunction<Document, Tuple4<String, String, Integer, Integer>>() {
            @Override
            public void flatMap(Document doc, Collector<Tuple4<String, String, Integer, Integer>> collector) throws Exception {
//				public Tuple3<String, Integer, Double> flatMap(ca.uwaterloo.cs451.index.Document doc) {
                List<String> words = doc.content;
                int docLength = words.size();
                String docId = doc.docid;
                COUNTS.clear();
                for (String token : words) {
                    if(COUNTS.containsKey(token)){
                        COUNTS.put(token, COUNTS.get(token)+1);
                    }else{
                        COUNTS.put(token, 1);
                    }
                }

                for (String word : words) {
                    //(term, docID, tf, doclen)
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
        });

        KeyedStream<Tuple4<String, String, Integer, Integer>,String> keyedStream = indexedStream.keyBy(new KeySelector<Tuple4<String, String, Integer, Integer>, String>() {
            @Override
            public String getKey(Tuple4<String, String, Integer, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //avgDocLengthStream.print("Running Average Document Length");
        keyedStream.writeAsText("/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/data/PostingsTuples1.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Stream Indexing1");
    }
}





