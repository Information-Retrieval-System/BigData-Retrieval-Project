package ca.uwaterloo.cs451.kafkaproducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.*;

public class KafkaProducer {

    private static final Logger LOG = Logger.getLogger(KafkaProducer.class);


    private static String filePath = "";
    private static final String bootstrapServers = "localhost:9092";
    private static final String topic = "indexer-topic";

    private static long counter = 0;

    public static void main(String[] args) {

//        final Args args = new Args();
//        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
//
//        try {
//            parser.parseArgument(argv);
//        } catch (CmdLineException e) {
//            System.err.println(e.getMessage());
//            parser.printUsage(System.err);
//            return -1;
//        }


        LOG.info(" - input path: " + args[0]);
        //LOG.info(" - output path: " + args[1]);
        //LOG.info(" - number of reducers: " + args[2]);
        filePath = args[0];


        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Kafka producer
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        // Schedule a task to read and send file content periodically
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            try {
                // Read file content
                String fileContent = readFile(filePath);
                System.out.println("Start writing to kafka producer....");
                // Produce message to Kafka topic
                producer.send(new ProducerRecord<>(topic, fileContent));
                System.out.println("Message sent to Kafka: " + fileContent);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 0, 5, TimeUnit.SECONDS); // Send every 5 seconds

        // Add shutdown hook to gracefully close resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.shutdown();
            producer.close();
        }));
    }

    private static String readFile(String filePath) throws IOException {
        String line = "";
        try{
            System.out.println("Start reading file....");
            RandomAccessFile file = new RandomAccessFile(filePath, "r");
            System.out.println("Start reading file....");
            file.seek(counter);
            System.out.println("Start reading file....");
            line = file.readLine();
            System.out.println("Start reading file....");
            long offset = file.getFilePointer();
            System.out.println("Start reading file....");
            counter = offset;


        }
        catch (IOException e) {
            e.printStackTrace();
        }
//        Path path = Paths.get(filePath);
//        byte[] fileBytes = Files.r(path);
//        return new String(fileBytes);
        System.out.println("Start reading file....");
        return line;
    }


//
//        try {
//            while(true){
//                // keep reading data from Shakespeare file and push to kafka broker
//
//                String key = "key_" + i;
//                String value = "Message " + i;
//
//                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
//                producer.send(record);
//
//            }
//            // Produce messages to Kafka topic
//            for (int i = 0; i < 10; i++) {
//                //read
//                String key = "key_" + i;
//                String value = "Message " + i;
//
//                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
//                producer.send(record);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            // Close the producer to release resources
//            producer.close();
//        }
    }
