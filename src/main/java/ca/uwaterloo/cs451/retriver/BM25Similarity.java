package ca.uwaterloo.cs451.retriever;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.file.Paths;

  public class BM25Similarity {
    //returns IDF for a term
    // numQ -  number of documents containing q_i = n(qi)


    public double idf(int totalDocs, int numQ) {
      double num = totalDocs - numQ + 0.5;
      double denom = numQ + 0.5;

      return Math.log((num / denom) + 1);
    }

    //returns ith value of bm25 score (to be summed in total later)
    //Parameters:
    //  fqd = calculates, f(q_i, D) the number of times that q_i occurs in the document D (also tf)
    //  idf - IDF value of q_i
    //  docLength - length of doc D in words
    //  avgDocLength - avg word length of Docs
    public double ithScore(int fqd, double idf, int docLength, double avgDocLength, double k1, double b) throws Exception {
      double numerator = fqd * (k1 + 1);
      double docDiv = docLength / avgDocLength;
      double denominator = fqd + (k1 * (1 - b + (b * docDiv)));

      return idf * (numerator / denominator);

    }

    // Given Query q_1 to q_n, the BM score of document is returned
    public double bm25DocScore(String query, int DocID) throws Exception {
      String[] queryArray = query.split("\\s+");
//    List<String> queryList = new ArrayList<>();
      double k1 = 1.5;
      double b = 0.75;
      double score = 0.0;
      for (String q : queryArray) {
        int tf = 0; // get from DB
        double idf = 0; //get from DB
        int docLength = 0; // get from DB
        double avgDocLength = 0; //get from DB
        score = ithScore(tf, idf, docLength, avgDocLength, k1, b) + score;
      }
      return score;
    }

    public static void main(String[] args) throws Exception {
        String query = args[0];

      try (CqlSession session = CqlSession.builder()
              // make sure you change the path to the secure connect bundle below
              .withCloudSecureConnectBundle(Paths.get("/Users/krishthek/Documents/uWaterloo/cs651/BigData-Retrieval-Project/secure-connect-posting-list-by-term.zip"))
              .withAuthCredentials("user_name","password")
              .withKeyspace("keyspace_name")
              .build()) {

        // For the sake of example, run a simple query and print the results
        ResultSet rs = session.execute("select release_version from system.local");
        Row row = rs.one();
        if (row != null) {
          System.out.println(row.getString("release_version"));
        } else {
          System.out.println("An error occurred.");
        }
      }
    }
  };






//  private void runQuery(String q) throws IOException {
//    String[] terms = q.split("\\s+");
//
//    for (String t : terms) {
//      if (t.equals("AND")) {
//        performAND();
//      } else if (t.equals("OR")) {
//        performOR();
//      } else {
//        pushTerm(t);
//      }
//    }
//
//    Set<Integer> set = stack.pop();
//
//    for (Integer i : set) {
//      String line = fetchLine(i);
//      System.out.println(i + "\t" + line);
//    }
//  }
//
//  private void pushTerm(String term) throws IOException {
//    stack.push(fetchDocumentSet(term));
//  }
//
//  private void performAND() {
//    Set<Integer> s1 = stack.pop();
//    Set<Integer> s2 = stack.pop();
//
//    Set<Integer> sn = new TreeSet<>();
//
//    for (int n : s1) {
//      if (s2.contains(n)) {
//        sn.add(n);
//      }
//    }
//
//    stack.push(sn);
//  }
//
//  private void performOR() {
//    Set<Integer> s1 = stack.pop();
//    Set<Integer> s2 = stack.pop();
//
//    Set<Integer> sn = new TreeSet<>();
//
//    for (int n : s1) {
//      sn.add(n);
//    }
//
//    for (int n : s2) {
//      sn.add(n);
//    }
//
//    stack.push(sn);
//  }
//
//  private Set<Integer> fetchDocumentSet(String term) throws IOException {
//    Set<Integer> set = new TreeSet<>();
//
//
//    ArrayList<BytesWritable> bytesWriteList = fetchPostings(term);
//    for(BytesWritable bytesWrite: bytesWriteList){
//      byte[] bytes = bytesWrite.getBytes();
//      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//      DataInput dataIn = new DataInputStream(in);
//      //        System.out.println(bytesWrite);
//      int count = 0;
//      int prevDocNo = 0;
//      while (true) {
//        try {
//          int number = WritableUtils.readVInt(dataIn);
//          int docNo = 0;
//          if(count%2==1){
//            if(count ==1){
//              docNo=number;
//            }
//            else{
//              docNo = prevDocNo+number;
//            }
//            set.add(docNo);
//            prevDocNo = docNo;
//          }
//
//          count++;
//          //                System.out.println(docNo);
//          //                System.out.println("ISSS");
//        } catch (EOFException eofe) {
//          //                System.out.println("exception");
//          break;
//        }
//      }
//    }
//
//
//
//
//    //        for (PairOfInts pair : fetchPostings(term)) {
//    //            set.add(pair.getLeftElement());
//    //        }
//
//    return set;
//  }
//
//  private ArrayList<BytesWritable> fetchPostings(String term) throws IOException {
//    Text key = new Text();
//    BytesWritable value = new BytesWritable();
//    key.set(term);
//
//
//
//    ArrayList<BytesWritable> bytesList = new ArrayList<>();
//    for(MapFile.Reader index: indexList){
//      index.get(key, value);
//      bytesList.add(value);
//    }
//
//    return bytesList;
//  }
//
//  public String fetchLine(long offset) throws IOException {
//    collection.seek(offset);
//    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));
//
//    String d = reader.readLine();
//    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
//  }
//
//  private static final class Args {
//    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
//    String index;
//
//    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
//    String collection;
//
//    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
//    String query;
//  }
//
//  /**
//   * Runs this tool.
//   */
//  @Override
//  public int run(String[] argv) throws Exception {
//    final Args args = new Args();
//    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
//
//    try {
//      parser.parseArgument(argv);
//    } catch (CmdLineException e) {
//      System.err.println(e.getMessage());
//      parser.printUsage(System.err);
//      return -1;
//    }
//
//    if (args.collection.endsWith(".gz")) {
//      System.out.println("gzipped collection is not seekable: use compressed version!");
//      return -1;
//    }
//
//    FileSystem fs = FileSystem.get(new Configuration());
//
//    initialize(args.index, args.collection, fs);
//
//    System.out.println("Query: " + args.query);
//    long startTime = System.currentTimeMillis();
//    runQuery(args.query);
//    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");
//
//    return 1;
//  }


