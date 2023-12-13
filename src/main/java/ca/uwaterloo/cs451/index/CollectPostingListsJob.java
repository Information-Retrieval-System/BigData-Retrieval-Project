package ca.uwaterloo.cs451.index;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class CollectPostingListsJob {

    public static void main(String[] args) throws Exception {


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("/Users/shakti/Desktop/University_of_Waterloo/Fall2023/CS651/Project/Information-Retrieval-System/BigData-Retrieval-Project/data/PostingsTuples1.txt");

        DataSet<Tuple2<String, List<List<String>>>> entire1 = text
                .map(new MapFunction<String, Tuple2<String, List<List<String>>>>() {
                    @Override
                    public Tuple2<String, List<List<String>>> map(String s) throws Exception {
                        s = s.replace("(", "");
                        s = s.replace(")", "");
                        String[] sArr = s.split(",");
                        List<List<String>> list = new ArrayList<>();
                        List<String> l1 = new ArrayList<>();
                        l1.add(sArr[1]);
                        l1.add(sArr[2]);
                        l1.add(sArr[3]);
                        list.add(l1);
                        return Tuple2.of(sArr[0], list);
                    }
                })
                .groupBy(x -> x.f0)
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
                        termTuple.f1.sort((x, y) -> Integer.parseInt(x.get(0)) - Integer.parseInt(y.get(0)));

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


        entire1.writeAsText("/Users/shakti/Desktop/University_of_Waterloo/Fall2023/CS651/Project/Information-Retrieval-System/BigData-Retrieval-Project/data/PostingLists.txt" ,FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Collect Posting List");
    }

}
