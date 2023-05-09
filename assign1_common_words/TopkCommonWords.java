import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TopkCommonWords {

    public static class TokenizerMapper1
    extends Mapper<Object, Text, Text, IntWritable>{

        private ArrayList<String> stopWords = new ArrayList<String>();
        private final static IntWritable docId1 = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("filePath");
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line = reader.readLine();
            while(line != null) {
                stopWords.add(line);
                line = reader.readLine();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (stopWords.contains(word.toString()))
                    continue;
                context.write(word, docId1);
            }
        }
    }

    public static class TokenizerMapper2
    extends Mapper<Object, Text, Text, IntWritable>{

        private ArrayList<String> stopWords = new ArrayList<String>();
        private final static IntWritable docId2 = new IntWritable(2);
        private Text word = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("filePath");
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line = reader.readLine();
            while(line != null) {
                stopWords.add(line);
                line = reader.readLine();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (stopWords.contains(word.toString()))
                    continue;
                context.write(word, docId2);
            }
        }
    }

    public static class IntSumReducer
    extends Reducer<Text, IntWritable, IntWritable, Text> {
        
        int result = 0;
        private Map<String, Integer> cw = new HashMap<String, Integer>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum1 = 0;
            int sum2 = 0;
            for (IntWritable val : values) {
                if (val.get() == 1) {
                    sum1++;
                } else if (val.get() == 2) {
                    sum2++;
                } else {
                    return;
                }
            }
            result = Math.min(sum1, sum2);
            cw.put(key.toString(), result);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException{
            int count = 0;
            LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();
            cw.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
            for(Map.Entry<String, Integer> w: sortedMap.entrySet()){
                if(count>=20)
                    break;
                count++;
                String k = w.getKey();
                Text word = new Text(k);
                IntWritable num = new IntWritable(w.getValue());
                context.write(num, word);
            }
        }
    }

    /* Command Format: TopkCommonWords <input_file1> <input_file2> <stopwords> <output_dir> */
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("filePath", args[2]); //stopwords
        Job job = Job.getInstance(conf, "TopkCommonWords");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(TokenizerMapper1.class);
        job.setMapperClass(TokenizerMapper2.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper1.class); //file1
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class); //file2
        FileOutputFormat.setOutputPath(job, new Path(args[3])); //output
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
