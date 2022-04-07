import java.io.IOException;
import java.util.StringTokenizer;
import java.time.Instant;
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10 {

    public class CountyAvg implements Comparable<CountyAvg> {
        public String county;
        public int avg;

        public CountyAvg(String county, int avg) {
            this.county = county;
            this.avg = avg;
        }

        @Override
        public int compareTo(CountyAvg other) {
            int diff = this.avg - other.avg;
            return diff;
        }

        @Override
        public String toString() {
            return this.county + ":" + this.avg;
        }
    }

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        // map api in 2020 to each county
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top10");
        job.setJarByClass(Top10.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}