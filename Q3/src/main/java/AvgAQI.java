import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgAQI {

    public static class CountyAvg implements Comparable<CountyAvg> {
        public String county;
        public double avg;

        public CountyAvg(String county, double avg) {
            this.county = county;
            this.avg = avg;
        }

        public int compareTo(CountyAvg other) {
            return new Double(this.avg).compareTo(new Double(other.avg));
        }

        public String toString() {
            return this.county + ":" + this.avg;
        }
    }

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        // map api in 2020 to each county

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Instant min_year = Instant.ofEpochMilli(1577836800000L); // start of 2020
            Instant max_year = Instant.ofEpochMilli(1609459199000L); // start of 2021

            String[] line = value.toString().split(",");
            Instant entry_time = Instant.ofEpochMilli(Long.parseLong(line[2]));
            if(entry_time.compareTo(min_year) >= 0 && entry_time.compareTo(max_year) < 0) {
                String county = line[0];
                Integer aqi = Integer.parseInt(line[1]);
                context.write(new Text(county), new IntWritable(aqi));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        List<CountyAvg> big_list = new ArrayList<CountyAvg>();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                ++count;
            }
            double avg = sum / (double)count;
            CountyAvg ca = new CountyAvg(key.toString(), avg);
            big_list.add(ca);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            big_list.sort(null);
            for(int i = 0; i < 10; i++) {
                CountyAvg ca = big_list.remove(0);
                context.write(new Text(ca.county), new DoubleWritable(ca.avg));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AvgAQI");
        job.setJarByClass(AvgAQI.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}