import java.io.IOException;
import java.time.LocalDate;
import java.time.Month;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2 {
    
      public static class AQIMonthMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text month = new Text();
        private IntWritable aqi = new IntWritable();
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            Integer aqiScore = Integer.parseInt(line[1]);
            LocalDate dateFromEpoch = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(line[2])), ZoneOffset.UTC).toLocalDate();
            Month monthFromEpoch = dateFromEpoch.getMonth();
            month.set(monthFromEpoch.toString());
            aqi.set(aqiScore);
            context.write(month, aqi);
        }

    }
    
      public static class AQIMonthReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        
        private Map<Text, Double> meanAqiPerMonth = new HashMap<>();
        private Text month = new Text();
        private DoubleWritable aqi = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> aqis, Context context) throws IOException, InterruptedException {
          Double sum = 0.0;
          int size = 0;
          for (IntWritable el : aqis) {
            sum += el.get();
            size++;
          }
          Double meanAqi = sum / size;
          Text keyCopy = new Text(key);
          meanAqiPerMonth.put(keyCopy, meanAqi);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            TreeMap<Double, String> sortedAqiPerMonth = new TreeMap<>();
            for (Map.Entry<Text, Double> entry : meanAqiPerMonth.entrySet()) {
                sortedAqiPerMonth.put(entry.getValue(), entry.getKey().toString());
            }
            Map.Entry<Double, String> first = sortedAqiPerMonth.firstEntry();
            month.set(String.format("Lowest: %s", first.getValue()));
            aqi.set(first.getKey());
            context.write(month, aqi);
            Map.Entry<Double, String> last = sortedAqiPerMonth.lastEntry();
            month.set(String.format("Highest: %s", last.getValue()));
            aqi.set(last.getKey());
            context.write(month, aqi);
        }

    }
    
      public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HW3 Q2");
        job.setJarByClass(Q2.class);
        job.setMapperClass(AQIMonthMapper.class);
        job.setReducerClass(AQIMonthReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }

    }
