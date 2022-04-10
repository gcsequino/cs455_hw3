
import java.io.IOException;
import java.time.LocalDate;
import java.time.DayOfWeek;
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


public class Q1 {

  public static class AQIDay extends Mapper<Object, Text, Text, IntWritable>{
    
    private IntWritable aqi = new IntWritable(1);
    private Text dayOfWeek = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",");  //Split each line (GIS,AQi,EPOCH)
      Integer aqiScore = Integer.parseInt(line[1]); // AQI
      LocalDate epochDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(line[2])), ZoneOffset.UTC).toLocalDate();
      DayOfWeek day = epochDate.getDayOfWeek();  //extract enum from epoch
      dayOfWeek.set(day.toString());             //key
      aqi.set(aqiScore);                         //value    
      context.write(dayOfWeek,aqi);             //Pass (Day, aqi) to reducer
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> { //(<Day, listOfAqisForEachDay> , <Day , AverageAqiForDay>)
    
    private Map<Text, Double> meanAqiPerDay = new HashMap<>();     // Hold <day, Average>
    private Text day = new Text();    
    private DoubleWritable aqi = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> aqis, Context context) throws IOException, InterruptedException {
      Double sum = 0.0;
      int size = 0;
      for (IntWritable value : aqis) {
        sum += value.get();
        size++;
      }
      Double meanAqi = sum / size;                                //mean of AQI for that key 
      Text keyCopy = new Text(key);
      meanAqiPerDay.put(keyCopy, meanAqi); 
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      
      TreeMap<Double, String> sortedAqiPerDay = new TreeMap<>();
      for (Map.Entry<Text, Double> entry : meanAqiPerDay.entrySet()) {
          sortedAqiPerDay.put(entry.getValue(), entry.getKey().toString());     //sort based on average (low to high) 
      }
      
      Map.Entry<Double, String> lowest = sortedAqiPerDay.firstEntry();
      day.set(String.format("Lowest day: %s", lowest.getValue()));
      aqi.set(lowest.getKey());
      context.write(day, aqi);
      
      Map.Entry<Double, String> highest = sortedAqiPerDay.lastEntry();
      day.set(String.format("Highest Day: %s", highest.getValue()));
      aqi.set(highest.getKey());
      context.write(day, aqi);
  }

     
  }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "HW3 Q1");
    job.setJarByClass(Q1.class);
    job.setMapperClass(AQIDay.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}