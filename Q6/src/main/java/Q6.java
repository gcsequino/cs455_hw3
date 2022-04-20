
import java.io.IOException;
import java.time.LocalDate;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Comparator;
import java.time.temporal.WeekFields;

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


public class Q6 {

  public static class WeekGIS extends Mapper<Object, Text,Text, Text>{
    
    private Text WeekGIS = new Text();
    private Text YearAqi = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",");                                                      //Split each line (GIS,AQi,EPOCH)
      String GIS = line[0];         //County
      Integer aqiScore = Integer.parseInt(line[1]);       // AQI
      LocalDate epochDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(line[2])), ZoneOffset.UTC).toLocalDate();
      WeekFields weekFields = WeekFields.of(Locale.getDefault()); 
      int weekNumber = epochDate.get(weekFields.weekOfWeekBasedYear());
      int year = epochDate.getYear();
      String data = String.format("%d , %d", year, aqiScore); // year aqi 
      String WeekGIS = String.format("%d , %s", weekNumber, GIS); // Week GIS 
      
      WeekGIS.set(WeekGIS);          // key               
      YearAqi.set(data);                        //value         
      context.write(WeekGIS,YearAqi);                 
    }
  }
  
  //Reducer 1
  public static class YearAQI extends Reducer<Text,IntWritable,Text,DoubleWritable> { 
    private DoubleWritable MeanAqiValue = new DoubleWritable();
    public void reduce(Text key, Iterable<IntWritable> aqis, Context context) throws IOException, InterruptedException {
      TreeMap<String, String> sortYear = new TreeMap<>(); 
      for(Text YearAqi : aqis){
        String YearAqi = WeekYearT.toString();
        String[] line = YearAqi.split(",");
        sortYear.put(line[0], line[1]); // sort by year
      }

      ArrayList<String> listOfAqiInOrder = new ArrayList<>();
      for(Map.Entry<String,String> entry : sortYear.entrySet()) {
        String value = entry.getValue();
        listOfAqiInOrder.add(value); // get 
      }
      Double Maxchange = 0.0;
      for(int i = 0;i<listOfAqiInOrder.size()-1;i++){
        if(Math.abs(Double.parseDouble(listOfAqiInOrder.get(i+1)) - Double.parseDouble(listOfAqiInOrder.get(i))) > Math.abs(Maxchange)) {
          Maxchange = Double.parseDouble(listOfAqiInOrder.get(i+1)) - Double.parseDouble(listOfAqiInOrder.get(i));
        }
      }
      
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "First Mapper");
    job.setJarByClass(Q6.class);
    job.setMapperClass(WeekGIS.class);
    job.setReducerClass(YearAQI.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion(true);
  }

}

