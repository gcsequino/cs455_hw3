
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


public class Q {

  public static class UniqueWeekMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private IntWritable WeekNumba = new IntWritable(1);
    private Text YearAqi = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",");                                                      //Split each line (GIS,AQi,EPOCH)
      String GIS = line[0];         //County
      
      Integer aqiScore = Integer.parseInt(line[1]);       // AQI
      LocalDate epochDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(line[2])), ZoneOffset.UTC).toLocalDate();
      WeekFields weekFields = WeekFields.of(Locale.getDefault()); 
      int weekNumber = epochDate.get(weekFields.weekOfWeekBasedYear());
      int year = epochDate.getYear();
      String data = String.format("%d , %d", year, aqiScore);
      
      WeekNumba.set(weekNumber);                         //key
      YearAqi.set(data);                                //value    
      context.write(weekNumba,YearAqi);                 //Pass (Week, year , aqi) to reducer
    }
  }
  
  //Reducer 1
  public static class UniqueWeekAverageReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> { //Reducing function - calculate the average for each unique week(key passed by the mapper)
    private DoubleWritable MeanAqiValue = new DoubleWritable();
    public void reduce(Text key, Iterable<IntWritable> aqis, Context context) throws IOException, InterruptedException {
      Double sum = 0.0;
      int size = 0;
      for (IntWritable value : aqis) {
        sum += value.get();
        size++;
      }
      Double meanAqiForEachWeek = sum / size;                                //mean of AQI for that County,in that year, in that week. 
      Text keyCopy = new Text(key);
      MeanAqiValue.set(meanAqiForEachWeek);                     
      context.write(keyCopy,MeanAqiValue);              // <UniqueWeek, Average Aqi>

      //meanAqiPerDay.put(keyCopy, meanAqiForEachWeek); 
    }
     
  }