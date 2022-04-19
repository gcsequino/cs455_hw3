
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


public class Q5 {

  public static class UniqueWeekMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private IntWritable aqi = new IntWritable(1);
    private Text timeInfo = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",");                                                      //Split each line (GIS,AQi,EPOCH)
      String GIS = line[0];         //County
      
      Integer aqiScore = Integer.parseInt(line[1]);       // AQI
      LocalDate epochDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(line[2])), ZoneOffset.UTC).toLocalDate();
      WeekFields weekFields = WeekFields.of(Locale.getDefault()); 
      int weekNumber = epochDate.get(weekFields.weekOfWeekBasedYear());
      int year = epochDate.getYear();
      String data = "" + GIS+","+weekNumber+","+year+",";
      timeInfo.set(line[2]);                         //key
      aqi.set(aqiScore);                        //value    
      context.write(timeInfo,aqi);             //Pass (CountyWeekYear, aqi) to reducer
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
      context.write(key,MeanAqiValue);              // <UniqueWeek, Average Aqi>

      //meanAqiPerDay.put(keyCopy, meanAqiForEachWeek); 
    }
     
  }

  public static class CountyMapper extends Mapper<Object, Text, Text, Text>{                 //Mapper 2

    private Text WeekYearAqi = new Text();
    private Text GISInfo = new Text();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",");  //Split each line (GIS,Week,Year,avgaqi)
      String county = line[0]; //use county has my key
      String WeekYearAvgAqi =  ""+ line[1]+"," + line[2]+"," + line[3];   // the rest of the info as my value
      WeekYearAvgAqi = WeekYearAvgAqi.trim(); 
      WeekYearAqi.set(WeekYearAvgAqi); //value
      GISInfo.set(county);    //key                         
      context.write(GISInfo,WeekYearAqi);             //Pass (County, WeekYearAqiAverage) to reducer
    }
  }

  public static class CountyReducer extends Reducer<Text,Text,Text,DoubleWritable> {          //Reducer 2                                                    
    
    
    public void reduce(Text key, Iterable<Text> aqis, Context context) throws IOException, InterruptedException { //<County, WeekYearAvgAqi>
      DoubleWritable greatestChange = new DoubleWritable(); 
      TreeMap<String, String> sortedAqiPerWeekYear = new TreeMap<>(); //used to sort the Aqis by week for each county
      for(Text WeekYearT : aqis){
        String WeekYear = WeekYearT.toString();
        String[] line = WeekYear.split(",");
        sortedAqiPerWeekYear.put(line[1] + line[0] , line[2]); //sort by <WeekYear>. Values are the aqis
      }

      ArrayList<String> listOfAverageInOrder = new ArrayList<>();
      for(Map.Entry<String,String> entry : sortedAqiPerWeekYear.entrySet()) {
        String value = entry.getValue();
      
        listOfAverageInOrder.add(value);
      }

      Double Maxchange = 0.0;
      for(int i = 0;i<listOfAverageInOrder.size()-1;i++){
        if(Double.parseDouble(listOfAverageInOrder.get(i+1)) - Double.parseDouble(listOfAverageInOrder.get(i)) > Maxchange){
          Maxchange = Double.parseDouble(listOfAverageInOrder.get(i+1)) - Double.parseDouble(listOfAverageInOrder.get(i));
        }
      }
      greatestChange.set(Maxchange);
      context.write(key, greatestChange);
   }
   
}

  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "First Mapper");
    job.setJarByClass(Q5.class);
    job.setMapperClass(UniqueWeekMapper.class);
    job.setReducerClass(UniqueWeekAverageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Second Mapper");
    job2.setJarByClass(Q5.class);
    job2.setMapperClass(CountyMapper.class);
    job2.setReducerClass(CountyReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[2]));
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}