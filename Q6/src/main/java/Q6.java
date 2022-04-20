
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

    public static class YearWeekCountyAqiMapper extends Mapper<Object, Text, Text, IntWritable>{
    
        private Text yearWeekCounty = new Text();
        private IntWritable aqi = new IntWritable(1);
      
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String[] line = value.toString().split(","); // Split each line (GIS,Aqi,Epoch)
          String county = line[0];
          ArrayList<String> validCounties = new ArrayList<>();
          validCounties.add("G0600570");
          validCounties.add("G0600610");
          validCounties.add("G0600170");
          validCounties.add("G0601150");
          validCounties.add("G0600070");
          validCounties.add("G0600910");
          validCounties.add("G0601010");
          validCounties.add("G0800690"); // Larimer
          validCounties.add("G0801230"); // Weld
          validCounties.add("G0800130"); // Boulder


          
          if(!validCounties.contains(county))
                return;
          Integer aqiScore = Integer.parseInt(line[1]);
          LocalDate epochDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(line[2])), ZoneOffset.UTC).toLocalDate();
          WeekFields weekFields = WeekFields.of(Locale.getDefault()); 
          int weekNumber = epochDate.get(weekFields.weekOfWeekBasedYear());
          int year = epochDate.getYear();
          String yearWeekCountyStr = String.format("%d,%02d,%s", year, weekNumber, county);
          yearWeekCounty.set(yearWeekCountyStr); // key
          aqi.set(aqiScore); // value    
          context.write(yearWeekCounty, aqi); // <YearWeekCounty, Aqi>
        }
      }
      
      public static class UniqueWeekAverageReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {

        private DoubleWritable meanAqiValue = new DoubleWritable();
        
        public void reduce(Text key, Iterable<IntWritable> aqis, Context context) throws IOException, InterruptedException {
          Double sum = 0.0;
          int size = 0;
          for (IntWritable value : aqis) {
            sum += value.get();
            size++;
          }
          Double meanAqiForEachWeek = sum / size; // mean of AQI in that year, in that week, in that county.
          Text keyCopy = new Text(key);
          meanAqiValue.set(meanAqiForEachWeek);
          context.write(keyCopy, meanAqiValue); // <YearWeekCounty, AverageAqi>
        }
      }

    public static class WeekCountyYearAqi extends Mapper<Object, Text, Text, Text> {

        private Text weekCounty = new Text();
        private Text yearAqi = new Text();
  
        public void map(Object lineNumber, Text lineValue, Context context) throws IOException, InterruptedException {
            String[] line = lineValue.toString().split(","); // Year Week County AvgAqi
            String year = line[0];
            String week = line[1];
            String county = line[2];
            Double avgAqi = Double.parseDouble(line[3]);
            String value = String.format("%s,%f", year, avgAqi); // year aqi 
            String key = String.format("%s,%s", week, county); // Week county 
            weekCounty.set(key);
            yearAqi.set(value);
            context.write(weekCounty, yearAqi);                 
        }

    }

    public static class CountyWeekYearToYearDiff extends Reducer<Text,Text,IntWritable,Text> { 

    ArrayList<String> countyWeekYearToYearDiff = new ArrayList<>();

    public void reduce(Text weekCounty, Iterable<Text> yearAqis, Context context) throws IOException, InterruptedException {

      TreeMap<String, String> sortYear = new TreeMap<>(); 

      for(Text yearAqi : yearAqis){
        String yearAqiString = yearAqi.toString();
        String[] line = yearAqiString.split(",");
        sortYear.put(line[0], line[1]); // sort by year
      }

      ArrayList<String> listOfAqiInOrder = new ArrayList<>();
      ArrayList<String> listOfYearInAqiOrder = new ArrayList<>();
      for(Map.Entry<String,String> entry : sortYear.entrySet()) {
        String value = entry.getValue();
        String key = entry.getKey();
        listOfAqiInOrder.add(value);
        listOfYearInAqiOrder.add(key);
      }
      Double maxChange = 0.0;
      String yearToYear = "";
      for(int i = 0;i<listOfAqiInOrder.size()-1;i++){
        if(Math.abs(Double.parseDouble(listOfAqiInOrder.get(i+1)) - Double.parseDouble(listOfAqiInOrder.get(i))) > Math.abs(maxChange)) {
          maxChange = Double.parseDouble(listOfAqiInOrder.get(i+1)) - Double.parseDouble(listOfAqiInOrder.get(i));
          yearToYear = listOfYearInAqiOrder.get(i+1) + '-' + listOfYearInAqiOrder.get(i);
        }
      }

      String[] weekCountyStrArr = weekCounty.toString().split(",");
      String week = weekCountyStrArr[0];
      String county = weekCountyStrArr[1];
     // context.write(new IntWritable(maxChange.intValue()), new Text(county + "," + week + "," + yearToYear));
      countyWeekYearToYearDiff.add(""+ String.format("%014.8f", Math.abs(maxChange)) + "," + county + "," + week + "," + yearToYear);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        countyWeekYearToYearDiff.sort(null);
        IntWritable key = new IntWritable();
        Text value = new Text();
        for (int i = 0; i < 25; ++i) {
            key.set(i);
            value.set(countyWeekYearToYearDiff.get(countyWeekYearToYearDiff.size() - 1 - i));
            context.write(key, value);
        }
    }


  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    Job job = Job.getInstance(conf, "WeekYearCountyMeanAqi");
    job.setJarByClass(Q6.class);
    job.setMapperClass(YearWeekCountyAqiMapper.class);
    job.setReducerClass(UniqueWeekAverageReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Top 10 year to year diffs");
    job2.setJarByClass(Q6.class);
    job2.setMapperClass(WeekCountyYearAqi.class);
    job2.setReducerClass(CountyWeekYearToYearDiff.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[2]));
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}