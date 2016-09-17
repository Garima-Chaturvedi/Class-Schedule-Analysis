//Team Members: 1.Garima Pradeep Chaturvedi
//2.Namita Balkrishna Marathe

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExamSlots {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); //new array of 9 elements
	if(fields[11].equalsIgnoreCase("roomcapacity"))return;    	
	if(Integer.parseInt(fields[11])==0)return;
	String key0=fields[3];
	key0=key0.concat(",");
	key0=key0.concat(fields[5]);
	key0=key0.concat(",");
	key0=key0.concat(fields[7]);
	key0=key0.concat(",");
	key0=key0.concat(fields[11]);
	key0=key0.concat(",");
	key0=key0.concat(fields[8]);
	key0=key0.concat(" ");
	key0=key0.concat(fields[9]);
    	word.set(key0);
    	context.write(word,new IntWritable(1));
    }
  }

  public static class Reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int sum=0;
    	for(IntWritable val:values){
    		sum+=val.get();
    	}
    	result.set(sum);
    	context.write(key,result);
    }
  }
  public static class Mapper2
  extends Mapper<Object, Text, Text, IntWritable>{
  	Text word=new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields=value.toString().split("\\t"); 
	String[] key0=fields[0].split(",");
	String key1=key0[0]+" "+key0[1]+" "+key0[2]+" "+key0[3];
	word.set(key1);
	context.write(word,new IntWritable(1));
}
}

public static class Reducer2
  extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
	int sum=0;
    	for(IntWritable val:values){
    		sum+=val.get();
    	}
    	result.set(sum);
    	context.write(key,result);
}
}
  public static void main(String[] args) throws Exception {
	String temp="Tempes3";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "get total number of students for every year");
    job.setJarByClass(ExamSlots.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "get # of students increasing between years");
    job2.setJarByClass(ExamSlots.class);
    job2.setMapperClass(Mapper2.class);
    //job2.setCombinerClass(FindIncreaseReducer.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("Tempes3"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
