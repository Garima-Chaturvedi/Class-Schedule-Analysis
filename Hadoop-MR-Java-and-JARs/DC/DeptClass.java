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

public class DeptClass {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); //new array of 9 elements
    	if(fields[5].equals("Unknown"))return;
	String key1=fields[3];
	key1=key1.concat(",");
	key1=key1.concat(fields[5]);
	key1=key1.concat(",");
	key1=key1.concat(fields[4]);
    	word.set(key1);
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
	String[] key1=fields[0].split(",");
	String key2=key1[0];
	key2=key2.concat(" ");
	key2=key2.concat(key1[1]);
	word.set(key2);
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
	String temp="Tempdc";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "get class, sem, department count");
    job.setJarByClass(DeptClass.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "get total departments per class");
    job2.setJarByClass(DeptClass.class);
    job2.setMapperClass(Mapper2.class);
    //job2.setCombinerClass(FindIncreaseReducer.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("Tempdc"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
