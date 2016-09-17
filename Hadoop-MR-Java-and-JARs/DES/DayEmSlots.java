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

public class DayEmSlots {

  public static class Mapper1
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); //new array of 9 elements
    	if(fields[2].equals("Unknown")||fields[3].equals("UNKWN")||fields[3].equals("ARR")||fields[4].equals("Unknown"))return;
	String key1=fields[1];
	key1=key1.concat(",");
	key1=key1.concat(fields[2]);
	key1=key1.concat(",");
	key1=key1.concat(fields[4]);
	int c=0;
	for (IntWritable i=new IntWritable(0); i.get()<fields[3].length(); i.set(i.get()+1))
	{
		if (fields[3].length()>1)
		{		
			if (fields[3].charAt(1)=='-' )
			{	
				if (c==0)
				{
				c=1;
				String key2=key1;
				key2=key2.concat(",");
				key2=key2.concat("M");
				word.set(key2);
				context.write(word,new IntWritable(1));	
				key2=key1;
				key2=key2.concat(",");
				key2=key2.concat("T");
				word.set(key2);
				context.write(word,new IntWritable(1));	
				key2=key1;
				key2=key2.concat(",");
				key2=key2.concat("W");
				word.set(key2);
				context.write(word,new IntWritable(1));	
				key2=key1;
				key2=key2.concat(",");
				key2=key2.concat("R");
				word.set(key2);
				context.write(word,new IntWritable(1));	
				key2=key1;
				key2=key2.concat(",");
				key2=key2.concat("F");
				word.set(key2);
				context.write(word,new IntWritable(1));	
				if (fields[3].charAt(2)=='S')
				{
					key2=key1;
					key2=key2.concat(",");
					key2=key2.concat("S");
					word.set(key2);
					context.write(word,new IntWritable(1));				
				}
				}	
			}
			else {
			String key2=key1;
			key2=key2.concat(",");
			key2=key2.concat(Character.toString(fields[3].charAt(i.get())));
			word.set(key2);
			context.write(word,new IntWritable(1));				
			}
		}		
		else {
			String key2=key1;
			key2=key2.concat(",");
			key2=key2.concat(Character.toString(fields[3].charAt(i.get())));
			word.set(key2);
			context.write(word,new IntWritable(1));				
		}	
	}
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
	String key1=key0[0]+" "+key0[1]+" "+key0[3];
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
	String temp="Tempdes";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "getting total number of timeslots a class is accessed in a week");
    job.setJarByClass(DayEmSlots.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "getting total number of timeslots a class is used for in a day in a week");
    job2.setJarByClass(DayEmSlots.class);
    job2.setMapperClass(Mapper2.class);
    //job2.setCombinerClass(FindIncreaseReducer.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("Tempdes"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
