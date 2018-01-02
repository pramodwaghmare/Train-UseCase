package q1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class WordcountMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    StringTokenizer st = new StringTokenizer(line," ");
	while(st.hasMoreTokens()){
		word.set(st.nextToken());
		context.write(word,one);
		if (word.toString().equalsIgnoreCase("and"))
		context.getCounter("Custom Counter", "number of AND/and in input" ).increment(1);

	}
  }
}
