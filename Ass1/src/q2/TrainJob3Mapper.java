package q2;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// How​ ​ many​ ​ trains​ ​ are​ ​ travelling​ ​ in​ ​ each​ ​ direction​ ​ in​ ​ each​ ​ hour
public class TrainJob3Mapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);


  private TrainRecordParser parser = new TrainRecordParser();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    if (parser.isValidRecord(value.toString())) {
    	parser.parse(value.toString());
      context.write(new Text(parser.gettrainDirection()+"\t"+parser.gettrainTimeHour()),one);
    }
    else
    {
		context.getCounter("Custom Counter", "number of Bad Records" ).increment(1);
    }
  }
}
