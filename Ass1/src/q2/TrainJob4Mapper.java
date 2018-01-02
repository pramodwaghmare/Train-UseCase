package q2;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// What​ ​ is​ ​ the​ ​ avg​ ​ speed​ ​ of​ ​ each​ ​ train​ ​ travelling​ ​ in​ ​ each​ ​ direction in​ ​ each​ ​ hour
public class TrainJob4Mapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
	


  private TrainRecordParser parser = new TrainRecordParser();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    if (parser.isValidRecord(value.toString())) {
    	parser.parse(value.toString());
      context.write(new Text(parser.gettrainNumber()+"\t"+parser.gettrainTimeHour()),new IntWritable(parser.gettrainSpeed()));
    }
    else
    {
		context.getCounter("Custom Counter", "number of Bad Records" ).increment(1);
    }
  }
}
