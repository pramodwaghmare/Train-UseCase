package q2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Total​ ​ how​ ​ many​ ​ train​ ​ data​ ​ in​ ​ present​ ​ in​ ​ the​ ​ input​ ​ data
public class TrainJob1Mapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private final static Text records = new Text("Records");


  private TrainRecordParser parser = new TrainRecordParser();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    if (parser.isValidRecord(value.toString())) {
      context.write(records,one);
    }
    else
    {
		context.getCounter("Custom Counter", "number of Bad Records" ).increment(1);
    }
  }
}
