



package q2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TrainJob4Reducer
  extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	 

  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
	  double sum = 0;
	  int count=0;
		for(IntWritable val : values) {
			sum = sum + val.get();
			count= count+1;
			
		}
		
		double avg =(double)sum/count;
		context.write(key, new DoubleWritable(avg));
  }
}
