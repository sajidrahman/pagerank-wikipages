package hadoop.job4.rank;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortRankReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
 public static final Log log = LogFactory.getLog(SortRankReducer.class);
  @Override
  public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  for (Text val: values){
		  // output will be in this format <page page-rank>
		  context.write(val, key);
	  }


  }
}
