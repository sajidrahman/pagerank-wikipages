package hadoop.job0.preprocess;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class WikiCorpusSizeCalculatorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	int sum = 0;
    	Iterator<IntWritable> itr = values.iterator();
    	
    	while(itr.hasNext()){
    		sum += itr.next().get();
    	}

        context.write(key, new IntWritable(sum));
    }
}
