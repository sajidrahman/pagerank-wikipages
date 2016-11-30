package hadoop.job4.rank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortRankMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// Get the page and split by tabs
    	String[] sections = value.toString().split("\\t");
    	
    	// Get the rank and the current page (ignoring the links at the end)
    	double rank = 10000 * Double.parseDouble(sections[1]);
    	String pageTitle = sections[0];
    	
    	// Output rank first
    	context.write(new DoubleWritable(rank), new Text(pageTitle));
    }
}
