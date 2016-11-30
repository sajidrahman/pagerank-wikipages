package hadoop.job2.creategraph;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class AdjacencyListMapper extends Mapper<LongWritable, Text, Text, Text> {
    
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	String svalues = value.toString();
        String[] array = svalues.split("\t");

        context.write(new Text(array[0]), new Text(array[1]));     
        
    }
    
}
