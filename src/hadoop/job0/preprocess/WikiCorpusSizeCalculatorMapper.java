package hadoop.job0.preprocess;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WikiCorpusSizeCalculatorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);
        
        String pageTitle = titleAndText[0];
        
        if(isInvalidPage(pageTitle))
            return;
        context.write(new Text("size"), new IntWritable(1) );
        
    }
    
    private boolean isInvalidPage(String pageString) {
        return pageString.contains(":");
    }



    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
      //Find opening <text> tag, excluding potential attributes
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

}
