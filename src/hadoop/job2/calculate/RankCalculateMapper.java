package hadoop.job2.calculate;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

	
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { //TODO: mess with
    	
    	// Gets the string of the value 
    	String valueStr = value.toString();
    	String[] sections = valueStr.split("\\t");

    	// Gets the mapped page
        String mappedPage = sections[0];
        
        // Gets the [thisPage]	[thisPagesRank]
        String mappedPageStr = sections[0] + "\t" + sections[1] + "\t";
        
        // Ignore if page contains no links
        if(sections.length < 3 || sections[2] == "")
        {
        	context.write(new Text(mappedPage), new Text("!"));
        	return;
        }
        
        // Mark page as an Existing page (ignore red wiki-links)
        context.write(new Text(mappedPage), new Text("!"));

        // Get the sink pages and total no. of outgoing links of the source
        String linkedPages = sections[2];
        String[] sinkPages = linkedPages.split(",");
        int total = sinkPages.length;
 
        // For each sink-page, store [sink-Page]    [sourcePageRank]    [totalNumberOfOutgoingLinksOfSource]
        for (String page : sinkPages) {
            context.write(new Text(page), new Text(mappedPageStr + total));
        }
 
        // Adds original links for preservation
        context.write(new Text(mappedPage), new Text("|" + linkedPages));
    
    }
    
}
