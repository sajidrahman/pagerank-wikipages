package hadoop.job3.calculate;


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
        
        // Gets current	pagerank
        String mappedPageStr = sections[1] + "\t";
        
        // Ignore if page contains no links
        if(sections.length < 3 || sections[2] == "")
        {
        	// this is a sink-page
        	context.write(new Text(mappedPage), new Text("#"));
        	return;
        }

        // Get the linked pages and total no. of outgoing links of the source
        String linksInPages = sections[2];
        String[] linkedPages = linksInPages.split(";");
        int total = linkedPages.length;
 
        // For each linked-page, store [linked-Page]    [sourcePageRank]    [totalNumberOfOutgoingLinksOfSource]
        for (String page : linkedPages) {
            context.write(new Text(page), new Text(mappedPageStr + total));
        }
 
        // Adds original links for preservation
        context.write(new Text(mappedPage), new Text("|" + linksInPages));
    
    }
    
}
