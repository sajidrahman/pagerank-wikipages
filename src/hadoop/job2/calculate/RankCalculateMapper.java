package hadoop.job2.calculate;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

 /***
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex+1);

        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);
        
        // Mark page as an Existing page (ignore red wiki-links)
        context.write(new Text(page), new Text("!"));

        // Skip pages with no links.
        if(rankTabIndex == -1) return;
        
        String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
        String[] allOtherPages = links.split(",");
        int totalLinks = allOtherPages.length;
        
        for (String otherPage : allOtherPages){
            Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
            context.write(new Text(otherPage), pageRankTotalLinks);
        }
        
        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|" + links));
    }
    ***/
	
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
        	context.write(new Text(mappedPage), new Text("! "));
        	return;
        }
        
        // Mark page as an Existing page (ignore red wiki-links)
        context.write(new Text(mappedPage), new Text("!"));

        // Gets the linked pages and [thisTotalNumberOfLinks]
        String linkedPages = sections[2];
        String[] pages = linkedPages.split(",");
        int total = pages.length;
 
        // For each linked to page, store [thisPage]    [thisPagesRank]    [thisTotalNumberOfLinks]
        for (String page : pages) {
            context.write(new Text(page), new Text(mappedPageStr + total));
        }
 
        // Adds original links for preservation
        context.write(new Text(mappedPage), new Text("|" + linkedPages));
    
    }
    
}
