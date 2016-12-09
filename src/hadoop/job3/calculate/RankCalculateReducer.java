package hadoop.job3.calculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException { //TODO: mess with

      Configuration config = context.getConfiguration();
      Integer N = Integer.parseInt(config.get("size"));
    	double d = 0.15;
    	boolean isSinkPage = false;
        double rankVote = 0.0;
        String links = "";
        Iterator<Text> val = values.iterator();
        
        // For each page that links to the current
        while (val.hasNext()) {
        	String pageStr = val.next().toString();
 
        	// If it is a links record, add it to the links array
            if (pageStr.startsWith("|")) {
                links = pageStr.substring(1);
            }else if(pageStr.equals("#")){
            	isSinkPage = true;
            }
            // If it is a normal record however
            else {
            // Find the pagerank and number of links for the given page
            String[] sections = pageStr.split("\\t");
            double currentPageRank = Double.valueOf(sections[0]);
            int outboundLinkCount = Integer.valueOf(sections[1]);
 
            // Add the given pagerank to the running total for the other pages
            rankVote += currentPageRank / outboundLinkCount;
            }
        }

        Double pageRank = d/N + (1-d) * rankVote ;

       if(isSinkPage){
    	   context.write(page, new Text(pageRank.toString()));
       }else{
    	   context.write(page, new Text(pageRank.toString() + "\t" + links));
       }

       
    }

    
}
