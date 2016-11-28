package hadoop.job2.calculate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {

/**    private static final float damping = 0.85F;

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExistingWikiPage = false;
        String[] split;
        float sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithRank;
        
        // For each otherPage: 
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        for (Text value : values){
            pageWithRank = value.toString();
            
            if(pageWithRank.equals("!")) {
                isExistingWikiPage = true;
                continue;
            }
            
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }

            split = pageWithRank.split("\\t");
            
            float pageRank = Float.valueOf(split[1]);
            int countOutLinks = Integer.valueOf(split[2]);
            
            sumShareOtherPageRanks += (pageRank/countOutLinks);
        }

        if(!isExistingWikiPage) return;
        float newRank = damping * sumShareOtherPageRanks + (1-damping);

        context.write(page, new Text(newRank + links));
    } **/
    
    
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException { //TODO: mess with
    	// d=0.85 is the normal damping factor
    	double d = 0.85;
    	boolean isExistingWikiPage = false;
        float sOtherPR = 0;
        String links = "";
        Iterator<Text> val = values.iterator();
        
        // For each page that links to the current
        while ((val).hasNext()) {
        	String pageStr = val.next().toString();
        	
            if(pageStr.equals("!")) {
                isExistingWikiPage = true;
                continue;
            }
 
        	// If it is a links record, add it to the links array
            if (pageStr.startsWith("|")) {
                links = pageStr.substring(1);
            } 
            // If it is a normal record however
            else {
            // Find the pagerank and number of links for the given page
            String[] sections = pageStr.split("\\t");
            float currentPageRank = Float.valueOf(sections[1]);
            int linkCount = Integer.valueOf(sections[2]);
 
            // Add the given pagerank to the running total for the other pages
            sOtherPR += currentPageRank / linkCount;
            }
        }
        
        // if the current page is not an original listed article in the corpus, then return
        if(!isExistingWikiPage) return;
        // Calculate pagerank by applying the dampening to the sum
        double pageRank = (1-d) + d * sOtherPR ;

        // Add new pagerank to total
        context.write(page, new Text(pageRank + "\t" + links));
    }
}
