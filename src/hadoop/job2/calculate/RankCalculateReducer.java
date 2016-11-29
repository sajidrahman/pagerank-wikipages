package hadoop.job2.calculate;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {
    
	private Map<Text, String> pageRankMap = new HashMap<Text, String>();
	private double normalizationFactor = 0.0;
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException { //TODO: mess with
    	// d=0.85 is the normal damping factor
    	double d = 0.85;
    	boolean isExistingWikiPage = false;
        double sOtherPR = 0;
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
            double currentPageRank = Double.valueOf(sections[1]);
            int outboundLinkCount = Integer.valueOf(sections[2]);
 
            // Add the given pagerank to the running total for the other pages
            sOtherPR += currentPageRank / outboundLinkCount;
            }
        }
        
        // if the current page is not an original listed article in the corpus, then return
        if(!isExistingWikiPage) return;
        //else
        // Calculate pagerank by applying the damping factor to the sum
//        normalizationFactor += sOtherPR;
        Double pageRank = (1-d) + d * sOtherPR ;
        
        //now put the page with calculated pagerank and links in the map
//       pageRankMap.put(page, pageRank.toString() + "\t" + links);
       
       context.write(page, new Text(pageRank.toString() + "\t" + links));
        

       
    }
    
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException{
//    	double c = 1.0/normalizationFactor;
//    	
//    	for(Text key: pageRankMap.keySet()){
//    		String[] tempArray = pageRankMap.get(key).split("\\t");
//    		double updatedPageRank = ( c * Double.parseDouble(tempArray[0]));
//    		 // output page with normalized page-rank
//            context.write(key, new Text(updatedPageRank + "\t" + tempArray[1]));
//    	}
//    }
    
}
