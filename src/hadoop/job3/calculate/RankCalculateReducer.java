package hadoop.job3.calculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {
	
	private static Integer corpusSize = null;
	@SuppressWarnings("rawtypes")
	public void setup(org.apache.hadoop.mapreduce.Reducer.Context context){
		Configuration conf = context.getConfiguration();
		String location = conf.get("corpus.size.path");
		
		
		if (location != null) {
//			System.out.println("stoplist found");
			BufferedReader br = null;
			try {
				FileSystem fs = FileSystem.get(conf);
				Path path = new Path(location);
				if (fs.exists(path)) {
					FSDataInputStream fis = fs.open(path);
					br = new BufferedReader(new InputStreamReader(fis));
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] temp = line.split("\\s");
						corpusSize = Integer.parseInt(temp[1]);
					}
				}
			} catch (IOException e) {
				System.out.println(e.toString());
			} finally {
				IOUtils.closeStream(br);
			}
		}
		
	}
    
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException { //TODO: mess with

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

        Double pageRank = d/corpusSize + (1-d) * rankVote ;

       if(isSinkPage){
    	   context.write(page, new Text(pageRank.toString()));
       }else{
    	   context.write(page, new Text(pageRank.toString() + "\t" + links));
       }

       
    }

    
}
