package hadoop.job1.parse;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WikiPageParseReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        Integer corpusSize = Integer.parseInt(config.get("size"));
        Integer pageRank = 1/corpusSize;
    	String initialPageRank = pageRank+"\t";
        String rankWithOutgoingLinks = initialPageRank;

        boolean first = true;

        for (Text value : values) {
            if(!first) rankWithOutgoingLinks += ",";

            rankWithOutgoingLinks += value.toString();
            first = false;
        }

        context.write(key, new Text(rankWithOutgoingLinks));
    }
}
