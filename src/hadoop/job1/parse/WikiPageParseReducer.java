package hadoop.job1.parse;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class WikiPageParseReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> linkSet = new HashSet<String>();
        
        for(Text val: values){
        	// remove duplication in outgoing links
        	linkSet.add(val.toString());
        }


        if(linkSet.contains("#")){
            Iterator<String> i = linkSet.iterator();
            while ( i.hasNext()){
                String val = (String) i.next();
                if ( !val.equals("#"))
                    context.write(new Text(val), key);
                else {
                    context.write( key, new Text("#"));
                }
            }
        }
        
//        context.write(key, new Text(rankWithOutgoingLinks));
    }
}
