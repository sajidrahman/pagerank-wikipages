package hadoop.job2.creategraph;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class AdjacencyListReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        Integer corpusSize = Integer.parseInt(config.get("size"));
        Double pageRank = 1.0 /corpusSize;

        StringBuilder temp = new StringBuilder();
        temp.append(pageRank+"\t");
        Iterator<Text> itr = values.iterator();

        if ( key.toString().equals("#")){
            while(itr.hasNext())
            {
                context.write(new Text(itr.next()), new Text(""));
            }

        }else{
        	
        	
            while(itr.hasNext())
            {
                String val = itr.next().toString();

                if ( !val.equals("#") )
                    temp.append(val + ";");
            }

            if (temp.length() > 0)
                temp.deleteCharAt(temp.length() - 1);

            // emit source-page initial-pagerank list-of-linked-pages
            context.write(key, new Text(temp.toString()));
        }
    }
}
