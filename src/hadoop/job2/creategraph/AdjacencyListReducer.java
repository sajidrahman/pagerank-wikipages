package hadoop.job2.creategraph;


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


public class AdjacencyListReducer extends Reducer<Text, Text, Text, Text> {
	
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
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double pageRank = 1.0 /corpusSize;

        StringBuilder temp = new StringBuilder();
        temp.append(pageRank+"\t");
        Iterator<Text> itr = values.iterator();
        	
        	
            while(itr.hasNext())
            {
                String val = itr.next().toString();

                if ( !val.equals("#") )
                    temp.append(val + ";");
            }

            if (temp.length() > 0)
                temp.deleteCharAt(temp.length() - 1);

            // emit <source-page initial-pagerank list-of-linked-pages>
            context.write(key, new Text(temp.toString()));
    }
}
