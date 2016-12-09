package hadoop.job31.normalize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public  class RankNormalizationMapper extends Mapper<LongWritable, Text, Text, Text> {
		float sumRanks,normalizationFactor = 1;

		public void setup(Context context){
			Configuration conf = context.getConfiguration();

			String location = conf.get("job.inputfile.path");

			ArrayList<Float> pageRankList = new ArrayList<Float>();
//			System.out.println("Input Location: "+location);
			if(location != null){
				BufferedReader br = null;
				try{
					FileSystem fs = FileSystem.get(conf);
					Path path = new Path(location);
					if(fs.exists(path)){
						FSDataInputStream fis = fs.open(path);
						br = new BufferedReader(new InputStreamReader(fis));
						String line =null;
						while((line = br.readLine()) != null && line.trim().length()>0){
							pageRankList.add(Float.parseFloat(line.split("\t")[1]));
						}

						for(Float pageRank : pageRankList )
						{
							sumRanks += pageRank;
						}
						normalizationFactor = 1/sumRanks;	
					}
				}
				catch(IOException e){
					//handle
				}
				finally{
					IOUtils.closeStream(br);
				}
			}

		}


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");
			Text links = new Text();

			Text page = new Text(parts[0]);
			float pageRank = Float.parseFloat(parts[1]) * normalizationFactor;
			if(parts.length > 2)
				links.set(pageRank + "\t" +parts[2]);
			else
				links.set(pageRank + "\t");

			context.write(page, links);
		}
	}



