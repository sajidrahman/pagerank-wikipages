package hadoop;

import hadoop.job0.preprocess.WikiCorpusSizeCalculatorMapper;
import hadoop.job0.preprocess.WikiCorpusSizeCalculatorReducer;
import hadoop.job1.parse.WikiPageParseMapper;
import hadoop.job1.parse.WikiPageParseReducer;
import hadoop.job1.parse.XmlInputFormat;
import hadoop.job2.creategraph.AdjacencyListMapper;
import hadoop.job2.creategraph.AdjacencyListReducer;
import hadoop.job3.calculate.RankCalculateMapper;
import hadoop.job3.calculate.RankCalculateReducer;
import hadoop.job31.normalize.RankNormalizationMapper;
import hadoop.job4.rank.SortRankMapper;
import hadoop.job4.rank.SortRankReducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
    	//job 0 : calculate total number of pages
    	calculateTotalPages(args[0], "wiki/size");
//    	String totalPage = readFile();
    	
    	//job 1: extract wiki pages and valid outgoing links from a page
        boolean isCompleted = runWikiXMLParser(args[0], "wiki/adjacencylist/stage1");
        
        //job 2: remove red links and generate adjacency graph
        isCompleted = runAdjacencyListGenerator("wiki/adjacencylist/stage1", "wiki/ranking/iter00");
        
        if (!isCompleted) return 1;

        String lastResultPath = null;
        String lastNormalizedResultPath = null;
        boolean first = true;
        String inPath = null;
        
        // job 3: iterative map reduce, calculate page rank
        for (int runs = 0; runs < 8; runs++) {
        	if(first){
        		 inPath = "wiki/ranking/iter" + nf.format(runs);
        		 first = false;
        	}
        	else
        		inPath = "wiki/ranking/normalized/iter" + nf.format(runs);

            
            lastResultPath = "wiki/ranking/iter" + nf.format(runs + 1);
            lastNormalizedResultPath = "wiki/ranking/normalized/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);
            // job 3.1: normalize page rank values
            isCompleted = runNormalizer(lastResultPath, lastNormalizedResultPath);

            if (!isCompleted) return 1;
        }
        
        //job 4: rank page in the descending order of pagerank values
        isCompleted = sortPageRank(lastNormalizedResultPath, "wiki/result");
        
        if (!isCompleted) return 1;
        return 0;
    }


    public boolean calculateTotalPages(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        conf.set("dfs.replication", "1");
        conf.set("mapreduce.client.submit.file.replication", "1");


        Job corpusSize = Job.getInstance(conf, "corpus-size");
        corpusSize.setJarByClass(Driver.class);

        // Input / Mapper
        FileInputFormat.addInputPath(corpusSize, new Path(inputPath));
        corpusSize.setInputFormatClass(XmlInputFormat.class);
        corpusSize.setMapperClass(WikiCorpusSizeCalculatorMapper.class);
        corpusSize.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(corpusSize, new Path(outputPath));
      
        corpusSize.setOutputKeyClass(Text.class);
        corpusSize.setOutputValueClass(IntWritable.class);
        corpusSize.setReducerClass(WikiCorpusSizeCalculatorReducer.class);

        return corpusSize.waitForCompletion(true);
    }
    
    
    public boolean runWikiXMLParser(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(hadoop.job1.parse.XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(hadoop.job1.parse.XmlInputFormat.END_TAG_KEY, "</page>");

		conf.set("dfs.replication", "1");
		conf.set("mapreduce.client.submit.file.replication", "1");
        
        Job xmlParser = Job.getInstance(conf, "xml-parser");
        xmlParser.setJarByClass(Driver.class);

        // Input / Mapper
        FileInputFormat.addInputPath(xmlParser, new Path(inputPath));
        xmlParser.setInputFormatClass(XmlInputFormat.class);
        xmlParser.setMapperClass(WikiPageParseMapper.class);
        xmlParser.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlParser, new Path(outputPath));
        xmlParser.setOutputFormatClass(TextOutputFormat.class);

        xmlParser.setOutputKeyClass(Text.class);
        xmlParser.setOutputValueClass(Text.class);
        xmlParser.setReducerClass(WikiPageParseReducer.class);

        return xmlParser.waitForCompletion(true);
    }

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("corpus.size.path", "wiki/size/part-r-00000");

		conf.set("dfs.replication", "1");
		conf.set("mapreduce.client.submit.file.replication", "1");
        
        Job rankCalculator = Job.getInstance(conf, "rank-calculator");
        rankCalculator.setJarByClass(Driver.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReducer.class);

        return rankCalculator.waitForCompletion(true);
    }
    
    private boolean runAdjacencyListGenerator(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("corpus.size.path", "wiki/size/part-r-00000");

		conf.set("dfs.replication", "1");
		conf.set("mapreduce.client.submit.file.replication", "1");

        Job graphGenerator = Job.getInstance(conf, "adjacency-graph");
        graphGenerator.setJarByClass(Driver.class);

        graphGenerator.setOutputKeyClass(Text.class);
        graphGenerator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(graphGenerator, new Path(inputPath));
        FileOutputFormat.setOutputPath(graphGenerator, new Path(outputPath));

        graphGenerator.setMapperClass(AdjacencyListMapper.class);
        graphGenerator.setReducerClass(AdjacencyListReducer.class);

        return graphGenerator.waitForCompletion(true);
    }
    
    private boolean sortPageRank(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
    	
		Configuration conf = new Configuration();

		conf.set("dfs.replication", "1");
		conf.set("mapreduce.client.submit.file.replication", "1");

		Job rankOrdering = Job.getInstance(conf, "sort-page-rank");
		rankOrdering.setJarByClass(Driver.class);

		rankOrdering.setMapOutputKeyClass(DoubleWritable.class);
		rankOrdering.setMapOutputValueClass(Text.class);
		rankOrdering.setOutputKeyClass(DoubleWritable.class);
		rankOrdering.setOutputValueClass(Text.class);
		
		
		FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));
		
		rankOrdering.setSortComparatorClass(KeyComparator.class);
		
		rankOrdering.setMapperClass(SortRankMapper.class);
		rankOrdering.setNumReduceTasks(1);
		rankOrdering.setReducerClass(SortRankReducer.class);
		
		return rankOrdering.waitForCompletion(true);
	}
    

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(DoubleWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable d1 = (DoubleWritable) w1;
            DoubleWritable d2 = (DoubleWritable) w2;
            int cmp = d1.compareTo(d2);
            return cmp * -1; //reverse
        }
    }
    
    private String readFile(){
    	String size = "";
    	BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader("wiki/size/part-r-00000"));

			while ((sCurrentLine = br.readLine()) != null) {
				String[] temp = sCurrentLine.split("\\s");
				size = temp[1];
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
		return size;
    }
    
	private boolean runNormalizer(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		conf.set("dfs.replication", "1");
		conf.set("mapreduce.client.submit.file.replication", "1");

		conf.set("job.inputfile.path", inputPath);

		Job normalizer = Job.getInstance(conf, "rank-normalizer");
		normalizer.setJarByClass(Driver.class);

		normalizer.setOutputKeyClass(Text.class);
		normalizer.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(normalizer, new Path(inputPath));
		FileOutputFormat.setOutputPath(normalizer, new Path(outputPath));

		normalizer.setMapperClass(RankNormalizationMapper.class);

		FileSystem fs = FileSystem.get(conf);
		/*Check if output path (args[1])exist or not*/
		if(fs.exists(new Path(outputPath))){
			/*If exist delete the output path*/
			fs.delete(new Path(outputPath),true);
		}

		return normalizer.waitForCompletion(true);
	}
    
}
