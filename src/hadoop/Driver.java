package hadoop;

import hadoop.job0.preprocess.*;
import hadoop.job1.parse.WikiPageParseMapper;
import hadoop.job1.parse.WikiPageParseReducer;
import hadoop.job1.parse.XmlInputFormat;
import hadoop.job2.calculate.*;
import hadoop.job3.rank.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class Driver extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
    	
    	calculateCorpusSize(args[0], "wiki-corpus-size");
    	String corpusSize = readFile();
    	
        boolean isCompleted = runWikiPageParsing(args[0], args[1], corpusSize);
        if (!isCompleted) return 1;

        String lastResultPath = null;

        for (int runs = 0; runs < 16; runs++) {
            String inPath = "wiki/ranking/iter" + nf.format(runs);
            lastResultPath = "wiki/ranking/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }
        
        isCompleted = sortPageRank(lastResultPath, "wiki/result");
        
        System.out.println("Is completed: "+isCompleted);
        if (!isCompleted) return 1;
        return 0;
    }


    public boolean calculateCorpusSize(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");


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
    
    
    public boolean runWikiPageParsing(String inputPath, String outputPath, String size) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(hadoop.job1.parse.XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(hadoop.job1.parse.XmlInputFormat.END_TAG_KEY, "</page>");
        conf.set("size", size);
        
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
        xmlParser.setReducerClass(hadoop.job1.parse.WikiPageParseReducer.class);

        return xmlParser.waitForCompletion(true);
    }

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

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
    
    private boolean sortPageRank(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		@SuppressWarnings("deprecation")
		Job rankOrdering = new Job(getConf(), "sort-page-rank");
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

			br = new BufferedReader(new FileReader("wiki-corpus-size/part-r-00000"));

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
}
