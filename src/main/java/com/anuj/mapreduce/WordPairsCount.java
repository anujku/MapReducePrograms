package com.anuj.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Master class that controls the reducers, mappers and combiners.
 * 
 * @author anujku
 * 
 */

public class WordPairsCount {

  /**
   * This class Counts the word pairs in each line of each document. For each
   * line of input, break the line into words, make wordPair as
   * sorted(previousWord, currentWord) and emit them as (<b>wordPair</b>,
   * <b>1</b>).
   * 
   * @author anujku
   * 
   */

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static String TAB = "\t";

    /**
     * This method takes each documents and counts the word pairs in each line
     * of each document. For each line of input, break the line into words,
     * makes wordPair as sorted(previousWord, currentWord) and emit them as
     * (<b>wordPair</b>, <b>1</b>).
     * 
     * @param key
     *          , value, context: Key = DocumentId, Value = The document,
     *          Context = InputContext
     * @throws IOException
     *           , InterruptedException
     * @return void
     */
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {

      String line = value.toString();
      StringTokenizer stringTokenizer = new StringTokenizer(line);
      String preWord = stringTokenizer.nextToken();
      String wordPair = null;

      while (stringTokenizer.hasMoreElements()) {
        String curWord = stringTokenizer.nextToken();

        List<String> wordPairs = new ArrayList<String>(2);
        wordPairs.add(curWord.toLowerCase());
        wordPairs.add(preWord.toLowerCase());

        Collections.sort(wordPairs);

        wordPair = wordPairs.get(0) + TAB + wordPairs.get(1);
        preWord = curWord;
        word.set(wordPair);
        context.write(word, one);
      }
    }
  }

  /**
   * A combiner class that just emits the sum of the input values.
   * 
   * @author anujku
   * 
   */
  public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      context.write(key, new IntWritable(sum));
    }
  }

  /**
   * A reducer class that just emits the sum of the input values if the sum >
   * WORDPAIR_COUNT_THRESHOLD .
   * 
   * @author anujku
   * 
   */

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static int WORDPAIR_COUNT_THRESHOLD = 100000;

    /**
     * A reducer class that just emits the sum of the input values if the sum >
     * WORDPAIR_COUNT_THRESHOLD.
     * 
     * @param key
     *          , value, context: Key = A Word Pair, Value = All the counts for
     *          that wordPair
     * @throws IOException
     *           , InterruptedException
     * @return void
     */

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (sum > WORDPAIR_COUNT_THRESHOLD)
        context.write(key, new IntWritable(sum));
    }
  }

  /**
   * Main master method to set the job, mappers, reducers, input and output
   * formats.
   * 
   * @param args
   *          : InputFilesPath OutputPath
   * @throws Exception
   */

  public static void main(String[] args) throws Exception {

    String jobName = "wordpairscount";

    Configuration configuration = new Configuration();

    Job job = new Job(configuration);
    job.setJobName(jobName);
    job.setJarByClass(WordPairsCount.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(Combine.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

  }
}
