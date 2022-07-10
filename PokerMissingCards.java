import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class PokerMissingCards {
	
	final static int cardsPerSuite = 13;
	
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = new Job(config, "pokermissingcards");
        job.setJarByClass(PokerMissingCards.class);
        job.setMapperClass(PokerMapper.class);
        job.setReducerClass(PokerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class PokerMapper extends Mapper < LongWritable, Text, Text, IntWritable > {

        public void map(LongWritable lw, Text t, Context c) throws IOException, InterruptedException {
            String line = t.toString();
            String[] split = line.split(" - ");                        
			c.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])));
        }
    }

    public static class PokerReducer extends Reducer < Text, IntWritable, Text, IntWritable > {

        public void reduce(Text t, Iterable < IntWritable > iw, Context c) throws IOException, InterruptedException {
        	int cardRank = 0;
            ArrayList < Integer > tempSuite = new ArrayList < Integer > ();
            for (int i = 1; i <= cardsPerSuite; ++i) {
                tempSuite.add(i);
            }
            for (IntWritable cards: iw) {
                cardRank = cards.get();
                if (tempSuite.contains(cardRank)) {
                    tempSuite.remove(tempSuite.indexOf(cardRank));
                }
            }
            for (int i = 0; i < tempSuite.size(); ++i) {
                c.write(t, new IntWritable(tempSuite.get(i)));
            }
        }
    }
}