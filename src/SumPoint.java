package my.hadoop.test;

import java.io.IOException;
import java.lang.NumberFormatException;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SumPoint {
    
    public static class MapperClass
    extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
        
        private LongWritable id = new LongWritable(0);
        private LongWritable pt = new LongWritable(0);
        
        public void map(LongWritable key, Text value, Context context
                        ) throws IOException, InterruptedException {
            try {
                String [] splits = value.toString().split("\t");
                
                id.set(Long.parseLong(splits[0]));
                pt.set(Long.parseLong(splits[1]));
                
                if (true) {
                    context.write(id, pt);
                }
            }
            catch (NumberFormatException e) {}
            catch (Exception e) {
                System.err.println("[Sum map]" + e.getMessage());
            }
        }
    }
    
    public static class CombinerClass
    extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        
        private LongWritable result = new LongWritable(0);
        
        public void reduce(LongWritable key, Iterable<LongWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            
            result.set(sum);
            context.write(key, result);
        }
    }
    
    public static class ReducerClass
    extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        
        private LongWritable result = new LongWritable(0);
        
        public void reduce(LongWritable key, Iterable<LongWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            
            result.set(sum);
            context.write(result, key);
        }
    }
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: sum <in> <out>");
            System.exit(2);
        }
        
        try {
            runJob(args[0], args[1]);
        } catch (Exception e) {
            System.err.println("[Sum runJob]" + e.getMessage());
        }
    }
    
    public static void runJob(String input, String output) throws
    IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "sum point");
        job.setJarByClass(SumPoint.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        job.waitForCompletion(true);
    }
}
