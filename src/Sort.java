package my.hadoop.test;

import java.io.IOException;
import java.lang.NumberFormatException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class Sort {
    
    public static class MapperClass
    extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
        
        private LongWritable pt = new LongWritable(0);
        private LongWritable id = new LongWritable(0);
        
        public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
                
            String [] splits = value.toString().split("\t");
            pt.set(Long.parseLong(splits[0]));
            id.set(Long.parseLong(splits[1]));
                
            try {
                context.write(pt, id);
            } catch (Exception e) {
                System.err.println("[Sort map] " + e.getMessage());
            }
        }
    }
    
    public static class ReducerClass
    extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
        
        private Long ranking = new Long(1);
        private Long count  = new Long(0);
        private Long prevPt = new Long(0);
        
        private Text info = new Text();
        
        public void reduce(LongWritable pt, Iterable<LongWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            try {
                long curPt = pt.get();
                
                if (curPt < prevPt) {
                    ++ranking;
                }
                prevPt = curPt;
                
                for (LongWritable val : values) {
                    ++count;
                    info.set(ranking.toString() + "\tID:" + val.get() + "\tPT:" + Long.toString(curPt));
                    context.write(info, null);
                }
            } catch (Exception e) {
                System.err.println("[Sort reduce] " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    
    public static class SortKeyComparator extends WritableComparator {
        protected SortKeyComparator() {
            super(LongWritable.class, true);
        }
        
        public int compare(WritableComparable a, WritableComparable b) {
            int result = 0;
            
            long aVal = 0;
            long bVal = 0;
            
            try {
                if (a instanceof LongWritable) {
                    aVal = ((LongWritable)a).get();
                    bVal = ((LongWritable)b).get();
                    //System.err.println("11111111[on Compare] " + aVal + "<->" + bVal);
                } else if (a instanceof Text) {
                    aVal = Long.parseLong(((Text)a).toString());
                    bVal = Long.parseLong(((Text)b).toString());
                    //System.err.println("22222222[on Compare] " + aVal + "<->" + bVal);
                }
            
                if (aVal > bVal) {
                    result = -1;
                } else if (aVal < bVal) {
                    result = 1;
                } else {
                    result = 0;
                }
            } catch (Exception e) {
                System.err.println("[on Compare]" + a.toString() + "<->" + b.toString() + "<->" + e.getMessage());
            }
            
            //System.err.println("[on compare] result :" + result);
            return result;
        }
    }
    
    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.err.println("Usage: Sort <in> <out> <reducers>");
                System.exit(2);
            }
        
            runJob(args[0], args[1], Integer.parseInt(args[2]));
        } catch (Exception e) {
            System.err.println("[Sort runJob] " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void runJob(String input, String output, int numReducer) throws
    IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "sort by point");
        job.setJarByClass(Sort.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(numReducer);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setSortComparatorClass(SortKeyComparator.class);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("input", "partitioning"));
        
        double  pcnt        = 0.1;
        int     numSample   = numReducer;
        int     maxSplit    = numReducer - 1;
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt, numSample, maxSplit);
        try {
            InputSampler.writePartitionFile(job, sampler);
        } catch (Exception e) {
            System.err.println("numReduceTask:" + numReducer + " " + e.getMessage());
            e.printStackTrace();
        }
        
        job.waitForCompletion(true);
    }
}
