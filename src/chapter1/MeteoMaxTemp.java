import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MeteoMaxTemp {

    public static class NumberMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable temperature = new IntWritable();
        private Text lineNumber = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nums = value.toString().split(";");

            int temp = Integer.parseInt(nums[2]);
            temperature.set(temp);
            lineNumber.set(key.toString()); // Utilisation du numéro de ligne comme clé
            context.write(lineNumber, temperature);
        }
    }

    public static class NumberReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable(Integer.MIN_VALUE);
        private boolean lastKeyProcessed = false;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable value : values) {
                if (value.get() > result.get()) {
                    result.set(value.get());
                }

            }// Marquer la clé comme traitée


        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MeteoMaxTemp <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Météo max");
        job.setJarByClass(MeteoMaxTemp.class);
        job.setMapperClass(NumberMapper.class);
        job.setReducerClass(NumberReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
