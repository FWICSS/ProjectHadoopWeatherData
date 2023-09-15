package chapter1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EcartTypeTemp {

    public static class TemperatureMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        private DoubleWritable temperature = new DoubleWritable();
        private Text station = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(";");
                String state = tokens[1];

                if (state.equals("2")) {
                    // Get the temperature as a double
                    double temp = Double.parseDouble(tokens[2]);
                    // Set the temperature as the value
                    temperature.set(temp);
                    // Emit the temperature with a constant key
                    station.set("");
                    context.write(station, temperature);
                }

        }

    }

    public static class TemperatureReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<Double> temperatureList = new ArrayList<Double>();
            for (DoubleWritable val : values) {
                temperatureList.add(val.get());
            }
            double mean = calculateMean(temperatureList);
            double sum = 0.0;
            for (Double temperature : temperatureList) {
                sum += Math.pow(temperature - mean, 2);
            }
            double variance = sum / (temperatureList.size() - 1);
            double stdDeviation = Math.sqrt(variance);
            result.set(stdDeviation);
            context.write(key, result);
        }

        private double calculateMean(List<Double> values) {
            double sum = 0.0;
            for (Double value : values) {
                sum += value;
            }
            return sum / values.size();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ecart-type temperature");
        job.setJarByClass(EcartTypeTemp.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}