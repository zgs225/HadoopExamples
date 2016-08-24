package me.yuez.jobs;

import me.yuez.reducers.MaxTemperatureReducer;
import me.yuez.utils.JobBuilder;
import me.yuez.utils.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MaxTemperatureWithCounters extends Configured implements Tool {

    enum Temperature {
        MISSING,
        MALFORMED
    }

    private static class MaxTemperatureMapperWithCounters
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);

            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
            } else if (parser.isMalformedTemperature()) {
                System.err.println("Ignoring possibly corrupt input: " + value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            } else if (parser.isMissingTemperature()) {
                context.getCounter(Temperature.MISSING).increment(1);
            }

            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseIntputAndOutput(this, getConf(), args);
        if (job == null)
            return -1;

        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
        System.exit(exitCode);
    }
}
