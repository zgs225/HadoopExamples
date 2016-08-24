package me.yuez.jobs;

import me.yuez.utils.JobBuilder;
import me.yuez.utils.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PartitionStationUsingMultipleOutputs extends Configured implements Tool {

    private static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new Text(parser.getStationId()), value);
        }
    }

    private static class MultipleOutputReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                multipleOutputs.write(NullWritable.get(), value, key.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseIntputAndOutput(this, getConf(), args);
        if (job == null)
            return -1;

        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(MultipleOutputReducer.class);
        job.setOutputKeyClass(NullWritable.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionStationUsingMultipleOutputs(), args);
        System.exit(exitCode);
    }
}
