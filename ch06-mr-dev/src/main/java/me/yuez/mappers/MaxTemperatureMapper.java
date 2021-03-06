package me.yuez.mappers;

import me.yuez.utils.NcdcRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);

        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();

            if (airTemperature > 1000) {
                System.err.println("Temperature over 100 degress for input: " + value);
                context.setStatus("Detected possibly coorupt record: see logs");
                context.getCounter(Temperature.OVER_100).increment(1);
            }

            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        }
    }

    enum Temperature {
        OVER_100
    }
}
