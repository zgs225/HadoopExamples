package me.yuez.mappers;

import me.yuez.mappers.MaxTemperatureMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MaxTemperatureMapperTest {

    @Test
    public void processValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
            "99999V0203201N00261220001CN9999999N9-00111+99999999999");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }

    @Test
    public void parsesMalformedTemperature() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                "99999V0203201N00261220001CN9999999N9+19991+99999999999");
        Counters counters = new Counters();

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withCounters(counters)
                .withOutput(new Text("1950"), new IntWritable(1999))
                .runTest();

        Counter c = counters.findCounter(MaxTemperatureMapper.Temperature.OVER_100);
        assertThat(c.getValue(), is(1L));
    }
}
