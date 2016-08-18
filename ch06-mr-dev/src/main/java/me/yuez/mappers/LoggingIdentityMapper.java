package me.yuez.mappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LoggingIdentityMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Map key: " + key);

        LOG.info("Map key: " + key);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Map value: " + value);
        }

        context.write(key, value);
    }
}
