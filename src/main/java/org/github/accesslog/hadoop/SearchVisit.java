/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.github.accesslog.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.github.accesslog.utils.MyMapWritable;

/**
 *
 * @author raimonbosch
 *
 */
public class SearchVisit {

  public static Job parseLine(Path input, Path output, Configuration jc) throws IOException, InterruptedException, ClassNotFoundException
  {
    Job j = new Job(jc);

    j.setJarByClass(SearchVisit.class);
    j.setJobName("AccessLogConsumer");
    j.setMapperClass(SearchVisitMapper.class);
    j.setReducerClass(SearchVisitReducer.class);

    j.setInputFormatClass(TextInputFormat.class);
    j.setOutputFormatClass(TextOutputFormat.class);

    j.setMapOutputKeyClass(Text.class);
    j.setMapOutputValueClass(MyMapWritable.class);

    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(j, input);
    FileOutputFormat.setOutputPath(j, output);

    j.setNumReduceTasks(2);

    j.submit();

    j.waitForCompletion(true);

    return j;
  }
}
