/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.github.accesslog.hadoop;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.Text;
import org.github.accesslog.utils.LogParser;
import org.github.accesslog.utils.MyMapWritable;
import org.github.accesslog.utils.Pcre;

/**
 *
 * @author raimon
 */
public class SearchVisitMapper extends Mapper<LongWritable, Text, Text, MyMapWritable>
{
  private MyMapWritable value;
  private LogParser lru;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
    value = new MyMapWritable();
    lru = new LogParser();
    super.setup(context);
  }

  @Override
  protected void map(LongWritable mapkey, Text mapvalue, Context context) throws IOException, InterruptedException
  {
    int i = 0;
    value.clear();

    lru.readLogLine(mapvalue.toString(), value);

    if(value.containsKey(LogParser.ACCESS_URL) &&
       value.containsKey(LogParser.IP_ADDRESS) &&
       !value.containsKey(LogParser.IS_BOT) &&
       isValidAccessUrl(value.get(LogParser.ACCESS_URL).toString(), value)){
      //System.out.println(mapvalue.toString());
      context.write((Text)value.get(LogParser.IP_ADDRESS), value);
    }
  }

  private static Pattern INVALID_URL = Pattern.compile("(gif|jpg|js|css|ico|txt|png) HTTP");

  public boolean isValidAccessUrl(String s, MyMapWritable m){
    return Pcre.preg_match(INVALID_URL, s) == 0;
  }
}