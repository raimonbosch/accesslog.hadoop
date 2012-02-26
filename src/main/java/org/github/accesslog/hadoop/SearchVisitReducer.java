/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.github.accesslog.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.github.accesslog.utils.LogParser;
import org.github.accesslog.utils.MyMapWritable;

/**
 *
 * @author raimon
 */
public class SearchVisitReducer extends Reducer<Text, MyMapWritable, Text, Text>
{

  private Text text_key;
  private Text text_value;
  private Map<String,MyMapWritable> visits;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
    text_key = new Text();
    text_value = new Text();
    visits = new TreeMap<String,MyMapWritable>();
    super.setup(context);
  }

  @Override
  public void reduce(Text reduceKey, Iterable<MyMapWritable> reduceValue, Context context) throws IOException, InterruptedException
  {
    visits.clear();
    boolean google_visit = false;
    Iterator<MyMapWritable> it = reduceValue.iterator();

    while (it.hasNext()) {
      MyMapWritable v = new MyMapWritable( it.next() );
      visits.put(v.get(LogParser.TS).toString(), v);
      if(v.containsKey(LogParser.GOOGLE_QUERY)){ google_visit = true; }
    }
    
    if(google_visit){
      int pageViews = 0;
      for(String ts : visits.keySet()){
        MyMapWritable v = visits.get(ts);
        if(v.containsKey(LogParser.GOOGLE_QUERY)){
          if(pageViews > 0){
            text_key.clear();
            text_value.clear();
            v.put(LogParser.PAGE_VIEWS, new Text(String.valueOf( pageViews )));
            text_key.set(v.toString());
            text_value.set("");
            context.write(text_key, text_value);
            pageViews = 0;
          }
        }
        pageViews++;
      }
    }
  }
}