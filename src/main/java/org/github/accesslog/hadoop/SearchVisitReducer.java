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
    text_value = new Text("");
    visits = new TreeMap<String,MyMapWritable>();
    super.setup(context);
  }

  @Override
  public void reduce(Text reduceKey, Iterable<MyMapWritable> reduceValue, Context context) throws IOException, InterruptedException
  {
    int i = 0;
    visits.clear();
    Iterator<MyMapWritable> it = reduceValue.iterator();
    while (it.hasNext()) { //Sorting data by timestamp
      MyMapWritable v = new MyMapWritable( it.next() );
      visits.put(v.get(LogParser.TS).toString(), v);
    }

    Map<Integer,MyMapWritable> googleVisits = new TreeMap<Integer,MyMapWritable>();
    for (String ts : visits.keySet()) {
      MyMapWritable v = visits.get(ts);
      if(v.containsKey(LogParser.GOOGLE_QUERY)){ //Visit from google
        v.put(LogParser.PAGE_VIEWS, new Text("1"));
        googleVisits.put(i, v);
        i++;
      }
      else if (i >= 1){ //Regular visit (after a visit from google)
        MyMapWritable googleVisit = googleVisits.get(i-1);
        int pageViews = Integer.parseInt( googleVisit.get(LogParser.PAGE_VIEWS).toString() );
        pageViews ++;
        googleVisit.put(LogParser.PAGE_VIEWS, new Text( String.valueOf( pageViews ) ));
        googleVisits.put(i-1, googleVisit);
      }
    }

    if(googleVisits.size()>0){
      for (Integer index : googleVisits.keySet()) {
        text_key.clear();
        text_key.set(googleVisits.get(index).toString());
        context.write(text_key, text_value);
      }
    }
  }
}