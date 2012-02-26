/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.github.accesslog.utils;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author raimonbosch
 */
public class MyMapWritable extends MapWritable{
  public MyMapWritable(){
    super();
  }

  public MyMapWritable(MyMapWritable m){
    super(m);
  }

  @Override
  public String toString(){
    String s = "{";
    int i = 0;
    for(Writable w : this.keySet()){
      if(i > 0) s += ", ";
      s+= ("\"" + w.toString() + "\":\"" + this.get(w).toString() + "\"" );
      i++;
    }
    s+= "}";
    return s;
  }
}
