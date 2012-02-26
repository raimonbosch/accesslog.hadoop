/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.github.accesslog.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.github.accesslog.utils.ShellUtils;

/**
 *
 * @author raimonbosch
 */
public class AccessLogStats {

  public static void index(Map<String, Object> params) throws IOException, InterruptedException, ClassNotFoundException
  {
    Configuration jc = new Configuration();

    String inputPath = (String)params.get("input");
    String outputPath = (String)params.get("output");

    //SEARCHVISIT GENERATOR PHASE
    System.out.println("Cleaning outputPath for SEARCHVISIT process...");
    ShellUtils.exec("rm -rf " + outputPath);

    SearchVisit.parseLine(new Path(inputPath), new Path(outputPath), jc);

    //TODO: BOT VISIT GENERATION PHASE
  }
}
