/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.github.accesslog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.github.accesslog.hadoop.AccessLogStats;
import org.github.accesslog.utils.CommandLineHelper;

/**
 *
 * @author raimonbosch
 */
public class App {

  public static void usage()
  {
    System.out.println("\t--input\n\t\tLocation of your accesslog file or directory");
    System.out.println("\t--output\n\t\tDirectory where you want to generate the output");
    System.out.println("\t--debug\n\t\tTo see DEBUG LEVEL i.e --debug");
    System.out.println("\t--help\n\t\tTo see HELP USAGE i.e --help");
    System.out.println("\tExample:\n\t\thadoop -jar accesslog.jar --input=/opt/apache2/logs --output=/tmp/analyzed_logs");
  }

  public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException
  {
    CommandLineHelper helper = new CommandLineHelper(args);
    helper.setRequiredOption("input", "input", true, "input");
    helper.setRequiredOption("output", "output", true, "output");
    helper.setOption("help", false, "help");
    helper.setOption("debug", false, "debug");
    String input = null, output = null;

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("help", false);
    params.put("debug", false);

    try {

      helper.parsePosix();

      params.put("input", helper.getOptionValue("input"));
      params.put("output", helper.getOptionValue("output"));

      if (helper.hasOption("help")) {
        App.usage();
        System.exit(-1);
      }

      if (helper.hasOption("debug")) {
        params.put("debug", true);
      }

    } catch (Exception e) {
      e.printStackTrace();
      App.usage();
      System.exit(-1);
    }

    AccessLogStats.index(params);
  }
}
