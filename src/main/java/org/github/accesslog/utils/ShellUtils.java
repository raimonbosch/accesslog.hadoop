/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.github.accesslog.utils;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author raimonbosch
 */
public class ShellUtils
{
  public static String exec(String cmd)
  {
    String output = "";
    try {
      System.out.println(cmd);
      Process ps = Runtime.getRuntime().exec(cmd);
      output = loadStream(ps.getInputStream());
      System.out.print(output);
      System.err.print(loadStream(ps.getErrorStream()));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    return output;
  }

  static String loadStream(InputStream in) throws IOException
  {
    int ptr = 0;
    in = new BufferedInputStream(in);
    StringBuilder buffer = new StringBuilder();
    while ((ptr = in.read()) != -1) {
      buffer.append((char) ptr);
    }
    return buffer.toString();
  }

  public static void executeTmpScript(String scriptTmpPath, String scriptContent)
  {
    ShellUtils.exec("rm " + scriptTmpPath);
    ShellUtils.exec("touch " + scriptTmpPath);

    try{
      BufferedWriter fw = new BufferedWriter(new FileWriter(scriptTmpPath));
      System.out.println("Command:(" + scriptContent + ") will be executed.");
      fw.write(scriptContent);
      fw.close();
    }
    catch(Exception e){
      System.out.println("Cannot create '" + scriptTmpPath + "'");
      e.printStackTrace();
    }

    ShellUtils.exec("chmod +x " + scriptTmpPath);
    ShellUtils.exec("sh " + scriptTmpPath);
    ShellUtils.exec("rm " + scriptTmpPath);
  }
}
