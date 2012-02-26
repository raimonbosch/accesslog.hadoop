/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.github.accesslog.utils;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;

/**
 *
 * @author raimonbosch
 */
public class LogParser
{
  private static Pattern PATTERN_IP_ADDRESS = Pattern.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");
  private static Pattern PATTERN_DATE = Pattern.compile("\\[[0-9]{2}\\/[A-Za-z]{3}\\/[0-9]{4}\\:[0-9]{2}\\:[0-9]{2}\\:[0-9]{2} \\+[0-9]{4}\\]");
  private static Pattern PATTERN_USER_AGENT = Pattern.compile("(Mozilla|Opera)\\/[0-9]{1}\\.[0-9]{1,2}");
  private static Pattern PATTERN_HTTP_STATUS = Pattern.compile("[0-9]{3}( [0-9]+)?");

  private SimpleDateFormat logDateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss +0000]", Locale.US);
  private SimpleDateFormat YYYYMMdd_HHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
  private SimpleDateFormat YYYYMMdd = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
  private Date classDate = new Date();

  public static Text IP_ADDRESS = new Text("ipAddress");
  public static Text DATE = new Text("date");
  public static Text ACCESS_URL = new Text("accessUrl");
  public static Text HTTP_STATUS = new Text("httpStatus");
  public static Text URL_REFERER = new Text("urlReferer");
  public static Text USER_AGENT = new Text("userAgent");
  public static Text TS = new Text("ts");
  public static Text IS_BOT = new Text("isBot");
  public static Text TRUE = new Text("1");
  public static Text FALSE = new Text("0");
  public static Text GOOGLE_QUERY = new Text("googleQuery");
  public static Text GOOGLE_RANK = new Text("googleRank");
  public static Text GOOGLE_RANKED_URL = new Text("googleRankedUrl");
  public static Text PAGE_VIEWS = new Text("pageViews");

  public void readLogLine(String mapvalue, MyMapWritable value)
  {
    String values[] = mapvalue.toString().split("[\\s\"\\-]{2,3}");
    for (String v : values) {
      if (v.equals("")) {
        continue;
      }

      v = v.replace("\"", "");
      Text v_text = new Text(v);

      if (value.get(IP_ADDRESS) == null && isIP(v)) {
        value.put(IP_ADDRESS, v_text);
      } else if (value.get(DATE) == null && isDate(v)) {
        String date = parseDate(v);
        value.put(DATE, new Text(date));
        value.put(TS, new Text(String.valueOf(getTimestamp(date))));
      } else if (value.get(ACCESS_URL) == null && isAccessUrl(v)) {
        value.put(ACCESS_URL, v_text);
      } else if (value.get(URL_REFERER) == null && isUrlReferer(v)) {
        value.put(URL_REFERER, v_text);
        getGoogleQueryFromReferer(v, value);
      } else if (value.get(USER_AGENT) == null && isUserAgent(v)) {
        value.put(USER_AGENT, v_text);
        if (isBot(v)) {
          value.put(IS_BOT, TRUE);
        }
      } else if (value.get(HTTP_STATUS) == null && isHttpStatus(v)) {
        value.put(HTTP_STATUS, v_text);
      }
    }
  }
  
  public void getGoogleQueryFromReferer(String s, MyMapWritable m)
  {
    if (!s.contains("www.google")) {
      return;
    }

    String url_params[] = s.toString().split("\\?");
    if (url_params.length < 2) { return; }
    String aParams[] = url_params[1].toString().split("&");

    for (String param : aParams) {
      String k_v[] = param.split("=");
      if (k_v.length < 2) { continue; }

      if (k_v[0].equals("q")) {
        m.put(GOOGLE_QUERY,
                new Text(URLDecoder.decode(k_v[1])));
      }
      if (k_v[0].equals("cd")) {
        m.put(GOOGLE_RANK, new Text(URLDecoder.decode(k_v[1])));
      }
      if (k_v[0].equals("url")) {
        m.put(GOOGLE_RANKED_URL,
                new Text(URLDecoder.decode(k_v[1])));
      }
    }
  }

  public boolean isIP(String s)
  {
    return Pcre.preg_match(PATTERN_IP_ADDRESS, s) == 1;
  }

  public boolean isDate(String s)
  {
    return Pcre.preg_match(PATTERN_DATE, s) == 1;
  }

  public boolean isUserAgent(String s)
  {
    return Pcre.preg_match(PATTERN_USER_AGENT, s) == 1;
  }

  public boolean isAccessUrl(String s)
  {
    return s.startsWith("GET");
  }

  public boolean isBot(String s)
  {
    s = s.toLowerCase();
    return (s.contains("bot") || s.contains("crawler") || s.contains("slurp") || s.contains("searchtech"));
  }

  public boolean isHttpStatus(String s)
  {
    return Pcre.preg_match(PATTERN_HTTP_STATUS, s) == 1;
  }

  public boolean isUrlReferer(String s)
  {
    return s.startsWith("http://") || s.startsWith("https://");
  }

  public String parseDate(String date)
  {
    try {
      return YYYYMMdd_HHmmss.format(logDateFormat.parse(date));
    } catch (Exception e) {
      System.out.println("Cannot parse date:" + date);
      e.printStackTrace();
    }

    return null;
  }

  public long getTimestamp(String date)
  {
    try {
      Date d = YYYYMMdd_HHmmss.parse(date);
      return d.getTime();
    } catch (Exception e) {
      System.out.println("<LogParser/parseTimestamp_YYYYMMdd_HHmmss()> Cannot get timestamp from date=" + date);
      e.printStackTrace();
    }

    return 0;
  }

  public String parseTimestamp_YYYYMMdd_HHmmss(String ts)
  {
    try {
      long timestamp = Long.parseLong(ts);
      classDate.setTime(timestamp);
      return YYYYMMdd_HHmmss.format(classDate);
    } catch (Exception e) {
      System.out.println("<LogParser/parseTimestamp_YYYYMMdd_HHmmss()> Cannot parse ts=" + ts);
      e.printStackTrace();
    }

    return null;
  }

  public String parseTimestamp_YYYYMMdd(String ts)
  {
    try {
      long timestamp = Long.parseLong(ts);
      classDate.setTime(timestamp);
      return YYYYMMdd.format(classDate);
    } catch (Exception e) {
      System.out.println("<LogParser/parseTimestamp_YYYYMMdd()> Cannot parse ts=" + ts);
      e.printStackTrace();
    }

    return null;
  }
}
