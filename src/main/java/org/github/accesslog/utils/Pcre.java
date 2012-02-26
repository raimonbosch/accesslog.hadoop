package org.github.accesslog.utils;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Raimon Bosch
 */
public class Pcre {

  public static String siu = "siu";
  public static String u = "u";
  public static String iu = "iu";
  public static String ue = "ue";
  public static String spc = "~";
  public static String empty = "";
  public static String[] s_alwsp = {
    "/",
    "@",
    "!"
  };
  public static String[] SLASH_CHARACTERS = {
    "<", //0
    ">", //1
    ".", //2
    ",", //3
    "\"", //4
    "/", //5
    "'", //6
    "(", //7
    ")", //8
    "=", //9
    "&", //10
    ";", //11
    ":", //12
    "-", //13
    "!", //14
    "_", //15
    "+", //16
    "#", //17
    "[", //18
    "]", //19
    "*", //20
    "?", //21
    "%", //22
    "(", //23
    ")" //24
  };
  public static String[] SLASH_CHARACTERS_SUBSTITUTION = {
    "\\<", //0
    "\\>", //1
    "\\.", //2
    "\\,", //3
    "\\\"", //4
    "\\/", //5
    "\\'", //6
    "\\(", //7
    "\\)", //8
    "\\=", //9
    "\\&", //10
    "\\;", //11
    "\\:", //12
    "\\-", //13
    "\\!", //14
    "\\_", //15
    "\\+", //16
    "\\#", //17
    "\\[", //18
    "\\]", //19
    "\\*", //20
    "\\?", //21
    "\\%", //22
    "\\(", //23
    "\\)" //24
  };

  /** Creates a new instance of RegexFinder */
  public Pcre() {
  }

  public static int preg_match(String pattern, String subject) {
    Pattern p = Pcre.compile(pattern);
    return preg_match(p, subject);
  }

  public static int preg_match(Pattern p, String subject) {
    Matcher m = p.matcher(subject); // get a matcher object
    int count = 0;
    //System.out.println("Trying to find matches using pattern:'"+pattern+"' on subject:'"+subject+"'");
    while (m.find()) {
      count++;
      return count;
    }

    return 0;
  }

  public static String preg_match(String pattern, String subject, boolean return_matches) {
    Pattern p = Pcre.compile(pattern);
    return  preg_match(p, subject, return_matches);
  }

  public static String preg_match(Pattern p, String subject, boolean return_matches) {
    Matcher m = p.matcher(subject); // get a matcher object
    int count = 0;
    while (m.find()) {
      count++;
      return (return_matches) ? m.group() : String.valueOf(count);
    }

    return (return_matches) ? "0" : "";
  }

  public static String[] preg_match_all(String pattern, String subject) {
    Pattern p = Pcre.compile(pattern);
    return preg_match_all(p, subject);
  }

  public static String[] preg_match_all(Pattern p, String subject) {
    Matcher m = p.matcher(subject); // get a matcher object
    StringBuilder out = new StringBuilder();

    boolean split = false;
    while (m.find()) {
      out.append(m.group());
      out.append(spc);
      split = true;
    }

    return (split) ? out.toString().split(spc) : new String[0];
  }

  public static String[] preg_match_all(String pattern, String subject, int groups) {
    Pattern p = Pcre.compile(Pcre.getPatternWithoutFlags(pattern));
    return preg_match_all(p, subject, groups);
  }

  public static String[] preg_match_all(Pattern p, String subject, int groups) {
    Matcher m = p.matcher(subject); // get a matcher object
    StringBuilder out = new StringBuilder();

    boolean split = false;
    while (m.find()) {
      out.append(m.group(groups));
      out.append(spc);
      split = true;
    }

    return (split) ? out.toString().split(spc) : new String[0];
  }

  public static Object[] array_unique(Object array[]) {
    SortedSet<Object> set = new TreeSet<Object>();
    set.addAll(Arrays.asList(array));
    return set.toArray();
  }

  public static String preg_replace(String pattern, String replacement, String subject) {
    Pattern p = Pcre.compile(pattern);
    return preg_replace(p, replacement, subject);
  }

  public static String preg_replace(Pattern p, String replacement, String subject) {
    return p.matcher(subject).replaceAll(replacement);
  }

  public static String str_replace(String[] oldChars, String[] newChars, String subject) {
    for (int i = 0; i < oldChars.length; i++) {
      if(subject.indexOf(oldChars[i]) != -1){
        if (newChars[i] != null && !newChars[i].equals(empty)) {
          subject = subject.replace(oldChars[i], newChars[i]);
        } else {
          subject = subject.replace(oldChars[i], empty);
        }
      }
    }

    return subject;
  }

  public static String str_replace(String[] oldChars, String newChar, String subject) {
    for (int i = 0; i < oldChars.length; i++) {
      subject = subject.replace(oldChars[i], newChar);
    }

    return subject;
  }

  public static String str_replace(String oldChar, String newChar, String subject) {
    subject = subject.replace(oldChar, newChar);
    return subject;
  }

  public static String preg_replace(String[] patterns, String[] replacements, String subject) {
    for (int i = 0; i < patterns.length; i++) {
      subject = Pcre.preg_replace(patterns[i], (replacements[i] != null) ? replacements[i] : "", subject);
    }

    return subject;
  }

  public static String preg_replace(String[] patterns, String[] replacements, String subject, String[] indexof) {
    //Simplified preg_replace multiple using an indexOf array. If indexOf[i] exists in subject we will perform the regular expression at i
    for (int i = 0; i < patterns.length; i++) {
      if (subject.indexOf(indexof[i]) != -1) {
        subject = Pcre.preg_replace(patterns[i], replacements[i], subject);
      }
    }

    return subject;
  }

  public static String preg_replace(Pattern[] patterns, String[] replacements, String subject, String[] indexof) {
    //Simplified preg_replace multiple using an indexOf array. If indexOf[i] exists in subject we will perform the regular expression at i
    for (int i = 0; i < patterns.length; i++) {
      if (subject.indexOf(indexof[i]) != -1) {
        subject = Pcre.preg_replace(patterns[i], replacements[i], subject);
      }
    }

    return subject;
  }

  public static String preg_replace(String[] patterns, String replacement, String subject) {
    for (int i = 0; i < patterns.length; i++) {
      subject = Pcre.preg_replace(patterns[i], replacement, subject);
    }

    return subject;
  }

  public static String preg_replace(Pattern[] patterns, String replacement, String subject) {
    for (int i = 0; i < patterns.length; i++) {
      subject = Pcre.preg_replace(patterns[i], replacement, subject);
    }

    return subject;
  }

  public static String[] preg_split(String pattern, String subject) {
    return subject.split(pattern);
  }

  /*public static String findMatch(String pattern, String regex, String text) {
  Pattern p = Pcre.compile(regex);
  Matcher m = p.matcher(text); // get a matcher object

  if(m.find()) {
  //return m.group();
  Pattern p2 = Pcre.compile(pattern);
  Matcher m2 = p2.matcher(m.group());
  if(m2.find())
  return m2.group();
  else return "";
  }
  else{
  return "";
  }
  }*/

  public static String preg_quote(String str) {
    return Pcre.str_replace(Pcre.SLASH_CHARACTERS, Pcre.SLASH_CHARACTERS_SUBSTITUTION, str);
  }

  public static String[] array_merge(String[] array1, String[] array2) {
    int i = 0;
    if (array1 != null && array1.length > 0 && array2 != null && array2.length > 0) {
      String array3[] = new String[array1.length + array2.length];
      for (String element : array1) {
        array3[i] = element;
        i++;
      }
      for (String element : array2) {
        array3[i] = element;
        i++;
      }

      return array3;
    } else if (array1 != null && array1.length > 0) {
      return array1;
    } else if (array2 != null && array2.length > 0) {
      return array2;
    } else {
      return null;
    }
  }

  public static Pattern compile(String pattern) {
    //System.out.println("compile(pattern:" + pattern + ") => woFlags:'" + Pcre.getPatternWithoutFlags(pattern) + "', flags: " + Pcre.getPatternFlags(pattern));
    return Pattern.compile(Pcre.getPatternWithoutFlags(pattern), Pcre.getPatternFlags(pattern));
  }

  public static String getPatternWithoutFlags(String pattern) {

    for (String s_alws : s_alwsp) {
      if (pattern.startsWith(s_alws) && pattern.endsWith(s_alws + siu)) {
        pattern = pattern.substring(1, pattern.length() - 4);
      } else if (pattern.startsWith(s_alws) && pattern.endsWith(s_alws + u)) {
        pattern = pattern.substring(1, pattern.length() - 2);
      } else if (pattern.startsWith(s_alws) && pattern.endsWith(s_alws + iu)) {
        pattern = pattern.substring(1, pattern.length() - 3);
      } else if (pattern.startsWith(s_alws) && pattern.endsWith(s_alws + ue)) {
        pattern = pattern.substring(1, pattern.length() - 3);
      }
    }

    //System.out.println("Out pattern:" + pattern);
    return pattern;
  }

  public static int getPatternFlags(String pattern) {

    for (String s_alws : s_alwsp) {
      if (pattern.startsWith(s_alws) && pattern.endsWith(s_alws + siu)) {
        return (Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
      }
    }

    return Pattern.CASE_INSENSITIVE;
  }
}
