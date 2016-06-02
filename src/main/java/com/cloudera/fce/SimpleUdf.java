package com.cloudera.fce;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by ianbuss on 02/03/2016.
 */
public class SimpleUdf extends UDF {

  public String evaluate(String field) {
    return "[[" + field.toUpperCase() + "]]";
  }

}
