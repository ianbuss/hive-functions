package com.cloudera.fce.simple;

import org.apache.hadoop.hive.ql.exec.UDF;

public class SimpleUdf extends UDF {

  public String evaluate(String field) {
    return "[[" + field.toUpperCase() + "]]";
  }

}

