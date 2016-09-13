package com.cloudera.fce.simple;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.UUID;

@UDFType(deterministic = false)
public class UUIDUdf extends UDF {

  public String evaluate() {
    return UUID.randomUUID().toString();
  }

}
