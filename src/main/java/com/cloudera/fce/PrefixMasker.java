package com.cloudera.fce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

@Description(name = "prefixmask",
  value = "_FUNC_(value) - Returns a masked value",
  extended = "Example:\n"
    + " > SELECT _FUNC_(col) FROM src LIMIT 1;\n")

public class PrefixMasker extends GenericUDF {

  private static final String USER_NAME_SENTRY_PROPERTY = "hive.sentry.subject.name";
  private static final String USER_NAME_ACCESS_PROPERTY = "hive.access.subject.name";
  private static final String MASK_STRING = "XXXXXXXXXXXX";
  private static final Logger LOG = LoggerFactory.getLogger(PrefixMasker.class);

  private StringObjectInspector argOI;
  private HashMap<String, Boolean> privileged = new HashMap<String, Boolean>();
  private boolean userIsPrivileged = false;

  private boolean initialised = false;
  private JobConf conf = null;

  private void getContext() {
    try {
      // THIS IS HORRIBLE, but the job config is passed to the ExecDriver on the command line...
      // Note sun.java.command is not portable
      String cmdLine = System.getProperty("sun.java.command");
      String[] tokens = cmdLine.split("\\s+");
      for (String token : tokens) {
        if (token.trim().endsWith("jobconf.xml")) {
          String file = token.trim();
          System.out.println("Reading config from: " + file);
          conf = new JobConf(new Path(file));
        }
      }
      setPrivileged();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  private void setPrivileged() {
    // Who is running this query?
    LOG.info("Sentry subject [{}]", conf.get(USER_NAME_SENTRY_PROPERTY));
    LOG.info("Access subject [{}]", conf.get(USER_NAME_ACCESS_PROPERTY));
    String currentUser = conf.get(USER_NAME_SENTRY_PROPERTY);
    if (null == currentUser) {
      currentUser = conf.get(USER_NAME_ACCESS_PROPERTY);
    }
    if (null == currentUser) {
      currentUser = conf.getUser();
    }
    LOG.info("User running UDF is [{}]", currentUser);

    // Load the privileged map - from a service, or HDFS file or group lookup, OBVIOUSLY not statically as here
    privileged.put("vagrant", true);

    // Is the current user privileged?
    if (privileged.containsKey(currentUser) && privileged.get(currentUser)) {
      userIsPrivileged = true;
    }
    LOG.info("User [{}] is privileged: {}", currentUser, userIsPrivileged);
  }

  @Override
  public void configure(MapredContext context) {
    super.configure(context);
    conf = context.getJobConf();
    setPrivileged();
    initialised = true;
  }

  @Override
  public ObjectInspector initialize(
    ObjectInspector[] objectInspectors) throws UDFArgumentException {
    if (objectInspectors.length != 1) {
      throw new UDFArgumentLengthException("The operator '" + this.getClass().getSimpleName() + "' requires a single argument");
    }
    if (!(objectInspectors[0] instanceof StringObjectInspector)) {
      throw new UDFArgumentTypeException(0, "The operator '" + this.getClass().getSimpleName() + "' expects string columns");
    }
    argOI = (StringObjectInspector) objectInspectors[0];
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    String arg = argOI.getPrimitiveJavaObject(deferredObjects[0].get());

    if (null == arg) return null;
    else return mask(arg);
  }

  @Override
  public String getDisplayString(String[] strings) {
    return null;
  }

  private String mask(String arg) {
    if (!initialised) {
      getContext();
      initialised = true;
      System.out.println("Called by privileged user: " + userIsPrivileged);
    }

    if (userIsPrivileged) {
      return arg;
    } else {
      int len = arg.length();
      if (len > 12) {
        String rest = arg.substring(12);
        return MASK_STRING + rest;
      } else {
        return MASK_STRING.substring(0, len);
      }
    }
  }
}
