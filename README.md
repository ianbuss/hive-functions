# Example Hive UDFs

This repo contains some simple UDFs to serve as exemplars. Real use-cases will require more complex
and robust functionality. UDFs in the `com.cloudera.fce.simple` package use the simplified `UDF`
class in which only the `evaluate` method needs to be overridden. More complex UDFs requiring
access to the runtime context and the ability to handle multiple types will need to use the
`GenericUDF` class.

# Simple

Two examples are included in this package. 

1. `UUIDUdf` simply generates and returns a random UUID as a
string when it is invoked. The UDF is non-deterministic.
2. `SimpleUdf` wraps the passed argument in double square brackets and
converts to uppercase.

# Generic

An example generic UDF, `PrefixMasker`, uses the supplied MapredContext to make decisions about
whether a query user can view the full contents of the column or merely a masked version. Obviously
a real implementation would have to be fully configurable and have beefed up security.

# Building

```
mvn clean package
```

# Installing

Place in the Hive Aux Jars path on the HiveServer2 node, restart HS2 and create a corresponding
function in Beeline as the following example:

```
-- create function as a member of a Sentry role with ALL on SERVER
create function uuid as 'com.cloudera.fce.simple.UUIDUdf'

-- use as anyone with SELECT on table foo
select uuid() as uniqueid from foo;
```

