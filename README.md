# GDS JDBC Driver

The GDS JDBC Driver allows a connection towards a GDS instance for Java programs without having to bother with database (GDS) specific codes. The communication uses the [GDS Java SDK](http://github.com/arh-eu/gds-java-sdk) behind the scenes.

## System requirements

The driver is compatible with the GDS 5 system, and needs Java 8 or newer to work.

## Installation

Probably you do not need to manually compile the driver. You simply download the precompiled binary (jar) from the Releases, or use it as a Maven Dependency, through the JitPack package manager.

```xml
<project>
    <repositories>
        <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
        </repository>
    </repositories>
    
    <dependencies>
        <dependency>
            <groupId>com.github.arh-eu</groupId>
            <artifactId>gds-jdbc</artifactId>
            <version>1.0</version>
        </dependency>
    </dependencies>
</project>
```

## Usage

This driver implements the JDBC standard, therefore it can be used as any other driver from this kind.

If you need help on how to use JDBC drivers, check out the [official Java™ tutorials](https://docs.oracle.com/javase/tutorial/jdbc/).

The commonly used Java interfaces and their implementations are the following:

| Interface | Implemented in |
| --- | --- |
| `java.sql.Driver` | `hu.gds.jdbc.GdsJdbcDriver` |
| `java.sql.Connection` | `hu.gds.jdbc.GdsJdbcConnection` |
| `java.sql.DatabaseMetaData` | `hu.gds.jdbc.GdsDatabaseMetaData` |
| `java.sql.Statement` | `hu.gds.jdbc.GdsBaseStatement` |
| `java.sql.PreparedStatement` | `hu.gds.jdbc.GdsPreparedStatement` |
| `java.sql.ResultSet` | `hu.gds.jdbc.resultset.AbstractGdsResultSet` |
| `java.sql.ResultSetMetaData` | `hu.gds.jdbc.resultset.GdsResultSetMetaData` |

 ### Connection URL
 The general format for a JDBC URL for connecting to a GDS instance is as follows, with items in square brackets (`[ ]`) being optional:
 
 ```
 jdbc:gds:<host>:<port>/gate[?property1=value1[&property2=value2]...]
 ```
 
 A valid example:
 
 ```
"jdbc:gds:192.168.0.106:8888/gate?user=brigittelindholm"
```
 

 ### Connection properties
 
 In addition to the standard connection parameters the driver supports a number of additional properties which can be used to specify additional driver behaviour specific to the GDS. These properties may be specified in either the connection URL or as the additional `Properties` object parameter passed to the `DriverManager.getConnection(..)`.

| Property | Type | Default value | Description |
| --- | --- | --- | --- |
| user | `String` | `""` | The database user on whose behalf the connection is being made. |
| password | `String` | `""` | The password used for password authentication. Leaving it empty means no auth. will be used. |
| sslenabled | `boolean` | `false` | Whether the connection should be made over SSL. |
| verifyServerCertificate | `boolean` | `true` | Configure a connection that uses SSL but does not verify the identity of the server. |
| keyStorePath | `String` | `""` | The path to the user certificate (in PKCS12 format) used to connect via TLS authentication. |
| keyStorePassword | `String` | `""` | The password of the user certificate specified by the `keyStorePath` property. |
| meta.sampling.size | `int` | `1000` | Number of documents that will be fetched per collection in order to return meta information from DatabaseMetaData.getColumns() method. |
| query.scan.consistency | `String` | `"not_bounded"` | Query scan consistency. |
| timeout | `int` | `10000` | Sets the timeout used for the statements in milliseconds. |
| retryLimitOnError | `int` | `3` | Sets the limit for retries if any error happens during the execution of the statement. |
| prefetch | `int` | `3` | Sets the number of prefetch on queries. |
| queryType | `String` | `"PAGE"` | Sets whether to use types of scroll or page. Default value is page type. |
| queryPageSize | `int` | `-1` | Sets the page size of the queries. Default value is -1 to use the GDSs internal settings. |
| serveOnTheSameConnection | `boolean` | true | Sets whether the reply from the GDS should be served on the same connection as the login. |
