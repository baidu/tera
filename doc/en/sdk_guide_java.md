# Tera Java SDK Guide

## Implementation

  * Java-SDK invokes [native-SDK](sdk_guide.md) through JNI library (libjni_tera.so).
  * Java-SDK uses [Maven](https://maven.apache.org/) to automate the build of projects.

## Have a quick try

  1. Build Tera. See [BUILD](../../build.md).
  1. Deploy Tera. See [pseudo-distributed mode](../onebox.md).
  1. Add the path of *libjni_tera.so* to environment variable **LD_LIBRARY_PATH**.
  1. Set environment variable **TERA_CONF_PATH** to the path of *tera.flag*.
  1. Under the directory src/sdk/java, execute command **mvn package** to run the unittest and build JAR files.
  1. Install the output JAR files into the Maven repository to use Tera Java SDK in the future.
