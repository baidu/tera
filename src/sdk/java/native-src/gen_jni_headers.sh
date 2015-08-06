#/bin/bash

classpath="../tera/target/classes"

javah -classpath $classpath -o jni_tera_base.h      com.baidu.tera.client.TeraBase
javah -classpath $classpath -o jni_tera_client.h    com.baidu.tera.client.TeraClientImpl
javah -classpath $classpath -o jni_tera_table.h     com.baidu.tera.client.TeraTableImpl
javah -classpath $classpath -o jni_tera_mutation.h  com.baidu.tera.client.TeraMutationImpl
javah -classpath $classpath -o jni_tera_reader.h    com.baidu.tera.client.TeraReaderImpl
javah -classpath $classpath -o jni_tera_result.h    com.baidu.tera.client.TeraResultImpl
javah -classpath $classpath -o jni_tera_scan.h      com.baidu.tera.client.TeraScanImpl
javah -classpath $classpath -o jni_tera_result_stream.h com.baidu.tera.client.ScanResultStreamImpl
