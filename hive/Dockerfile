FROM apache/hive:3.1.3

USER root

# S3A 라이브러리
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.4/hadoop-aws-3.2.4.jar /opt/hive/lib/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/hive/lib/

# PostgreSQL JDBC
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/hive/lib/

# Hadoop 설정
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

# 커스텀 entrypoint
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV HADOOP_CLASSPATH=/opt/hive/lib/*

# 권한 설정
RUN chown -R hive:hive /opt/hive/lib/*.jar && \
    chmod 644 /opt/hive/lib/*.jar

USER hive

# 커스텀 entrypoint 사용
ENTRYPOINT ["/custom-entrypoint.sh"]