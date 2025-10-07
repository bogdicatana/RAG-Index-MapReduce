FROM openjdk:21-slim
WORKDIR /app
COPY target/scala-3.3.6/cs441hw1-assembly-0.1.0-SNAPSHOT.jar .
ENV HADOOP_HOME=/opt/hadoop
RUN mkdir -p /opt/hadoop
CMD ["java", "-cp", "cs441hw1-assembly-0.1.0-SNAPSHOT.jar", "Main"]
