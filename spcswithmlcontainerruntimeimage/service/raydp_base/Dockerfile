FROM sfsenorthamerica-demo391.registry.snowflakecomputing.com/snowflake/images/snowflake_images/st_plat/runtime/x86/generic_gpu/runtime_image/snowbooks:1.5.0
ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PYTHONHASHSEED=1
ENV DAEMON_RUN=true
ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"
RUN apt-get update -y
RUN apt-get install -y wget \
                net-tools \
                openjdk-17-jre \
                openjdk-17-jdk-headless \
                unzip

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
   && mkdir -p /opt/spark \
   && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
   && rm apache-spark.tgz
RUN mkdir -p $SPARK_HOME/logs
RUN mkdir -p $SPARK_HOME/work
RUN chmod g+w $SPARK_HOME/work
RUN apt-get clean
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY entrypoint.sh ./