FROM nvidia/cuda:12.4.1-cudnn-devel-ubuntu22.04
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PYTHONHASHSEED=1
ENV DAEMON_RUN=true
ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"

RUN apt-get update -y
RUN apt-get install -y build-essential \
                python3-pip \
                git \
                wget \
                ca-certificates \
                software-properties-common \
                ssh \
                net-tools \
                rsync \
                jq \
                openjdk-17-jre \
                openjdk-17-jdk-headless \
                unzip \
                libaio-dev \
                python3-dev

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
   && mkdir -p /opt/spark \
   && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
   && rm apache-spark.tgz
RUN mkdir -p $SPARK_HOME/logs
RUN mkdir -p $SPARK_HOME/work
RUN chmod g+w $SPARK_HOME/work

RUN mkdir -p /root/.triton/autotune && chmod 777 /root/.triton/autotune
RUN apt-get install -y cuda-libraries-dev-12-4
RUN apt-get install -y cuda-cudart-dev-12-4
RUN apt-get install -y libcudnn8-dev
RUN apt-get clean
RUN pip3 install --upgrade pip

COPY requirements.txt ./
RUN pip3 install --ignore-installed -r requirements.txt
COPY entrypoint.sh ./