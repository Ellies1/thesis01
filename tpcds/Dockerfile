FROM elliesgood00/1213:k8s-trusted-with-example

COPY parquet-data-generator_2.12-1.0.jar /opt/tpcds/
COPY lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar /opt/tpcds/lib/
COPY tools /opt/tpcds-kit/tools
COPY tpcds_2_4 /opt/tpcds/queries



RUN apt update && apt install -y make gcc && \
    cd /opt/tpcds-kit/tools && \
    make OS=LINUX && \
    chmod +x dsdgen && \
    mkdir -p /tpcds-data


