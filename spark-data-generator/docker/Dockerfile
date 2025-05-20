FROM apache/spark:3.4.1-scala2.12-java11-ubuntu

USER root

RUN mkdir -p /opt/contistuff && \
    mkdir -p /opt/contistuff/keyfile && \
    mkdir -p /opt/spark/conf && \
    chown -R spark:spark /opt/contistuff && \
    chmod -R 777 /opt/contistuff

# COPY foo bar/foo copies foo into bar
# COPY foo bar/ copies contents of foo into bar
# https://stackoverflow.com/questions/26504846/copy-directory-to-another-directory-using-add-command
COPY docker/picklejars /opt/contistuff/picklejars
COPY docker/tpcds-bin /opt/contistuff/tpcds-bin
COPY docker/spark-conf /opt/spark/conf/
COPY target/scala-2.12/parquet-data-generator_2.12-1.0.jar /opt/contistuff/
COPY docker/entrypoint.sh /opt/contistuff/entrypoint.sh

# Only use outside google cloud if accessing google cloud storage
# ENTRYPOINT [ "/opt/contistuff/entrypoint.sh" ]
ENTRYPOINT [ "/opt/entrypoint.sh" ]

USER 185