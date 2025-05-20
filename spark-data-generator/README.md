# spark-data-generator
The program support 4 modes: test, datagen, metagen, query

### Usage
Assuming you have java 11 and sbt installed. Use sdkman to install them if you don't.

Make sure `conf/spark-defaults.conf` in your spark installation directory are configured properly.

```
~ sbt package
~ spark-submit --class "ParquetGenerator" \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \ \
    --deploy-mode cluster \
    local:///opt/contistuff/parquet-data-generator_2.12-1.0.jar [mode] [storagePath] [dsdgenPath] [queryName]
~ mode validOptions ...
~ test storagePath
~ datagen storagePath dsdgenPath
~ metagen storagePath
~ query queryName
```

For `metagen` and `query`, the appropriate hive metastore should be configured in spark-defaults.conf.

For `datagen` and `metagen`, the appropriate GCS keyfile needs to be mounted using docker or kubernetes.

The project provides a Dockerfile which can be used to build a docker image with the necessary libraries and binaries using `docker build -t sacheendra/contispark:something -f docker/Dockerfile .`.

Upload the created image to a registry, then modify the container image to use in `docker/spark-conf/spark-defaults.conf`.

### If running outside GCP
Add the gcs keyfile as secret named `gcs-keyfile` to kubernetes.

Use command `kubectl create secret generic gcs-keyfile --from-file=gcs_keyfile.json` to create the kubernetes secret.

### Modify spark defaults when submitting the job to change config
Copy the `spark-defaults.conf` into your local spark conf directory, and then run the spark-submit command above.

For convenience, you could also use the docker image itself to run the spark-submit command.

Configuration options for spark performance here: https://spark.apache.org/docs/latest/running-on-kubernetes.html

### Start metastore
Run `docker-compose up` in the docker folder to start metastore.

Add the IP and port of the metastore to the spark-defaults.conf as option `spark.hive.metastore.uris`.
