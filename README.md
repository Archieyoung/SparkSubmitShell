SparkSubmitShell
----------------

spark-submit-shell is a tool for submit shell job to yarn/standalone cluster or local computer using spark.


## Build

```sh
git clone git@github.com:Archieyoung/SparkSubmitShell.git
cd SparkSubmitShell
mvn package
```

## Usage

```sh

spark-submit --master yarn \
    --driver-memory 1g \
    --num-executors 1 \
    --executor-memory 1g \
    --executor-cores 1 \
    --class org.bgi.flexlab.SparkSubmitShell \
    spark-submit-shell-<version>.jar \
    job.sh [job_args...] \
    -o job.o.log -e job.e.log
```

## License

Licensed under the MIT License. See the [LICENSE](https://github.com/Archieyoung/SparkSubmitShell/blob/master/LICENSE)