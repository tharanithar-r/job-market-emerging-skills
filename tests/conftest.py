import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# The launched JVM must be able to write its temp files; the default /tmp is not writable
# in restricted shells, which makes the gateway exit before printing a port.
TMP = Path(__file__).resolve().parent.parent / "tmp" / "spark-tests"
TMP.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.io.tmpdir={TMP}")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
# workers must use the same interpreter as the driver, or PySpark refuses to run
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """One local session for the whole test run."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("jobmarket-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.local.dir", str(TMP))
        .getOrCreate()
    )
    yield session
    session.stop()
