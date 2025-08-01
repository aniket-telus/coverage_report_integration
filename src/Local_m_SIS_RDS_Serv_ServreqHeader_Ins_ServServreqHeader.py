import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from py4j.java_gateway import java_import

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str) -> SparkSession:
    """Create and configure Spark session"""
    try:
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.driver.extraClassPath", "C:/Oracle/product/11.2.0/x86/client_1/jdbc/lib/ojdbc5.jar")
                .getOrCreate())
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise


def get_oracle_connection(spark,db_params: Dict[str, str]):
    """Create Oracle connection using Java DriverManager"""
    gateway = spark.sparkContext._gateway
    java_import(gateway.jvm, "java.sql.*")

    return gateway.jvm.DriverManager.getConnection(
        db_params["jdbcUrl"],
        db_params["user"],
        db_params["password"]
    )


def execute_pre_sql(spark: SparkSession, retention_days: int,db_params: Dict[str, str]) -> None:
    """Execute pre-SQL statement to delete records"""
    try:
        conn = get_oracle_connection(spark,db_params)
        delete_sql = f"""
            DELETE FROM SIS_ODS.SERV_SERVREQ_HEADER 
            WHERE (load_date >= TRUNC(SYSDATE) 
            OR load_date < TRUNC(SYSDATE) - {retention_days})
        """
        # with conn.createStatement() as stmt:
        stmt = conn.createStatement()
        stmt.executeUpdate(delete_sql)
        conn.commit()
        logger.info("Pre-SQL deletion completed successfully")
    except Exception as e:
        logger.error(f"Error executing pre-SQL: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def read_source_data(spark: SparkSession, event_type: str, status: str, days_offset: int,db_params: Dict[str, str]) -> pd.DataFrame:
    """Read data from source table using PySpark"""
    try:
        query = f"""
            SELECT 
                h.SERVREQ_TRANSACTION_TS, h.SERVREQ_HEADER_ID, h.EFFECTIVE_DT,
                h.SERVREQ_PRIORITY_ID, h.LANGUAGE_CD, h.SERVREQ_HEADER_TXT,
                h.SERVREQ_HEADER_NM, h.SERVREQ_HEADER_REF_ID, h.REFERENCE_NO,
                h.EXPIRY_DT, h.LOAD_DT, h.UPDATE_DT, h.SERVREQ_EVENT_TYPE_ID,
                h.SERVREQ_HEADER_STATUS_SEQ_NO, h.SERVREQ_CURRENT_STATUS_ID,
                h.SERVREQ_ORIG_SRC_TXN_TS, h.SERVREQ_REFERENCE_NO_TYPE_ID
            FROM ICS_ODS.TMP_SERVREQ_HEADER h
            WHERE h.servreq_event_type_id IN ({event_type})
            AND EXISTS (
                SELECT NULL
                FROM ICS_ODS.TMP_SERVREQ_HEADER_STATUS hs
                WHERE hs.servreq_status_id IN ({status})
                AND hs.servreq_transaction_ts = h.servreq_transaction_ts
                AND hs.servreq_header_id = h.servreq_header_id
                AND hs.update_dt >= TRUNC(SYSDATE) - {days_offset}
                AND hs.update_dt < TRUNC(SYSDATE)
            )
        """
        logger.info("Reading source data")
        df = spark.read \
            .format("jdbc") \
            .option("url", db_params["jdbcUrl"]) \
            .option("query", query) \
            .option("user", db_params["user"]) \
            .option("password", db_params["password"]) \
            .option("driver", db_params["jdbcDriver"]) \
            .load()

        print(f"Total Number of rows in dataframe under read func are : {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Error reading source data: {str(e)}")
        raise


def transform_data(spark: SparkSession, df: Any) -> Any:
    """Transform data using PySpark"""
    try:
        spark_df = df
        result_df = spark_df.withColumn("LOAD_DATE", current_timestamp()) \
            .withColumn("UPDATE_DATE", current_timestamp())
        logger.info("Data transformation completed")
        print(f"Total Number of rows in dataframe under transform data func : {result_df.count()}")
        return result_df
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise


def write_target_data(spark: SparkSession, df: Any,db_params: Dict[str, str]) -> None:
    """Write data to target table"""
    try:
        df.write \
            .format("jdbc") \
            .option("url", db_params["jdbcUrl"]) \
            .option("dbtable", "SERV_SERVREQ_HEADER") \
            .option("user", db_params["user"]) \
            .option("password", db_params["password"]) \
            .mode("append") \
            .save()
        logger.info("Data written to target successfully")
    except Exception as e:
        logger.error(f"Error writing to target: {str(e)}")
        raise


def main(args: argparse.Namespace) -> None:
    """Main execution function"""
    spark = None
    # Database connection details
    tgt_conn_param = {
        "user": "******",
        "password": "******",
        "server" : "******",
        "port" : 41521,
        "service_name": "******",
        "db_type": "oracle"
    }
    if tgt_conn_param["db_type"] == "oracle":
        tgt_conn_param["jdbcDriver"] = "oracle.jdbc.driver.OracleDriver"
        tgt_conn_param["jdbcUrl"] = f"jdbc:oracle:thin:@//{tgt_conn_param['server']}:{tgt_conn_param['port']}/{tgt_conn_param['service_name']}"

    src_conn_param = {
        "user": "******",
        "password": "******",
        "server": "******",
        "port": 41521,
        "service_name": "******",
        "db_type": "oracle"
    }
    if src_conn_param["db_type"] == "oracle":
        src_conn_param["jdbcDriver"] = "oracle.jdbc.driver.OracleDriver"
        src_conn_param["jdbcUrl"] = f"jdbc:oracle:thin:@//{src_conn_param['server']}:{src_conn_param['port']}/{src_conn_param['service_name']}"

    try:
        spark = create_spark_session(f"{args.mapping_name}_job")

        execute_pre_sql(spark, args.retention_days,tgt_conn_param)

        source_df = read_source_data(
            spark,
            args.event_type,
            args.status,
            args.days_offset,
            src_conn_param
        )

        transformed_df = transform_data(spark, source_df)

        write_target_data(spark, transformed_df,tgt_conn_param)

        logger.info("Job completed successfully")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Service Request Header ETL Job")
    parser.add_argument("--mapping_name", required=True, help="Name of the mapping")
    parser.add_argument("--event_type", required=True, help="Event type IDs (comma-separated)")
    parser.add_argument("--status", required=True, help="Status IDs (comma-separated)")
    parser.add_argument("--days_offset", type=int, required=True, help="Days offset for data filter")
    parser.add_argument("--retention_days", type=int, required=True, help="Retention days for data cleanup")

    args = parser.parse_args()

    main(args)