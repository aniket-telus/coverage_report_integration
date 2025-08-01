import os
import sys
import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader import (
    create_spark_session,
    get_oracle_connection,
    execute_pre_sql,
    read_source_data,
    transform_data,
    write_target_data,
    main
)

@pytest.fixture( scope="module")
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

def test_create_spark_session():
    session = create_spark_session("test_app")
    assert isinstance(session, SparkSession)
    session.stop()


@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.get_oracle_connection")
def test_execute_pre_sql(mock_get_conn, spark):
    mock_conn = MagicMock()
    mock_stmt = MagicMock()
    mock_conn.createStatement.return_value = mock_stmt
    mock_get_conn.return_value = mock_conn

    db_params = {"jdbcUrl": "jdbc:oracle:thin:@//localhost:1521/XE", "user": "test", "password": "test"}
    execute_pre_sql(spark, 30, db_params)

    mock_stmt.executeUpdate.assert_called_once()
    mock_conn.commit.assert_called_once()

@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.read_source_data")
def test_read_source_data(spark):
    # Create a mock DataFrame
    mock_df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])

    # Mock the spark.read.format().option()... chain
    with patch.object(spark.read, "format") as mock_format:
        mock_option = MagicMock()
        mock_format.return_value.option = mock_option
        mock_option.return_value.option = mock_option
        mock_option.return_value.load.return_value = mock_df
        print("mocked the data frame ")

        # Test data
        db_params = {
            "jdbcUrl": "jdbc:oracle:thin:@//localhost:1521/XE",
            "user": "test",
            "password": "test",
            "jdbcDriver": "oracle.jdbc.driver.OracleDriver"
        }

        # Call the function
        print("running the function with params")
        result = read_source_data(spark, "1,2,3", "4,5,6", 7, db_params)
        print("Function execution completed")
        # print(f"result from datafrome : {result.take(2).show()}")

        # Assertions
        assert result.count() == mock_df.count()
        assert result.columns == mock_df.columns

        # Verify that the correct methods were called
        mock_format.assert_called_once_with("jdbc")
        mock_option.assert_any_call("url", db_params["jdbcUrl"])
        mock_option.assert_any_call("user", db_params["user"])
        mock_option.assert_any_call("password", db_params["password"])
        mock_option.assert_any_call("driver", db_params["jdbcDriver"])

def test_transform_data(spark):
    input_data = [("2021-01-01", 1), ("2021-01-02", 2)]
    input_df = spark.createDataFrame(input_data, ["date", "value"])

    result = transform_data(spark, input_df)

    assert "LOAD_DATE" in result.columns
    assert "UPDATE_DATE" in result.columns
#
@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.write_target_data")
def test_write_target_data_with_real_df(spark):
    # Create a test DataFrame
    test_data = [(1, "test1"), (2, "test2")]
    test_df = spark.createDataFrame(test_data, ["id", "value"])

    # Mock the write operations
    with patch.object(test_df, 'write') as mock_write:
        mock_chain = MagicMock()
        mock_write.format.return_value = mock_chain
        mock_chain.option.return_value = mock_chain
        mock_chain.mode.return_value = mock_chain

        # Test data
        db_params = {
            "jdbcUrl": "jdbc:oracle:thin:@//localhost:1521/XE",
            "user": "test",
            "password": "test"
        }

        # Execute function
        write_target_data(spark, test_df, db_params)

        # Verify calls
        mock_write.format.assert_called_once_with("jdbc")
        assert mock_chain.option.call_count >= 3  # At least 3 options should be set
        mock_chain.mode.assert_called_once_with("append")
        mock_chain.save.assert_called_once()

#
@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.create_spark_session")
@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.execute_pre_sql")
@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.read_source_data")
@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.transform_data")
@patch("src.Local_m_SIS_RDS_Serv_ServreqHeader_Ins_ServServreqHeader.write_target_data")
def test_main(mock_write, mock_transform, mock_read, mock_execute, mock_create_session):
    mock_args = MagicMock()
    mock_args.mapping_name = "test"
    mock_args.event_type = "1,2,3"
    mock_args.status = "4,5,6"
    mock_args.days_offset = 7
    mock_args.retention_days = 30

    main(mock_args)

    mock_create_session.assert_called_once()
    mock_execute.assert_called_once()
    mock_read.assert_called_once()
    mock_transform.assert_called_once()
    mock_write.assert_called_once()