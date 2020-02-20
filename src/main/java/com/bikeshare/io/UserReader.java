package com.bikeshare.io;

import com.bikeshare.BikeshareConf;
import org.apache.parquet.format.StringType;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;


import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface UserReader{

    default Dataset<Row> readUserInfo(BikeshareConf conf, SparkSession spark) {
        Dataset<Row> rowsDS;
        try {
            rowsDS = spark.read().json(conf.uniqueUserPath);
        } catch (Exception e) {
            rowsDS = spark.emptyDataFrame().withColumn("user_id", lit("")).withColumn("first_timestamp", lit(""));
        }
        return rowsDS;
    }

}