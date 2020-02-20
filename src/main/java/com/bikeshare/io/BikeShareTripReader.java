package com.bikeshare.io;

import com.bikeshare.BikeshareConf;
import com.bikeshare.Utils;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.lit;

public interface BikeShareTripReader {

    default Dataset readBikeShareTrip(BikeshareConf conf, SparkSession spark) {
        String path = Utils.pathGenerator(conf.inputBikeSharePath, conf.datePrefix, conf.processDate);

        Dataset<Row> bikeShareDS;
        try {
            bikeShareDS = spark.read().json(path);
        } catch (Exception e) {
            bikeShareDS = spark.emptyDataFrame().withColumn("user_id", lit(""))
                    .withColumn("subscriber_type", lit(""))
                    .withColumn("start_station_id", lit(""))
                    .withColumn("end_station_id", lit(""))
                    .withColumn("zip_code", lit(""))
                    .withColumn("duration_sec", lit(0.0))
                    .withColumn("start_timestamp", lit(""));
        }
        return bikeShareDS;
    }

    default String dayAgoReadDataOutPath(BikeshareConf conf) throws Exception {
        String dateString = dayAgoDateString(conf);

        String path = "";
        switch (conf.dayAgo) {
            case 1:
                path = Utils.pathGenerator(conf.outputDataPath, conf.datePrefix, dateString);
                break;
            case 3:
                path = Utils.pathGenerator(conf.outputDataPath + "/1", conf.datePrefix, dateString);
                break;
            case 7:
                path = Utils.pathGenerator(conf.outputDataPath + "/7", conf.datePrefix, dateString);
                break;
            default:
                throw new Exception("input date is invalid");
        }
        return path;

    }

    default Dataset<Row> readDayAgoBikeShareTrip(BikeshareConf conf, SparkSession spark) throws Exception {
        String path = dayAgoReadDataOutPath(conf);

        Dataset<Row> bikeShareDS;
        try {
            bikeShareDS = spark.read().json(path);
        } catch (Exception e) {
            bikeShareDS = spark.emptyDataFrame().withColumn("user_id", lit(""))
                    .withColumn("subscriber_type", lit(""))
                    .withColumn("start_station_id", lit(""))
                    .withColumn("end_station_id", lit(""))
                    .withColumn("zip_code", lit(""))
                    .withColumn("duration_sec", lit(0.0));
        }
        return bikeShareDS;
    }

    default String dayAgoWriteDataOutPath(BikeshareConf conf) throws Exception {
        String dateString = dayAgoDateString(conf);
        String path = "";
        switch (conf.dayAgo) {
            case 1:
                path = Utils.pathGenerator(conf.outputDataPath + "/1", conf.datePrefix, dateString);
                break;
            case 3:
                path = Utils.pathGenerator(conf.outputDataPath + "/3", conf.datePrefix, dateString);
                break;
            case 7:
                path = Utils.pathGenerator(conf.outputDataPath + "/7", conf.datePrefix, dateString);
                break;
            default:
                throw new Exception("input date is invalid");
        }
        return path;
    }

    default String dayAgoDateString(BikeshareConf conf) {
        DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime processDate = DateTime.parse(conf.processDate, dateFormat);
        return dateFormat.print(processDate.minusDays(conf.dayAgo));
    }

}