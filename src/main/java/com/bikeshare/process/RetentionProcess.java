package com.bikeshare.process;

import com.bikeshare.BikeshareConf;
import com.bikeshare.io.BikeShareTripReader;
import com.bikeshare.io.UserReader;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

class RetentionProcess implements UserReader, BikeShareTripReader {

    public static void main(String[] args) throws Exception {
        BikeshareConf conf = new BikeshareConf();

        SparkSession spark = SparkSession
                .builder()
                .appName("Bike-share")
                .getOrCreate();

        spark.conf().set("spark.sql.crossJoin.enabled", "true");
        RetentionProcess process = new RetentionProcess();
        process.retentionPrep(spark, conf);
    }
    // It may help you to understand if you go to Wiki page
    public void retentionPrep(SparkSession spark, BikeshareConf conf) throws Exception {
        Dataset<Row> bikeShareDS = readBikeShareTrip(conf, spark);
        Dataset<Row> userDS = readUserInfo(conf, spark);
        Dataset<Row> dayAgoBikeShareDS = readDayAgoBikeShareTrip(conf, spark);

        Dataset<Row> joinedBikeShareDS = bikeShareDS
                .join(userDS, bikeShareDS.col("user_id").equalTo(userDS.col("user_id")), "left")
                .drop(bikeShareDS.col("user_id"));

        Dataset<Row> bikeUserAgeDays = joinedBikeShareDS.withColumn("user_age_days",
                floor((unix_timestamp(col("start_timestamp"), "yyyy-MM-dd'T'HH:mm:ss").minus(unix_timestamp(col("first_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))).divide(86400.0)));

        Dataset<Row> bikeFilteredDf = null;
        switch (conf.dayAgo) {
            case 1:
                bikeFilteredDf = bikeUserAgeDays.filter(col("user_age_days").equalTo(1));
                break;
            case 3:
                bikeFilteredDf = bikeUserAgeDays.filter(col("user_age_days").equalTo(3));
                break;
            case 7:
                bikeFilteredDf = bikeUserAgeDays.filter(col("user_age_days").equalTo(7));
                break;
            default:
                throw new Exception("input date is invalid");
        }

        Dataset<Row> bikeFilteredAgoDS = bikeFilteredDf.select("user_id", "user_age_days").distinct();

        // Get retention by calculate dayAgoBikeShareDS and bikeFilteredAgoDS
        Dataset<Row> aggPrepDS = dayAgoBikeShareDS.join(bikeFilteredAgoDS,
                dayAgoBikeShareDS.col("user_id").equalTo(bikeFilteredAgoDS.col("user_id")), "left")
                .drop(bikeFilteredAgoDS.col("user_id"));

        List<String> groupbyFields = new ArrayList<>(BikeShareProcess.fields);
        groupbyFields.add(BikeShareProcess.avgDurationSec);
        Column[] groupbyCols = new Column[groupbyFields.size()];
        groupbyFields.stream().map(x -> col(x)).collect(Collectors.toList()).toArray(groupbyCols);

        Dataset<Row> bikeUserAggDf = aggPrepDS.groupBy(groupbyCols)
                .agg(
                        max(when(col("user_age_days").equalTo(1), 1).otherwise(0)).alias("retention_1"),
                        max(when(col("user_age_days").equalTo(3), 1).otherwise(0)).alias("retention_3"),
                        max(when(col("user_age_days").equalTo(7), 1).otherwise(0)).alias("retention_7")
                );

        String outputPath = dayAgoWriteDataOutPath(conf);
        bikeUserAggDf.coalesce(1).write().mode(SaveMode.Overwrite).json(outputPath);
    }

}