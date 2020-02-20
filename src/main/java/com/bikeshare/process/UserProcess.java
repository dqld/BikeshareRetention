package com.bikeshare.process;

import com.bikeshare.BikeshareConf;
import com.bikeshare.Utils;
import com.bikeshare.io.BikeShareTripReader;
import com.bikeshare.io.UserReader;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UserProcess implements UserReader, BikeShareTripReader {

    private static SparkSession spark = SparkSession
            .builder()
            .appName("Unique-users")
            .getOrCreate();

    public static void main(String[] args) throws Exception {
        BikeshareConf conf = new BikeshareConf();

        String inputPath = Utils.pathGenerator(conf.inputBikeSharePath, conf.datePrefix, conf.processDate);

        spark.conf().set("spark.sql.crossJoin.enabled", "true");
        UserProcess process = new UserProcess();
        process.uniqueUser(conf.uniqueUserPath, inputPath, conf);
    }

    private void uniqueUser(String uniqueUsersPath, String inputBikeSharePath, BikeshareConf conf) {

        Dataset<Row> inputUniqueUsersDS = readUserInfo(conf, spark);
        Dataset<Row> inputBikeShareDS = readBikeShareTrip(conf, spark);

        Dataset<Row> users = Utils.selectColumns(conf, "bike.unique.user", inputBikeShareDS)
                .withColumn("first_timestamp", inputBikeShareDS.col("start_timestamp"))
                .drop(inputBikeShareDS.col("start_timestamp"));

        Dataset<Row> uniqueUserDS = inputUniqueUsersDS.union(users)
                .groupBy("user_id")
                .agg(min(col("first_timestamp")).as("first_timestamp"));

        uniqueUserDS.distinct().coalesce(1).write().mode(SaveMode.Overwrite).json(uniqueUsersPath);
    }
}
