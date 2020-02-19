package com.bikeshare.process;

import com.bikeshare.BikeshareConf;
import com.bikeshare.Utils;
import com.bikeshare.io.BikeShareTripReader;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

class BikeShareProcess implements BikeShareTripReader {

    public static List<String> fields = Arrays.asList("user_id",
                "subscriber_type",
                "start_station_id",
                "end_station_id",
                "zip_code");

    public static String avgDurationSec = "avg_duration_sec";

    public static void main(String[] args) {
        BikeshareConf conf = new BikeshareConf();

        SparkSession spark = SparkSession
                .builder()
                .appName("Bike-share")
                .config("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                .getOrCreate();

        String outputPath = Utils.pathGenerator(conf.outputDataPath, conf.datePrefix, conf.processDate);

        BikeShareProcess process = new BikeShareProcess();
        process.bikeShareAgg(spark, conf, outputPath);
    }

    private void bikeShareAgg(SparkSession spark, BikeshareConf conf, String outputPath) {

        Dataset<Row> bikeShareDS = readBikeShareTrip(conf, spark);

        Column[] groupbyCols = new Column[fields.size()];
        fields.stream().map(x -> col(x)).collect(Collectors.toList()).toArray(groupbyCols);

        Dataset<Row> bikeShareAggDS = bikeShareDS
                .groupBy(groupbyCols)
      .agg(avg(col("duration_sec")).as(avgDurationSec));

        bikeShareAggDS.coalesce(1).write().mode(SaveMode.Overwrite).json(outputPath);
    }
}
