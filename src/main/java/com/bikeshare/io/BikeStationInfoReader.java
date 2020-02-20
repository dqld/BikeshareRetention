package com.bikeshare.io;

import com.bikeshare.BikeshareConf;
import com.bikeshare.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface BikeStationInfoReader {
    default Dataset<Row> readBikeStation(BikeshareConf conf, SparkSession spark) {
        String path = String.format("%s/bike-station-info", conf.inputMetaDataPath);

        Dataset<Row>  bikeStationDf = spark.read().json(path);
        return Utils.selectColumns(conf, "bike.station.info", bikeStationDf);
    }
}
