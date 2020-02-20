package com.bikeshare.io;

import com.bikeshare.Utils;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import com.bikeshare.BikeshareConf;

public interface BikeStationInfoReader {
    default Dataset<Row> readBikeStation(BikeshareConf conf, SparkSession spark) {
        String path = String.format("%s/bike-station-info", conf.inputMetaDataPath);

        Dataset<Row>  bikeStationDf = spark.read().json(path);
        return Utils.selectColumns(conf, "bike.station.info", bikeStationDf);
    }
}
