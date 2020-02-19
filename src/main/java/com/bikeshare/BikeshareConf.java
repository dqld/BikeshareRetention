package com.bikeshare;

public class BikeshareConf {

    public final String selectColumnsConfigFile = Utils.getSysParam("select.columns.config", "select-columns");

    public final String bikeTripKey = Utils.getSysParam("bike.trip.key", "bike-trips");

    public final String env = Utils.getSysParam("env", "stage");

    public final String inputBikeSharePath = Utils.getSysParam("input.bike.path", getBikeSharePath());

    private String getBikeSharePath() {
        switch (this.env) {
            case "test":
                return "gs://spark-zep/bikeshare_b";
            case "stage":
                return "gs://spark-zep/bikeshare_b";
            case "prod":
                return "gs://spark-zep/bikeshare_b";
        }
        return "";
    }

    public final String inputMetaDataPath = Utils.getSysParam("input.meta.path", getInputMetaDataPath());

    private  String getInputMetaDataPath() {
        switch (this.env) {
            case "test":
                return "gs://dataproc-c4988a84-0770-4a1c-8ac0-0102a723b09b-us-central1/google-cloud-dataproc-metainfo/bike-bike-station-info.json";
            case "stage":
                return "gs://dataproc-c4988a84-0770-4a1c-8ac0-0102a723b09b-us-central1/google-cloud-dataproc-metainfo/bike-bike-station-info.json";
            case "prod":
                return "gs://dataproc-c4988a84-0770-4a1c-8ac0-0102a723b09b-us-central1/google-cloud-dataproc-metainfo/bike-bike-station-info.json";
        }
        return "";
    }

    public final String outputDataPath = Utils.getSysParam("output.data.path", getOutputDataPath());

    private final String getOutputDataPath() {
        switch (this.env) {
            case "test":
                return "gs://spark-zep/output_d";
            case "stage":
                return "gs://spark-zep/output_d";
            case "prod":
                return "gs://spark-zep/output_d";
        }
        return "";
    }

    public final String datePrefix = Utils.getSysParam("date.prefix", "start_date");

    public final String processDate = Utils.getSysParam("process.date", "2014-01-01");

    public final String uniqueUserPath = Utils.getSysParam("unique.user.path", "gs://dataproc-c4988a84-0770-4a1c-8ac0-0102a723b09b-us-central1/google-cloud-dataproc-metainfo/_SUCCESS");

    public final int dayAgo = Utils.getSysParamInt("day.ago",1);

}
