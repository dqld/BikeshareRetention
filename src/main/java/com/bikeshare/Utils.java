package com.bikeshare;

import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

    public static Dataset selectColumns(BikeshareConf conf, String sourceKey, Dataset ds) {
        List<Column> fields = getListFromConf(conf.selectColumnsConfigFile, sourceKey).stream().map(x -> new Column(x)).collect(Collectors.toList());
        Column[] cols = new Column[fields.size()];

        return ds.select(fields.toArray(cols));
    }

    public static List<String> getListFromConf(String configFileName, String confKey) {
        try {
            return ConfigFactory.load(configFileName).getStringList(confKey);
        } catch (Exception e) {
            System.out.println(String.format("*** Error parsing for %s as List[String] from %s ***\n %s", confKey, configFileName, e.getMessage()));
        }
        return new ArrayList<String>();
    }

    public static String pathGenerator(String inputParentPath, String datePrefix, String processDate) {
        return String.format("%s/%s/%s/", inputParentPath, datePrefix, processDate);
    }

    public static String getSysParam(String name, String def) {
        return System.getProperty(name, def);
    }

    public static int getSysParamInt(String name, int def) throws NumberFormatException {
        String nameStr = System.getProperty(name);
        if (nameStr == null || StringUtils.isEmpty(nameStr)) return def;
        return Integer.parseInt(nameStr);
    }
}
