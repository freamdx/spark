package org.apache.hive.service.cli;

import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT$;
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT$;

import java.util.HashMap;
import java.util.Map;

final class UDTUtil {
    private final static Map<String, String> udtTypes;

    static {
        udtTypes = new HashMap<String, String>();
        udtTypes.put(GeometryUDT$.MODULE$.simpleString(), Type.STRING_TYPE.getName());
        udtTypes.put(RasterUDT$.MODULE$.simpleString(), Type.STRING_TYPE.getName());
    }

    static boolean existsType(String simpleType) {
        return udtTypes.containsKey(simpleType);
    }

    static String getType(String simpleType) {
        return udtTypes.get(simpleType);
    }
}
