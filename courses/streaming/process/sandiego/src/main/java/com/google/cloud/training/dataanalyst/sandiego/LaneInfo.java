
package com.google.cloud.training.dataanalyst.sandiego;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class LaneInfo {
  private String[] fields;

  private enum Field {
    FREEWAY_DIR, FREEWAY_ID, LANE, LATITUDE, LONGITUDE, SENSOR_ID, SPEED, TIMESTAMP;
  }

  public LaneInfo() {
    // for Avro
  }

  public static LaneInfo newLaneInfo(String line) {
    String[] pieces = line.split(",");
    for (int i=0; i < pieces.length; i++) {
      String[] noKey = pieces[i].split(":", 2);
      String noQuotes = noKey[1].replaceAll("\"", "").replaceAll("[{}]", "");
      pieces[i] = noQuotes.trim();
    }
    LaneInfo info = new LaneInfo();
    info.fields = pieces;
    return info;
  }

  private String get(Field f) {
    return fields[f.ordinal()];
  }

  public String getTimestamp() {
    return fields[Field.TIMESTAMP.ordinal()];
    // return Timestamp.valueOf(fields[Field.TIMESTAMP.ordinal()]).getTime();
  }

  /**
   * Create unique key for sensor in a particular lane
   *
   * @return
   */
  public String getSensorKey() {
    return get(Field.SENSOR_ID);
  }

  /**
   * Create unique key for all the sensors for traffic in same direction at a
   * location
   *
   * @return
   */
  public String getLocationKey() {
    StringBuilder sb = new StringBuilder();
    for (int f = Field.LATITUDE.ordinal(); f <= Field.FREEWAY_DIR.ordinal(); ++f) {
      sb.append(fields[f]);
      sb.append(',');
    }
    return sb.substring(0, sb.length() - 1); // without trailing comma
  }

  public double getLatitude() {
    return Double.parseDouble(get(Field.LATITUDE));
  }

  public double getLongitude() {
    return Double.parseDouble(get(Field.LONGITUDE));
  }

  public String getHighway() {
    return get(Field.FREEWAY_ID);
  }

  public String getDirection() {
    return get(Field.FREEWAY_DIR);
  }

  public int getLane() {
    return Integer.parseInt(get(Field.LANE));
  }

  public double getSpeed() {
    return Double.parseDouble(get(Field.SPEED));
  }
}
