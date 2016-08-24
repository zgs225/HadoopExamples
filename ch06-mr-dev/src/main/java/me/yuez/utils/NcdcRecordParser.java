package me.yuez.utils;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {

    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private String stationId;
    private int airTemperature;
    private String quality;
    private boolean airTemperatureMalformed;

    protected void parse(String record) {
        year = record.substring(15, 19);
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        String airTemperatureString = "0";
        airTemperatureMalformed = false;

        if (record.charAt(87) == '+')
            airTemperatureString = record.substring(88, 92);
        else if (record.charAt(87) == '-')
            airTemperatureString = record.substring(87, 92);
        else
            airTemperatureMalformed = true;

        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return !isMalformedTemperature() &&
                ! isMissingTemperature() &&
                quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getStationId() {
        return stationId;
    }

    public String getQuality() {
        return quality;
    }
}
