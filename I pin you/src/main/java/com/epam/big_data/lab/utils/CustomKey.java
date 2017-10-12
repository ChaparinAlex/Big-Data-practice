package com.epam.big_data.lab.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {

    private Text cityOrRegionName = new Text();
    private Text operatingSystemType = new Text();


    public CustomKey() {
    }

    public CustomKey(Text cityOrRegionName, Text operatingSystemType) {
        this.cityOrRegionName = cityOrRegionName;
        this.operatingSystemType = operatingSystemType;
    }

    public Text getCityOrRegionName() {
        return cityOrRegionName;
    }

    public Text getOperatingSystemType() {
        return operatingSystemType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        cityOrRegionName.write(out);
        operatingSystemType.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityOrRegionName.readFields(in);
        operatingSystemType.readFields(in);
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cityOrRegionName == null) ? 0 : cityOrRegionName.hashCode());
        result = prime * result + ((operatingSystemType == null) ? 0 : operatingSystemType.hashCode());
        return result;
    }

   @Override
    public boolean equals(Object o){
        if(o instanceof CustomKey){
            CustomKey ck = (CustomKey) o;
            return cityOrRegionName.equals(ck.getCityOrRegionName()) &&
                    operatingSystemType.equals(ck.getOperatingSystemType());
        }
        return false;
    }

    @Override
    public int compareTo(CustomKey o) {
        int returnValue = cityOrRegionName.compareTo(o.getCityOrRegionName());
        if (returnValue != 0) {
            return returnValue;
        }
        return operatingSystemType.compareTo(o.getOperatingSystemType());
    }

    @Override
    public String toString() {
        return "CustomKey [cityOrRegionId=" + cityOrRegionName.toString() +
                ", operatingSystemType=" + operatingSystemType.toString() + "]";
    }

}
