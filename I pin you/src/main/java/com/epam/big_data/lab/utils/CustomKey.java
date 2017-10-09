package com.epam.big_data.lab.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {

    private String cityOrRegionId;
    private String operatingSystemType;

    public CustomKey() {
    }

    public CustomKey(String cityOrRegionId, String operatingSystemType) {
        this.cityOrRegionId = cityOrRegionId;
        this.operatingSystemType = operatingSystemType;
    }

    private String getCityOrRegionId() {
        return cityOrRegionId;
    }

    String getOperatingSystemType() {
        return operatingSystemType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(cityOrRegionId);
        out.writeChars(operatingSystemType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityOrRegionId = in.readLine();
        operatingSystemType = in.readLine();
    }

    @Override
    public int hashCode() {
        return cityOrRegionId == null ? 0 : cityOrRegionId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CustomKey other = (CustomKey) obj;
        return (cityOrRegionId != null || other.cityOrRegionId == null) && cityOrRegionId != null
                && cityOrRegionId.equals(other.cityOrRegionId);
    }

    @Override
    public int compareTo(CustomKey o) {
        return cityOrRegionId.compareTo(o.getCityOrRegionId());
    }

    @Override
    public String toString() {
        return "CustomKey [cityOrRegionId=" + cityOrRegionId + ", operationSystemType=" + operatingSystemType + "]";
    }

}
