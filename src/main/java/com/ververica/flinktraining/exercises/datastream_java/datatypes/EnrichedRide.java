package com.ververica.flinktraining.exercises.datastream_java.datatypes;

import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.joda.time.DateTime;

public class EnrichedRide extends TaxiRide {

    public int startCell;
    public int endCell;


//    public long rideId;
//    public boolean isStart;
//    public DateTime startTime;
//    public DateTime endTime;
//    public float startLon;
//    public float startLat;
//    public float endLon;
//    public float endLat;
//    public short passengerCnt;
//    public long taxiId;
//    public long driverId;

    public EnrichedRide() {
    }

    public EnrichedRide(TaxiRide ride) {

        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.startTime = ride.startTime;
        this.endTime = ride.endTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;


        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    public String toString() {
        return super.toString() + "," +
                Integer.toString(this.startCell) + "," +
                Integer.toString(this.endCell);
    }

    public int getStartCell() {
        return startCell;
    }

    public void setStartCell(int startCell) {
        this.startCell = startCell;
    }

    public int getEndCell() {
        return endCell;
    }

    public void setEndCell(int endCell) {
        this.endCell = endCell;
    }
}
