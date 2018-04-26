package main;

import java.io.Serializable;

public class POI implements Serializable{
    private int id;
    private String name;
    private double latitude;
    private double longitude;

    public POI(int id, String name, double latitude, double longitude){
        this.id = id;
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public int getId(){
        return id;
    }

    public String getName(){
        return name;
    }

    public double getLatitude(){
        return latitude;
    }

    public double getLongitude(){
        return longitude;
    }

    public String toString(){
        return "POI with ID: " + id + " and name: " + name + "at pos: " + latitude + " " + longitude;
    }
}
