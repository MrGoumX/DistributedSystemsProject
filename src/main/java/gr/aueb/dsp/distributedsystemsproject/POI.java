package gr.aueb.dsp.distributedsystemsproject;

import java.io.Serializable;

// POI Class for POI Information

public class POI implements Serializable{
    private static final long serialVersionUID = 42L; // Serial Version UID for Authentication
    private int id; // POI ID (NUMBER)
    private double latitude; // POI Latitude
    private double longitude; // POI Longitude
    private String r_id; // POI UID from JSON (STRING)
    private String photo; // POI Photo Link
    private String cat; // POI Category
    private String name; // POI Name
    private double distance; // POI Distance from Current User Position (Not Used if POI is not in recommendation)

    // Constructor
    public POI(int id, String r_id, double latitude, double longitude, String photo, String cat, String name){
        this.id = id;
        this.r_id = r_id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.photo = photo;
        this.cat = cat;
        this.name = name;
    }

    // Distance setter
    public void setDistance(double distance){
        this.distance = distance;
    }

    // Getters
    public int getId(){
        return id;
    }

    public String getR_id(){
        return r_id;
    }

    public double getLatitude(){
        return latitude;
    }

    public double getLongitude(){
        return longitude;
    }

    public String getPhoto(){
        return photo;
    }

    public String getCat(){
        return cat;
    }

    public String getName(){
        return name;
    }

    public double getDistance(){
        return distance;
    }

    public String toString(){
        return "POI with ID: " + id + "at pos: " + latitude + " " + longitude;
    }
}
