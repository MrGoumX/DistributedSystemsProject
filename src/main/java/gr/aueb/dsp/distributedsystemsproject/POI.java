package gr.aueb.dsp.distributedsystemsproject;

import java.io.Serializable;

public class POI implements Serializable{
    private static final long serialVersionUID = 42L;
    private int id;
    private double latitude;
    private double longitude;
    private String r_id;
    private String photo;
    private String cat;
    private String name;
    private double distance;

    public POI(int id, String r_id, double latitude, double longitude, String photo, String cat, String name){
        this.id = id;
        this.r_id = r_id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.photo = photo;
        this.cat = cat;
        this.name = name;
    }
    public void setDistance(double distance){
        this.distance = distance;
    }

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
