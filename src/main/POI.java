package main;

import java.io.Serializable;

public class POI implements Serializable{
    private int id;
    private double latitude;
    private double longitude;
    private String r_id;
    private String photo;
    private String cat;
    private String name;

    public POI(int id, String r_id, double latitude, double longitude, String photo, String cat, String name){
        this.id = id;
        this.r_id = r_id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.photo = photo;
        this.cat = cat;
        this.name = name;
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

    public String toString(){
        return "POI with ID: " + id + "at pos: " + latitude + " " + longitude;
    }
}
