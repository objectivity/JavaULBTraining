/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.objy.javaulb.labs.lab05.addresses;

/**
 *
 * @author Daniel
 */
public class Address {

    public String number;
    public String street;
    public String city;
    public String state;
    public String zip;
    public double latitude;
    public double longitude;


    public Address(
            String number,
            String street,
            String city,
            String state,
            String zip,
            double latitude,
            double longitude) {

        this.number = number;
        this.street = street;
        this.city = city;
        this.state = state;
        this.zip = zip;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    
    

    public String toString() {
        return number + " " + street + "\n"
               + city + ", " + state + " " + zip + "\n"
                + latitude + " / " + longitude + "\n";
    }

}
