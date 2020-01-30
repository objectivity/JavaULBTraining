package com.objy.javaulb.utils.names;

/**
 *
 * @author Daniel Hall
 */
public class Name {
    
    private String gender;
    
    public String first;
    public String middle;
    public String last;
    
    
    public Name(String gender, String fn, String mn, String ln) {
        this.gender = gender;
        this.first = fn;
        this.middle = mn;
        this.last = ln;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getFirstName() {
        return first;
    }

    public void setFirstName(String firstName) {
        this.first = firstName;
    }

    public String getMiddleName() {
        return middle;
    }

    public void setMiddleName(String middleName) {
        this.middle = middleName;
    }

    public String getLastName() {
        return last;
    }

    public void setLastName(String lastName) {
        this.last = lastName;
    }
    
    
    
    
    
}
