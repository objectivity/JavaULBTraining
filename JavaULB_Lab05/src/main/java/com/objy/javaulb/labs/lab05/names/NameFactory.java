/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.objy.javaulb.labs.lab05.names;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class NameFactory {
    
    private static Logger logger = LoggerFactory.getLogger(NameFactory.class);

    private final String femaleNameFilename;
    private final String maleNameFilename;
    private final String lastNameFilename;
    
    private ArrayList<String> femaleNamesList = new ArrayList<>();
    private int femaleListSize;
    
    private ArrayList<String> maleNamesList = new ArrayList<>();
    private int maleListSize;
    
    private ArrayList<String> lastNamesList = new ArrayList<>();
    private int lastListSize;
    
    private Random random = new Random();
    
    
    
    public NameFactory(String femaleNameFilename, String maleNameFilename, String lastNameFilename) throws FileNotFoundException, IOException {        
        this.femaleNameFilename = femaleNameFilename;
        this.maleNameFilename = maleNameFilename;
        this.lastNameFilename = lastNameFilename;
        
        File femaleNameFile = new File(this.femaleNameFilename);
        if (!femaleNameFile.exists()) {
            throw new FileNotFoundException("Female names file not found: " + this.femaleNameFilename);
        } else {
            logger.info("Found: " + femaleNameFile.getAbsolutePath());
        }
        
        File maleNameFile = new File(this.maleNameFilename);
        if (!maleNameFile.exists()) {
            throw new FileNotFoundException("Male names file not found: " + this.maleNameFilename);
        } else {
            logger.info("Found: " + maleNameFile.getAbsolutePath());
        }
        
        File lastNameFile = new File(this.lastNameFilename);
        if (!lastNameFile.exists()) {
            throw new FileNotFoundException("Last names file not found: " + this.lastNameFilename);
        } else {
            logger.info("Found: " + lastNameFile.getAbsolutePath());
        }
        
        loadNamesFromFile(femaleNameFile, femaleNamesList);
        femaleListSize = femaleNamesList.size();
        
        loadNamesFromFile(maleNameFile, maleNamesList);
        maleListSize = maleNamesList.size();
        
        loadNamesFromFile(lastNameFile, lastNamesList);
        lastListSize = lastNamesList.size();
        
        
    }
    
    
    private void loadNamesFromFile(File file, ArrayList<String> namesList) throws FileNotFoundException, IOException {
        
        BufferedReader reader = new BufferedReader(new FileReader(file));
        
        String line;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            String name = line.substring(0, line.indexOf(" "));
            namesList.add(name);
            count++;
        }
        logger.info(file.getName() + " Loaded: " + count);
        reader.close();
    }
    
    
    
    public Name createName() {
        
        String gender;
        String lastName;
        String firstName;
        String middleName;
        
        
        if (random.nextDouble() < 0.5) {
            gender = "Female";
            firstName = femaleNamesList.get((int)(random.nextDouble() * femaleListSize));
            middleName = femaleNamesList.get((int)(random.nextDouble() * femaleListSize));
        } else {
            gender = "Male";
            firstName = maleNamesList.get((int)(random.nextDouble() * maleListSize));
            middleName = maleNamesList.get((int)(random.nextDouble() * maleListSize));
        }
        
        lastName = lastNamesList.get((int)(random.nextDouble() * lastListSize));
        
        return new Name(gender, firstName, middleName, lastName);
    }
    
    
}
