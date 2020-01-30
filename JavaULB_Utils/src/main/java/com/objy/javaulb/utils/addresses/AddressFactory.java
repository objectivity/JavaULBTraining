/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.objy.javaulb.utils.addresses;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class AddressFactory {

    private static Logger logger = LoggerFactory.getLogger(AddressFactory.class);

    private final String addressFilename;

    private ArrayList<Address> addressList = new ArrayList<>();
    private int addressListSize;


    private Random random = new Random();
    private Properties properties;



    public AddressFactory() throws FileNotFoundException, IOException, Exception {
        
        String appConfig = System.getProperty("AppConfig");
        if (appConfig == null) {
            throw new Exception("Missing environment variable 'AppConfig'. Use '-DAppConfig=<path>'");
        }        
        logger.info("AppConfig = " + appConfig);
        
        String dataDir = System.getProperty("DataDir");
        if (dataDir == null) {
            throw new Exception("Missing environment variable 'DataDir'. Use '-DDataDir=<path>'");
        }
        logger.info("DataDir = " + dataDir);
        
        File appConfigFile = new File(appConfig);
        if (!appConfigFile.exists()) {
            throw new FileNotFoundException("AppConfig file not found: " + appConfig);
        }
        
        FileInputStream inStream;
        try {
            inStream = new FileInputStream(appConfigFile);
        } catch(IOException ioe) {
            throw new IOException("Unable to read: " + appConfig);
        }
        
        properties = new Properties();
        properties.load(inStream);
               
        this.addressFilename = dataDir + File.separator + (String)properties.get("data.addresses");;

        File addressFile = new File(this.addressFilename);
        if (!addressFile.exists()) {
            throw new FileNotFoundException("Address file not found: " + this.addressFilename);
        } else {
            logger.info("Found: " + addressFile.getAbsolutePath());
        }

        loadAddressesFromFile(addressFile, addressList);
        addressListSize = addressList.size();
    }


    private void loadAddressesFromFile(File file, ArrayList<Address> addressList) throws FileNotFoundException, IOException {

        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            
            double longitude = Double.parseDouble(parts[6]);
            double latitude = Double.parseDouble(parts[7]);
            
            Address address = new Address(parts[1],parts[2],parts[3],parts[4],parts[5],longitude,latitude);
            
            addressList.add(address);
            count++;
            if ((count % 1000) == 0) {
                logger.info(file.getName() + " Loaded: " + count);
            }
        }
        logger.info(file.getName() + " Loaded: " + count);
        reader.close();
    }



    public Address getAddress() {

        return addressList.get((int)(random.nextDouble() * addressListSize));
        
    }


}
