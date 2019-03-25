package com.objy.javaulb.labs.lab01;

import com.objy.db.Connection;
import java.io.File;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab01 {

    private static Logger logger = LoggerFactory.getLogger(Lab01.class);

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile. 
    private String bootFile;
    
    // The connection to the ThingSpan federation.
    private Connection connection;





    public Lab01() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();
            
            openConnection(bootFile);             
            
            closeConnection();
            
        } catch (Exception ex) {
            logger.error("Error: ", ex);
            return;
        }


    }


    private void validateProperties() throws Exception {

        properties = System.getProperties();
        
        String bootFileProperty = System.getProperty("BOOT_FILE");
	if (bootFileProperty == null) {
	    String msg = "BOOT_FILE property not defined.";
	    logger.error(msg);
	    throw new Exception(msg);
	}

        bootFile = bootFileProperty.replace('/', File.separatorChar);

	logger.info("bootFile: <" + bootFile + ">");

	File f = new File(bootFile);
	if (!f.exists()) {
	    logger.error("Boot file is invalid. It does not exist: <" + bootFile + ">");
	} else {
	    logger.info("Boot file is valid: " + bootFile);
	}
        
    }

    
    private void openConnection(String bootFile) throws Exception {
        
        connection = new Connection(bootFile);
        
        logger.info("Connected to ThingSpan federation: " + bootFile);
        
    }
    
    
    
    private void closeConnection() throws Exception {
        
        connection.dispose();
        
        logger.info("Disconnected from ThingSpan federation: " + bootFile);
        
    }
    
    
    public static void main(String[] args) {
        new Lab01();
    }
}
