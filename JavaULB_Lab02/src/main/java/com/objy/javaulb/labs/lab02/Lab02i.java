package com.objy.javaulb.labs.lab02;

import com.objy.data.Encoding;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.dataSpecificationBuilder.*;

import com.objy.data.LogicalType;
import com.objy.data.Storage;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import java.io.File;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab02i {

    private static Logger logger = LoggerFactory.getLogger(Lab02i.class);

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;





    public Lab02i() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            openConnection(bootFile);

            createSchema();

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



    /**
     * This function will create two classes, a Geolocation class and an
     * Address class.
     */
    private void createSchema() {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                //--------------------------------------------------------------
                // Use ClassBuilder to create the schema definition.
                com.objy.data.ClassBuilder cBuilderGeoLoc = new com.objy.data.ClassBuilder("GeoLocation");
                cBuilderGeoLoc.setEmbeddable();
                cBuilderGeoLoc.addAttribute("Latitude",
                                      new RealSpecificationBuilder(Storage.Real.B64)
                                            .setEncoding(Encoding.Real.IEEE)
                                            .build());
                cBuilderGeoLoc.addAttribute("Longitude",
                                      new RealSpecificationBuilder(Storage.Real.B64)
                                            .setEncoding(Encoding.Real.IEEE)
                                            .build());

                // Actually build the the schema representation.
                com.objy.data.Class cGeoLoc = cBuilderGeoLoc.build();


                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cGeoLoc);



                com.objy.data.ClassBuilder cBuilderAddress = new com.objy.data.ClassBuilder("Address");
                cBuilderAddress.addAttribute(LogicalType.STRING, "Street1");
                cBuilderAddress.addAttribute(LogicalType.STRING, "Street2");
                cBuilderAddress.addAttribute(LogicalType.STRING, "City");
                cBuilderAddress.addAttribute(LogicalType.STRING, "State");
                cBuilderAddress.addAttribute(LogicalType.STRING, "ZIP");

                cBuilderAddress.addAttribute("GeoLocation", new InstanceSpecificationBuilder()
                                    .setClass("GeoLocation")
                                    .build());

                // Actually build the the schema representation.
                com.objy.data.Class cAddress = cBuilderAddress.build();

                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cAddress);





                // Complete and close the transaction
                tx.complete();
                tx.close();

                logger.info("Person class created in schema.");

                transactionSuccessful = true;

	    } catch(LockConflictException lce) {
		logger.info("LockConflictException. Attempting retry...  retryCount = " + ++transLCERetryCount);
		try {
		    Thread.sleep(10*transLCERetryCount);
		} catch(InterruptedException ie) { }

	    } catch (Exception ex) {
		ex.printStackTrace();
                break;
	    }
	}
    }


    public static void main(String[] args) {
        new Lab02i();
    }
}
