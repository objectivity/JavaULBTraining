package com.objy.javaulb.labs.lab02;

import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.dataSpecificationBuilder.*;

import com.objy.data.LogicalType;
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
public class Lab02iSetUniDirRefToMany {

    private static Logger logger = LoggerFactory.getLogger(Lab02iSetUniDirRefToMany.class);

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;





    public Lab02iSetUniDirRefToMany() {

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
     * This function will create two classes, an Egg class and a Carton class.
     * The Carton class has an attribute that is a list of references to Egg 
     * objects.
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
                com.objy.data.ClassBuilder cBuilderPhoneNumber = new com.objy.data.ClassBuilder("PhoneNumber");
                cBuilderPhoneNumber.addAttribute("Number",LogicalType.STRING);
                
                // Actually build the the schema representation.
                com.objy.data.Class cPhoneNumber = cBuilderPhoneNumber.build();
                
                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cPhoneNumber);
                
                
                
                //--------------------------------------------------------------
                // Use ClassBuilder to create the schema definition.
                com.objy.data.ClassBuilder cBuilderPerson = new com.objy.data.ClassBuilder("Person");
                cBuilderPerson.addAttribute("FirstName", LogicalType.STRING);
                cBuilderPerson.addAttribute("LastName", LogicalType.STRING);
                cBuilderPerson.addAttribute("MiddleInitial", LogicalType.STRING);
                cBuilderPerson.addAttribute("Birthdate", LogicalType.DATE); 
                
                cBuilderPerson.addAttribute("PhoneNumbers", 
                         new SetSpecificationBuilder()
                                .setCollectionName("TreeSetOfReferences")
                                .setElementSpecification(
                                    new ReferenceSpecificationBuilder()
                                            .setReferencedClass("PhoneNumber")
                                            .build())   
                                .build());
                
                // Actually build the the schema representation.
                com.objy.data.Class cPerson = cBuilderPerson.build();
                
                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cPerson);
                
                // Process the schema changes.
                SchemaProvider.getDefaultPersistentProvider().activateEdits();
                
                
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
        new Lab02iSetUniDirRefToMany();
    }
}
