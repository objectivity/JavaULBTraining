package com.objy.javaulb.labs.lab03;

import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Variable;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab03d {

    private static Logger logger = LoggerFactory.getLogger(Lab03d.class);

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;





    public Lab03d() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            openConnection(bootFile);

            createSchemaPerson();

            GregorianCalendar gCal = new GregorianCalendar();
            
            
            gCal.set(Calendar.DAY_OF_MONTH, 28);
            gCal.set(Calendar.MONTH, 3);
            gCal.set(Calendar.YEAR, 2000);            
            Date dob = gCal.getTime();
            
            long ts = dob.getTime() + 10000000L;
            
            Date timestamp = new Date(ts);
            
            createPersonInstance("John", "Q", "Doe", dob, timestamp);
            
            
            gCal.set(Calendar.DAY_OF_MONTH, 15);
            gCal.set(Calendar.MONTH, 5);
            gCal.set(Calendar.YEAR, 1966);            
            dob = gCal.getTime();
            ts = dob.getTime() + 10000000L;            
            timestamp = new Date(ts);
            
            createPersonInstance("Mary", "B", "Smith", dob, timestamp);

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




    private void createPersonInstance(String firstName, 
                                    String middleInitial, 
                                    String lastName,
                                    Date dateOfBirth,
                                    Date timestamp) {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Lookup the Person class from the schema in the ThingSpan federation.
                com.objy.data.Class cPerson = com.objy.data.Class.lookupClass("Person");

                // Using the cPerson Class object, create a Person Instance.
                Instance iPerson = Instance.createPersistent(cPerson);

                logger.info("iPerson OID: " + iPerson.getIdentifier().toString());

                // We access the value of each attribute in the Instance using
                // a variable that we 'associate' with each attribute.
                Variable vFirstName = iPerson.getAttributeValue("FirstName");
                vFirstName.set(firstName);

                Variable vMiddleInitial = iPerson.getAttributeValue("MiddleInitial");
                vMiddleInitial.set(middleInitial);

                Variable vLastName = iPerson.getAttributeValue("LastName");
                vLastName.set(lastName);
                
                
                Variable vBirthdate = iPerson.getAttributeValue("DateOfBirth");
                GregorianCalendar gCal = new GregorianCalendar();
                gCal.setTime(dateOfBirth);
                    // java.util.Calendar months are zero-based. 2 == March.                
                vBirthdate.set(new com.objy.data.Date(
                                        gCal.get(Calendar.YEAR), 
                                        gCal.get(Calendar.MONTH), 
                                        gCal.get(Calendar.DAY_OF_MONTH)));
                
                
                Variable vTimestamp = iPerson.getAttributeValue("Timestamp");
                vTimestamp.set(new com.objy.data.DateTime((java.util.Date)timestamp));



                // The complete writes the data out to the database.
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




    private void createSchemaPerson() {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Use ClassBuilder to create the schema definition.
                com.objy.data.ClassBuilder cBuilder = new com.objy.data.ClassBuilder("Person");
                cBuilder.addAttribute(LogicalType.STRING, "FirstName");
                cBuilder.addAttribute(LogicalType.STRING, "LastName");
                cBuilder.addAttribute(LogicalType.STRING, "MiddleInitial");
                
                cBuilder.addAttribute(LogicalType.DATE, "DateOfBirth");
                
                cBuilder.addAttribute(LogicalType.DATE_TIME, "Timestamp");

                // Actually build the the schema representation.
                com.objy.data.Class cPerson = cBuilder.build();

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
	    }
	}
    }







    public static void main(String[] args) {
        new Lab03d();
    }
}
