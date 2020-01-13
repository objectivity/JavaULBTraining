package com.objy.javaulb.labs.lab04;

import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Variable;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.ObjectId;
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
public class Lab04a {

    private static Logger logger = LoggerFactory.getLogger(Lab04a.class);

    private static final boolean SIGNED = true;
    private static final boolean UNSIGNED = false;

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;

    public Lab04a() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            openConnection(bootFile);

            createPersonSchema();

            String oid = createPersonInstance("John", "Q", "Doe");

            lookupPersonByOID(oid);

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

    private void createPersonSchema() {

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

            } catch (LockConflictException lce) {
                logger.info("LockConflictException. Attempting retry...  retryCount = " + ++transLCERetryCount);
                try {
                    Thread.sleep(10 * transLCERetryCount);
                } catch (InterruptedException ie) {
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private String createPersonInstance(String firstName, String middleInitial, String lastName) {

        String oid = null;

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

                oid = iPerson.getObjectId().toString();

                logger.info("iPerson OID: " + iPerson.getObjectId().toString());

                // We access the value of each attribute in the Instance using
                // a variable that we 'associate' with each attribute.
                Variable vFirstName = iPerson.getAttributeValue("FirstName");
                vFirstName.set(firstName);

                Variable vMiddleInitial = iPerson.getAttributeValue("MiddleInitial");
                vMiddleInitial.set(middleInitial);

                Variable vLastName = iPerson.getAttributeValue("LastName");
                vLastName.set(lastName);

                // The complete writes the data out to the database.
                tx.complete();

                tx.close();

                logger.info("Person object created in federation.");

                transactionSuccessful = true;

            } catch (LockConflictException lce) {
                logger.info("LockConflictException. Attempting retry...  retryCount = " + ++transLCERetryCount);
                try {
                    Thread.sleep(10 * transLCERetryCount);
                } catch (InterruptedException ie) {
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                break;
            }
        }

        return oid;
    }

    private void lookupPersonByOID(String oid) {

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Using the cPerson Class object, create a Person Instance.
                Instance iPerson = Instance.lookup(ObjectId.fromString(oid));

                logger.info("iPerson OID: " + iPerson.getObjectId().toString());

                // We access the value of each attribute in the Instance using
                // a variable that we 'associate' with each attribute.
                Variable vFirstName = iPerson.getAttributeValue("FirstName");
                logger.info(oid + " Person.FirstName:     " + vFirstName.stringValue());

                Variable vMiddleInitial = iPerson.getAttributeValue("MiddleInitial");
                logger.info(oid + " Person.MiddleInitial: " + vMiddleInitial.stringValue());

                Variable vLastName = iPerson.getAttributeValue("LastName");
                logger.info(oid + " Person.LastName:      " + vLastName.stringValue());

                // The complete writes the data out to the database.
                tx.complete();
                tx.close();

                transactionSuccessful = true;

            } catch (LockConflictException lce) {
                logger.info("LockConflictException. Attempting retry...  retryCount = " + ++transLCERetryCount);
                try {
                    Thread.sleep(10 * transLCERetryCount);
                } catch (InterruptedException ie) {
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                break;
            }
        }
    }

    public static void main(String[] args) {
        new Lab04a();
    }
}
