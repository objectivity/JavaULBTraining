package com.objy.javaulb.labs.lab03;

import com.objy.data.Attribute;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Variable;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import com.objy.javaulb.utils.names.Name;
import com.objy.javaulb.utils.names.NameFactory;
import java.io.File;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab03i {

    private static Logger logger = LoggerFactory.getLogger(Lab03i.class);

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;
    private NameFactory nameFactory;





    public Lab03i() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            openConnection(bootFile);

            createPersonSchema();

            createEmployeeSchema();

            readSchema("Person");
            readSchema("Employee");

            createData();

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
                cBuilder.addAttribute(LogicalType.DATE, "Birthdate");

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


    private void createEmployeeSchema() {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Use ClassBuilder to create the schema definition.
                com.objy.data.ClassBuilder cBuilder = new com.objy.data.ClassBuilder("Employee");

                // Set the superclass.
                cBuilder.setSuperclass("Person");

                cBuilder.addAttribute(LogicalType.STRING, "EmployeeID");


                // Actually build the the schema representation.
                com.objy.data.Class cEmployee = cBuilder.build();

                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cEmployee);

                // Process the schema changes.
                SchemaProvider.getDefaultPersistentProvider().activateEdits();

                // Complete and close the transaction
                tx.complete();
                tx.close();

                logger.info("Employee class created in schema.");

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


    private void readSchema(String classname) {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Lookup the "Person" class from the schema in the database.
                com.objy.data.Class cxClass = com.objy.data.Class.lookupClass(classname);

                logger.info("----------------------------------------------");
                logger.info("Displaying Attributes for type: " + classname);
                // Iterate over the attributes in our Person class.
                // getAttrbutes() returns an Interator<Variable> object.
                for (Variable v : cxClass.getAttributes()) {
                    Attribute at = v.attributeValue();
                    logger.info(String.format("Attribute:    %-20s    %s", at.getName(), at.getAttributeValueSpecification().getLogicalType()));
                }

                // Complete and close the transaction
                tx.complete();
                tx.close();

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


    private void createData() {

        try {
            nameFactory = new NameFactory();

            int transLCERetryCount = 0;
            boolean transactionSuccessful = false;
            while (!transactionSuccessful) {
                // Create a new TransactionScope that is READ_UPDATE.
                try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                    createSomePersons(100, nameFactory);
                    createSomeEmployees(100, nameFactory);


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

        } catch (Exception ex) {
            logger.error("Error: ", ex);
            return;
        }

    }


    private void createSomePersons(int personCount, NameFactory nameFactory) throws Exception {

        // Ensure that our view of the schema is up to date.
        SchemaProvider.getDefaultPersistentProvider().refresh(true);

        // Lookup the Person class from the schema in the ThingSpan federation.
        com.objy.data.Class cPerson = com.objy.data.Class.lookupClass("Person");

        Name name;

        for (int i = 0; i < personCount; i++) {
            
            name = nameFactory.createName();

            // Using the cPerson Class object, create a Person Instance.
            Instance iPerson = Instance.createPersistent(cPerson);

            logger.info("iPerson OID: " + iPerson.getObjectId().toString());

            // We access the value of each attribute in the Instance using
            // a variable that we 'associate' with each attribute.
            Variable vFirstName = iPerson.getAttributeValue("FirstName");
            vFirstName.set(name.first);

            Variable vMiddleInitial = iPerson.getAttributeValue("MiddleInitial");
            vMiddleInitial.set(name.middle.substring(0,1));

            Variable vLastName = iPerson.getAttributeValue("LastName");
            vLastName.set(name.last);
        }



        
    }

    private void createSomeEmployees(int employeeCount, NameFactory nameFactory) {

        // Ensure that our view of the schema is up to date.
        SchemaProvider.getDefaultPersistentProvider().refresh(true);


        // Lookup the Person class from the schema in the ThingSpan federation.
        com.objy.data.Class cEmployee = com.objy.data.Class.lookupClass("Employee");

        Name name;
        int employeeId = 1000;

        for (int i = 0; i < employeeCount; i++) {

            name = nameFactory.createName();

            // Using the cPerson Class object, create a Person Instance.
            Instance iEmployee = Instance.createPersistent(cEmployee);

            logger.info("iEmployee OID: " + iEmployee.getObjectId().toString());

            // We access the value of each attribute in the Instance using
            // a variable that we 'associate' with each attribute.
            Variable vFirstName = iEmployee.getAttributeValue("FirstName");
            vFirstName.set(name.first);

            Variable vMiddleInitial = iEmployee.getAttributeValue("MiddleInitial");
            vMiddleInitial.set(name.middle.substring(0,1));

            Variable vLastName = iEmployee.getAttributeValue("LastName");
            vLastName.set(name.last);

            Variable vEmployeeId = iEmployee.getAttributeValue("EmployeeID");
            vEmployeeId.set("" + ++employeeId);
                
	}
    }


    public static void main(String[] args) {
        new Lab03i();
    }
}
