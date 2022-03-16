package com.objy.javaulb.labs.lab03;

import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Reference;
import com.objy.data.Variable;
import com.objy.data.dataSpecificationBuilder.ListSpecificationBuilder;
import com.objy.data.dataSpecificationBuilder.ReferenceSpecificationBuilder;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.ObjectId;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import java.io.File;
import java.util.GregorianCalendar;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab03g {

    private static Logger logger = LoggerFactory.getLogger(Lab03g.class);

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;





    public Lab03g() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            openConnection(bootFile);

            createSchemaPerson();

            GregorianCalendar gCal = new GregorianCalendar();



            String pOID1 = createPersonInstance("Seth", "B", "Franklin");
            String pOID2 = createPersonInstance("Tracy", "A", "Michaels");
            String pOID3 = createPersonInstance("Diane", "M", "Thoman");

            establishKnows(pOID1, pOID2);
            establishKnows(pOID1, pOID3);
            
            
            
            removeKnows(pOID1, pOID2);




            closeConnection();

        } catch (Exception ex) {
            logger.error("Error: ", ex);
            return;
        }


    }
    
    
    private void removeKnows(String fromOID, String toOID) {
        
        logger.info("removeKnows(" + fromOID + ", " + toOID + ")");

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Lookup the Person associated with fromOID.
                Instance iPersonFrom = Instance.lookup(ObjectId.fromString(fromOID));

                // Lookup the Person associated with toOID.
                Instance iPersonTo = Instance.lookup(ObjectId.fromString(toOID));


                // Get the Knows list from iPersonFrom.
                Variable vKnows = iPersonFrom.getAttributeValue("Knows");
                com.objy.data.List knowsList = vKnows.listValue();
                
                
                // vKnownEntry represents the Person that is "Known"
                Reference rPersonTo = new Reference(iPersonTo);
                
                Variable vKnowsEntry;
                boolean found = false;
                for (int i = 0; i < knowsList.size(); i++) {
                    vKnowsEntry = knowsList.get(i);
                    Reference rPersonToCheck = vKnowsEntry.referenceValue();
                    
                    logger.info("Evaluating " + rPersonToCheck.getIdentifier().toString());
                                      
                    // Check to see if the referenced objects are the same by OID.
                    if (rPersonToCheck.getIdentifier().toString().equals(rPersonTo.getIdentifier().toString())) {
                        logger.info("Removing " + rPersonToCheck.getIdentifier().toString() + " from " + fromOID);
                        knowsList.remove(i);
                        found = true;
                        break;
                    }
                    
                    vKnowsEntry.clear();
                }
                
                if (!found) {
                    logger.info("The 'Knows' list in object " + fromOID + " did reference " + toOID + ". No action taken.");             
                }
                



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




    private void establishKnows(String fromOID, String toOID) {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

            // Lookup the Person associated with fromOID.
            Instance iPersonFrom = Instance.lookup(ObjectId.fromString(fromOID));

            // Lookup the Person associated with toOID.
            Instance iPersonTo = Instance.lookup(ObjectId.fromString(toOID));


            // Get the Knows list from iPersonFrom.
            Variable vKnows = iPersonFrom.getAttributeValue("Knows");
            com.objy.data.List knowsList = vKnows.listValue();

            // vKnownEntry represents the Person that is "Known"
            Reference refPersonTo = new Reference(iPersonTo);
            Variable vPersonTo = new Variable(refPersonTo);

            // Add the the vPersonTo entry to the knowsList.
            knowsList.add(vPersonTo);



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


    private String createPersonInstance(String firstName,
                                    String middleInitial,
                                    String lastName) {

        String personOID = null;

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

                // Get and retain the OID of the Instance object we just created.
                personOID = iPerson.getIdentifier().toString();

                logger.info("iPerson OID: " + iPerson.getIdentifier().toString());

                // We access the value of each attribute in the Instance using
                // a variable that we 'associate' with each attribute.


                // Set the FirstName.
                Variable vFirstName = iPerson.getAttributeValue("FirstName");
                vFirstName.set(firstName);

                // Set the MiddleInitial.
                Variable vMiddleInitial = iPerson.getAttributeValue("MiddleInitial");
                vMiddleInitial.set(middleInitial);

                // Set the LastName.
                Variable vLastName = iPerson.getAttributeValue("LastName");
                vLastName.set(lastName);


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

        return personOID;

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
                cBuilder.addAttribute("FirstName", LogicalType.STRING);
                cBuilder.addAttribute("LastName", LogicalType.STRING);
                cBuilder.addAttribute("MiddleInitial", LogicalType.STRING);

                cBuilder.addAttribute("DateOfBirth", LogicalType.DATE);

                // Create the "Knows" end of the bidirectional to-many reference.
                cBuilder.addAttribute("Knows",
                            new ListSpecificationBuilder()
                                .setElementSpecification(
                                new ReferenceSpecificationBuilder()
                                        .setReferencedClass("Person")
                                        .setInverseAttribute("KnownBy")
                                        .build())
                                    .build());

                // Create the "KnownBy" end of the bidirectional to-many reference.
                cBuilder.addAttribute("KnownBy",
                            new ListSpecificationBuilder()
                                .setElementSpecification(
                                new ReferenceSpecificationBuilder()
                                        .setReferencedClass("Person")
                                        .setInverseAttribute("Knows")
                                        .build())
                                    .build());


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
                break;
	    }
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
        new Lab03g();
    }
}
