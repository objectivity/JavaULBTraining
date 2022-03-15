package com.objy.javaulb.labs.lab04;

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
import com.objy.statement.Statement;
import java.io.File;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab04c {

    private static Logger logger = LoggerFactory.getLogger(Lab04c.class);

    private static final boolean SIGNED = true;
    private static final boolean UNSIGNED = false;

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;


    private NameFactory nameFactory;

    public Lab04c() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();



            nameFactory = new NameFactory();


            openConnection(bootFile);

            createPersonSchema();


            int count = 1000;
            createPeople(count);

            String doQuery1 = "FROM Person "
                    + "WHERE LastName =~ 'M.*' "
                    + "AND FirstName =~ 'T.*' "
                    + "RETURN LastName";
            query(doQuery1);

            String doQuery2 = "FROM Person "
                    + "WHERE LastName =~ 'M.*' "
                    + "AND FirstName =~ 'T.*' "
                    + "RETURN LastName, FirstName";
            query(doQuery2);

            String doQuery3 = "FROM Person "
                    + "WHERE LastName =~ 'M.*' "
                    + "AND FirstName =~ 'T.*' "
                    + "RETURN LastName, FirstName, $$ID as oid";
            query(doQuery3);


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
                cBuilder.addAttribute(LogicalType.STRING, "MiddleName");

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

    private String createPeople(int count) {

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


                for (int i = 0; i < 1000; i++) {
                    Name name = nameFactory.createName();

                    //logger.info("Name: " + name.first + " " + name.middle + " " + name.last);

                    // Using the cPerson Class object, create a Person Instance.
                    Instance iPerson = Instance.createPersistent(cPerson);

                    //logger.info("iPerson OID: " + iPerson.getIdentifier().toString());

                    // We access the value of each attribute in the Instance using
                    // a variable that we 'associate' with each attribute.
                    Variable vFirstName = iPerson.getAttributeValue("FirstName");
                    vFirstName.set(name.first);

                    Variable vMiddleInitial = iPerson.getAttributeValue("MiddleName");
                    vMiddleInitial.set(name.middle);

                    Variable vLastName = iPerson.getAttributeValue("LastName");
                    vLastName.set(name.last);

                    if ((i%100) == 0) {
                        logger.info("Persons created: " + i);
                    }
                }

                // The complete writes the data out to the database.
                tx.complete();

                tx.close();

                //logger.info("Person class created in federation.");

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


    private void query(String doQuery) {

        print("");
        print("");
        print("========================================================");
        print("QUERY: " + doQuery);
        print("--------------------------------------------------------");

        String oid = null;

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                Variable vStatementExecute;

                Statement statement = new Statement("DO", doQuery);

                vStatementExecute = statement.execute();


                java.util.Iterator<Variable> it = vStatementExecute.sequenceValue().iterator();
                if (!it.hasNext()) {
                    logger.info("There were no results on query:\n\n" + doQuery);
                }

                boolean headerPrinted = false;

                int resultCount = 0;
                while (it.hasNext()) {

                    Variable vResult = it.next();

                    Instance ix = vResult.instanceValue();

                    if (!headerPrinted) {
                        headerPrinted = true;
                        displayHeader(ix);
                    }

                    displayInstance(ix);

                    resultCount++;
                }

                print("");
                print("--------------------------------------------------------");
                print("Result Count: " + resultCount);

                // The complete writes the data out to the database.
                tx.complete();

                tx.close();

                //logger.info("Person class created in federation.");

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

        print("========================================================");
        print("");
        print("");
    }


    private void displayInstance(Instance ix) {

        com.objy.data.Class cx = ix.getClass(true);

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getLogicalType();

            switch (lt) {
                case STRING:
                    sb.append(String.format("%-15s    ", v.stringValue()));
                    break;
                case REFERENCE:
                    sb.append(String.format("%-15s    ", v.referenceValue().getIdentifier().toString()));
                    break;
                default:
                    sb.append(String.format("%-15s    ", "Not Handled"));
            }
        }

        print(sb.toString());
    }


    private void displayHeader(Instance ix) {

        com.objy.data.Class cx = ix.getClass(true);

        StringBuilder sb = new StringBuilder();
        StringBuilder sbSeparator = new StringBuilder();

        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getLogicalType();

            sb.append(String.format("%-15s    ", at.getName()));
            sbSeparator.append("---------------    ");
        }
        print(sb.toString());
        
        sb = new StringBuilder();
        
        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getLogicalType();
            
            sb.append(String.format("%-15s    ", lt.toString()));
        }

        print(sb.toString());        
        print(sbSeparator.toString());
    }


    
    private void print(String s) {
        
        System.out.println(s);
        
//        logger.info(s);
    }



    public static void main(String[] args) {
        new Lab04c();
    }
}
