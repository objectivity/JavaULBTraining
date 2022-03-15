package com.objy.javaulb.lab06.collections.maps;

import com.objy.data.Attribute;
import com.objy.data.Edge;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Variable;
import com.objy.data.Walk;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.ObjectivityException;
import com.objy.db.SessionLogging;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import com.objy.statement.Statement;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab06MapsB {

    private static Logger logger = LoggerFactory.getLogger(Lab06MapsB.class);


    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;


    public Lab06MapsB() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            SessionLogging.setLoggingOptions(SessionLogging.LogAll, properties.getProperty("SESSION_LOG_DIR"));

            openConnection(bootFile);

            try {
                createSchema();

                addEntries();

                readEntries();

            }catch(Exception ex) {
                ex.printStackTrace();
                return;
            }

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

    private void createSchema() {

        logger.info("=====================================================");
        logger.info("createSchema() - Begin...");

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                Statement statement;

                try {

                    String doQuery
                            = "UPDATE SCHEMA {\n"
                            + "    CREATE CLASS Contract {\n"
                            + "         Number : String\n"
                            + "     }\n"
                            + "    CREATE CLASS Customer {\n"
                            + "         Name : String\n"
                            + "     }\n"
                            + "    CREATE CLASS MyType {\n"
                            + "         MyName : String,\n"
                            + "         MyMap  : Map {\n"
                            + "                     CollectionTypeName :HashMapOfReferences,\n"
                            + "                     Key :Reference { Referenced: Contract },\n"
                            + "                     Element : Reference { Referenced: Customer }\n"
                            + "                 }\n"
                            + "     }\n"
                            + "}";

                    logger.info("doQuery: <" + doQuery + ">");
                    statement = new Statement("DO", doQuery);

                    statement.execute();

                } catch (ObjectivityException oe) {
                    oe.printStackTrace();
                }

                logger.info("Calling tx.complete()...");


                // Complete and close the transaction
                tx.complete();
                tx.close();

                logger.info("Back from tx.close()");

                transactionSuccessful = true;

            } catch (LockConflictException lce) {
                logger.info("LockConflictException. Attempting retry...  retryCount = " + ++transLCERetryCount);
                try {
                    Thread.sleep(10 * transLCERetryCount);
                } catch (InterruptedException ie) {
                }

            } catch (Exception ex) {
                logger.error("Error: ", ex);
                ex.printStackTrace();
                throw ex;
            }
        }

        logger.info("createSchema() - End...");
    }





    class Data {
        public String number;
        public String name;

        public Data(String number, String name) {
            this.number = number;
            this.name = name;
        }
    }





    /**
     * This method creates an instance of MyType which contains a com.objy.data.Map
     * attribute and then puts some things in the map.
     */
    private void addEntries() {

        logger.info("=====================================================");
        logger.info("addEntries() - Begin...");

        Data[] data = {
            new Data("11111", "Jones"),
            new Data("22222", "Smith"),
            new Data("33333", "Graham"),
            new Data("44444", "Duval")
        };

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Create an instance of MyType
                com.objy.data.Class cMyType = com.objy.data.Class.lookupClass("MyType");
                Instance iMT = Instance.createPersistent(cMyType);

                // Get the Map attribute from the instance.
                com.objy.data.Map map = iMT.getAttributeValue("MyMap").mapValue();


                // Look up the Customer and Contract classes.
                com.objy.data.Class cCustomer = com.objy.data.Class.lookupClass("Customer");
                com.objy.data.Class cContract = com.objy.data.Class.lookupClass("Contract");

                for (Data dataEntry : data) {

                    // Create the target Contract object.
                    Instance iContract = Instance.createPersistent(cContract);
                    iContract.getAttributeValue("Number").set(dataEntry.number);

                    // Create the target Customer object.
                    Instance iCustomer = Instance.createPersistent(cCustomer);
                    iCustomer.getAttributeValue("Name").set(dataEntry.name);


                    // Create a Variable to represent the key.
                    Variable vKey = new Variable(new com.objy.data.Reference(iContract));

                    // We need an empty Variable that will hold the Reference
                    // to the iKV object.
                    Variable vValue = new Variable(new com.objy.data.Reference(iCustomer));

                    logger.info("Adding: " + dataEntry.number + " --> " + dataEntry.name);

                    // Put the vKey and vValue into the map attribute.
                    map.put(vKey, vValue);
                }

                logger.info("Calling tx.complete()...");


                // Complete and close the transaction
                tx.complete();
                tx.close();

                logger.info("Back from tx.close()");

                transactionSuccessful = true;

            } catch (LockConflictException lce) {
                logger.info("LockConflictException. Attempting retry...  retryCount = " + ++transLCERetryCount);
                try {
                    Thread.sleep(10 * transLCERetryCount);
                } catch (InterruptedException ie) {
                }

            } catch (Exception ex) {
                logger.error("Error: ", ex);
                ex.printStackTrace();
                throw ex;
            }
        }

        logger.info("addEntries() - End...");
    }



    private void readEntries() {

        logger.info("=====================================================");
        logger.info("readEntries() - Begin...");

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                Statement statement;

                try {
                    String doQuery = "FROM MyType return *";

                    logger.info("doQuery: <" + doQuery + ">");
                    statement = new Statement("DO", doQuery);

                    Iterator<Variable> it = statement.execute().sequenceValue().iterator();



                    // Loop over the MyType objects in the results.
                    while (it.hasNext()) {

                        Variable v = it.next();

                        Instance iMT = v.instanceValue();

                        logger.info("MyType OID = " + iMT.getIdentifier().toString());

                        // Get the MyMap attribute from the current MyType object.
                        Variable vMTMyMap = iMT.getAttributeValue("MyMap");

                        // Convert the Variable to Map.
                        com.objy.data.Map map = vMTMyMap.mapValue();

                        ArrayList<String> keys = new ArrayList<>();

                        // Iteratr over the keys in the map.
                        Iterator<Variable> itKeys = map.keys().iterator();
                        while (itKeys.hasNext()) {
                            Variable vKey = itKeys.next();

                            // Get the object referenced by the current key.
                            // Remember that the key is a reference (OID).
                            com.objy.data.Reference rKey = vKey.referenceValue();


                            keys.add(rKey.getIdentifier().toString());


                            // Within the map, the value is a Reference.
                            // Get the referenced object which is of type KVPair.
                            com.objy.data.Reference rValue = map.get(vKey).referenceValue();

                            logger.info("MyType.MyMap: iKey <" + rKey.getIdentifier() + ">   iValue <" + rValue.getIdentifier() + ">");

                            Instance iKey = rKey.getReferencedObject();
                            Instance iValue = rValue.getReferencedObject();

                            logger.info("    Key:   " + rKey.getIdentifier() + ": " + iKey.getAttributeValue("Number").stringValue());
                            logger.info("    Value: " + rValue.getIdentifier() + ": " + iValue.getAttributeValue("Name").stringValue());
                        }


                        logger.info("--------------------------------");
                        int i = 1;
                        logger.info("Key Order (Unordered):");
                        for (String key : keys) {
                            logger.info("   " + i + ".  " + key);
                            i++;
                        }
                        logger.info("--------------------------------");

                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                    break;
                }



                logger.info("Calling tx.complete()...");


                // Complete and close the transaction
                tx.complete();
                tx.close();

                logger.info("Back from tx.close()");

                transactionSuccessful = true;

            } catch (LockConflictException lce) {
                logger.info("LockConflictException. Attempting retry...  retryCount = " + ++transLCERetryCount);
                try {
                    Thread.sleep(10 * transLCERetryCount);
                } catch (InterruptedException ie) {
                }

            } catch (Exception ex) {
                logger.error("Error: ", ex);
                ex.printStackTrace();
                throw ex;
            }
        }

        logger.info("readEntries() - End...");




    }



    private void matchQuery(String doQuery) {

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

                    Variable vProjection = it.next();

                    //logger.info("vProjection is " + vProjection.getSpecification().getLogicalType().toString());

                    Instance iWalk = vProjection.instanceValue();
                    displayInstance(iWalk);


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

        if (ix.getIdentifier() != null) {
            sb.append(String.format("        %-15s:    %-15s\n", "OID", ix.getIdentifier().toString()));
            sb.append(String.format("        %-15s:    %-15s\n", "Classname", ix.getClass(true).getName()));
            sb.append("        - - - - - - - - - - - - - - - - - - - - - - - - - - -\n");
        }
        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getLogicalType();

            switch (lt) {
                case STRING:
                    sb.append(String.format("        %-15s:    %-15s    \n", at.getName(), v.stringValue()));
                    break;
                case REFERENCE:
                    sb.append(String.format("        %-15s:    %-15s    \n",  at.getName(), v.referenceValue().getIdentifier().toString()));
                    break;
                case INSTANCE:
                    sb.append(String.format("        %-15s:    %-15s    \n",  at.getName(), v.instanceValue().getIdentifier().toString()));
                    break;
                case WALK:
                    sb.append("==============================================\n");
                    sb.append("WALK...\n");
                    processWalk(v, sb);
                    break;
//                default:
//                    sb.append(String.format("%s : %-15s    ", at.getName(), "Not Handled"));
            }
        }

        print(sb.toString());
    }


    private void processWalk(Variable vWalk, StringBuilder sb) {

        Walk walk = vWalk.walkValue();

        Instance iFrom = null;
        Instance iTo = null;

        Iterator<Variable> itEdges = walk.edges().iterator();
        while (itEdges.hasNext()) {

            Edge edge = itEdges.next().edgeValue();

            print("  Edge:");
            print("      Edge ClassType: " + edge.edgeData().getClass(true).getName());

            if (iFrom == null) {
                iFrom = edge.from();
                print("    --------------------------------------------------");
                print("    Node:");
                displayInstance(iFrom);
            }
            iTo = edge.to();
            print("    --------------------------------------------------");
            print("    Node:");
            displayInstance(iTo);

//            logger.info("Got Edge: "
//                    + " FROM: " + edge.from().getIdentifier().toString()
//                    + "  TO: " + edge.to().getIdentifier().toString());

        }
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
        new Lab06MapsB();
    }
}
