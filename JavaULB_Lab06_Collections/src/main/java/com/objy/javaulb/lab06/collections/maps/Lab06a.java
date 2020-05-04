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
import com.objy.expression.language.Language;
import com.objy.expression.language.LanguageRegistry;
import com.objy.statement.Statement;
import java.io.File;
import java.util.Iterator;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab06a {

    private static Logger logger = LoggerFactory.getLogger(Lab06a.class);


    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;


    public Lab06a() {

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

        logger.info("createSchema() - Begin...");

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                Language doLang = LanguageRegistry.lookupLanguage("DO");
                Statement statement;

                try {

                    String doQuery
                            = "UPDATE SCHEMA {\n"
                            + "    CREATE CLASS KVPair {\n"
                            + "         Key : String,\n"
                            + "         Value : String\n"
                            + "     }\n"
                            + "    CREATE CLASS MyType {\n"
                            + "         MyName : String,\n"
                            + "         MyMap  : Map {\n"
                            + "                     CollectionTypeName :NameToReferenceMap,\n"
                            + "                     Key :String { Encoding: BYTE },\n"
                            + "                     Element : Reference { Referenced: KVPair }\n"
                            + "                 }\n"
                            + "     }\n"
                            + "}";

                    logger.info("doQuery: <" + doQuery + ">");
                    statement = new Statement(doLang, doQuery);

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




    /**
     * A simple key-value class.
     */
    class KVData {
        public String key;
        public String value;

        public KVData(String k, String v) {
            this.key = k;
            this.value = v;
        }
    }





    /**
     * This method creates an instance of MyType which contains a com.objy.data.Map
     * attribute and then puts some things in the map.
     */
    private void addEntries() {

        logger.info("addEntries() - Begin...");

        KVData[] kvData = {
            new KVData("AKey", "AValue"),
            new KVData("BKey", "BValue"),
            new KVData("CKey", "CValue"),
            new KVData("DKey", "DValue")
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

                
                com.objy.data.Class cKVPair = com.objy.data.Class.lookupClass("KVPair");

                for (KVData kvd : kvData) {
                    // Create the target KVPair object.
                    Instance iKV = Instance.createPersistent(cKVPair);

                    // Set the attributes on the new KVPair object.
                    iKV.getAttributeValue("Key").set(kvd.key);
                    iKV.getAttributeValue("Value").set(kvd.value);

                    // We need a Variable to represent key String value.
                    Variable vKey = new Variable(kvd.key);
                    
                    // We need an empty Variable that will hold the Reference
                    // to the iKV object.
                    Variable vValue = new Variable();

                    // Wrap iKV in a Reference and put that into vValue.
                    vValue.set(new com.objy.data.Reference(iKV));

                    // Put the vKey and vValue into the map.
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

        logger.info("readEntries() - Begin...");

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                Language doLang = LanguageRegistry.lookupLanguage("DO");
                Statement statement;

                try {
                    String doQuery = "FROM MyType return *";

                    logger.info("doQuery: <" + doQuery + ">");
                    statement = new Statement(doLang, doQuery);

                    Iterator<Variable> it = statement.execute().sequenceValue().iterator();

                    // Loop over the MyType objects in the results.
                    while (it.hasNext()) {

                        Variable v = it.next();

                        Instance iMT = v.instanceValue();
                        
                        logger.info("MyType OID = " + iMT.getObjectId().toString());

                        // Get the MyMap attribute from the current MyType object.
                        Variable vMTMyMap = iMT.getAttributeValue("MyMap");

                        // Convert the Variable to Map.
                        com.objy.data.Map map = vMTMyMap.mapValue();

                        // Iteratr over the keys in the map.
                        Iterator<Variable> itKeys = map.keys().iterator();
                        while (itKeys.hasNext()) {
                            Variable vKey = itKeys.next();

                            // Get the String value for the current key.
                            String sKey = vKey.stringValue();

                            // Within the map, the value is a Reference.
                            // Get the referenced object which is of type KVPair.
                            Instance iValue = map.get(vKey).referenceValue().getReferencedObject();

                            // Get the Value attribute from the KVPair object.
                            String sValue = iValue.getAttributeValue("Value").stringValue();

                            logger.info("MyType.MyMap: sKey <" + sKey + ">   sValue <" + sValue + ">");
                        }
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


                Language doLang = LanguageRegistry.lookupLanguage("DO");

                Variable vStatementExecute;

                Statement statement = new Statement(doLang, doQuery);

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

        if (ix.getObjectId() != null) {
            sb.append(String.format("        %-15s:    %-15s\n", "OID", ix.getObjectId().toString()));
            sb.append(String.format("        %-15s:    %-15s\n", "Classname", ix.getClass(true).getName()));
            sb.append("        - - - - - - - - - - - - - - - - - - - - - - - - - - -\n");
        }
        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getFacet().getLogicalType();

            switch (lt) {
                case STRING:
                    sb.append(String.format("        %-15s:    %-15s    \n", at.getName(), v.stringValue()));
                    break;
                case REFERENCE:
                    sb.append(String.format("        %-15s:    %-15s    \n",  at.getName(), v.referenceValue().getObjectId().toString()));
                    break;
                case INSTANCE:
                    sb.append(String.format("        %-15s:    %-15s    \n",  at.getName(), v.instanceValue().getObjectId().toString()));
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
//                    + " FROM: " + edge.from().getObjectId().toString()
//                    + "  TO: " + edge.to().getObjectId().toString());

        }
    }

    private void displayHeader(Instance ix) {

        com.objy.data.Class cx = ix.getClass(true);

        StringBuilder sb = new StringBuilder();
        StringBuilder sbSeparator = new StringBuilder();

        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getFacet().getLogicalType();

            sb.append(String.format("%-15s    ", at.getName()));
            sbSeparator.append("---------------    ");
        }
        print(sb.toString());

        sb = new StringBuilder();

        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getFacet().getLogicalType();

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
        new Lab06a();
    }
}
