package com.objy.javaulb.lab06.collections.maps;

import com.objy.data.Attribute;
import com.objy.data.Edge;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Reference;
import com.objy.data.Variable;
import com.objy.data.Walk;
import com.objy.data.dataSpecificationBuilder.ListSpecificationBuilder;
import com.objy.data.dataSpecificationBuilder.ReferenceSpecificationBuilder;
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


            SessionLogging.setLoggingOptions(SessionLogging.LogAll, "D:/Root/temp");

            openConnection(bootFile);

            try {
                createSchema();
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
                            + "    CREATE CLASS KPPair {\n"
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





    private String createData(int count) {

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

                com.objy.data.Class cAddress = com.objy.data.Class.lookupClass("Address");
                com.objy.data.Class cLivesEdge = com.objy.data.Class.lookupClass("LivesEdge");


                for (int i = 0; i < 1000; i++) {
                    Name name = nameFactory.createName();

                    //logger.info("Name: " + name.first + " " + name.middle + " " + name.last);

                    // Using the cPerson Class object, create a Person Instance.
                    Instance iPerson = Instance.createPersistent(cPerson);

                    //logger.info("iPerson OID: " + iPerson.getObjectId().toString());

                    // We access the value of each attribute in the Instance using
                    // a variable that we 'associate' with each attribute.
                    Variable vFirstName = iPerson.getAttributeValue("FirstName");
                    vFirstName.set(name.first);

                    Variable vMiddleInitial = iPerson.getAttributeValue("MiddleName");
                    vMiddleInitial.set(name.middle);

                    Variable vLastName = iPerson.getAttributeValue("LastName");
                    vLastName.set(name.last);


                    Address address = addressFactory.getAddress();
                    Instance iAddress = Instance.createPersistent(cAddress);

                    Variable vStreet1 = iAddress.getAttributeValue("Street1");
                    vStreet1.set(address.number + " " + address.street);

                    Variable vCity = iAddress.getAttributeValue("City");
                    vCity.set(address.city);

                    Variable vState = iAddress.getAttributeValue("State");
                    vState.set(address.state);

                    Variable vZIP = iAddress.getAttributeValue("ZIP");
                    vZIP.set(address.zip);

                    Variable vLat = iAddress.getAttributeValue("Latitude");
                    vLat.set(address.latitude);

                    Variable vLon = iAddress.getAttributeValue("Longitude");
                    vLon.set(address.longitude);

                    // Remember, we only have to set one end of the relationship.
                    // The other end it set automatically based on the schema
                    // definition.
                    Variable vLivesHere = iAddress.getAttributeValue("LivesHere");
                    com.objy.data.List livesHereList = vLivesHere.listValue();

                    Instance iLivesEdge = Instance.createPersistent(cLivesEdge);
                    Variable vLEPerson = iLivesEdge.getAttributeValue("ToPerson");
                    vLEPerson.set(new Reference(iPerson));

                    Variable vLEAddress = iLivesEdge.getAttributeValue("ToAddress");
                    vLEAddress.set(new Reference(iAddress));



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
