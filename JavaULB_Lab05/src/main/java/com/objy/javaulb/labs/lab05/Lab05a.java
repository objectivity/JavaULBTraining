package com.objy.javaulb.labs.lab04;

import com.objy.data.Attribute;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Reference;
import com.objy.data.Variable;
import com.objy.data.dataSpecificationBuilder.ListSpecificationBuilder;
import com.objy.data.dataSpecificationBuilder.ReferenceSpecificationBuilder;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import com.objy.expression.language.Language;
import com.objy.expression.language.LanguageRegistry;
import com.objy.javaulb.labs.addresses.Address;
import com.objy.javaulb.labs.addresses.AddressFactory;
import com.objy.javaulb.labs.names.Name;
import com.objy.javaulb.labs.names.NameFactory;
import com.objy.statement.Statement;
import java.io.File;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab04d {

    private static Logger logger = LoggerFactory.getLogger(Lab04d.class);

    private static final boolean SIGNED = true;
    private static final boolean UNSIGNED = false;

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;


    private NameFactory nameFactory;
    private AddressFactory addressFactory;

    public Lab04d() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();



            nameFactory = new NameFactory(
                    System.getProperty("FEMALE_NAME_FILE"),
                    System.getProperty("MALE_NAME_FILE"),
                    System.getProperty("LAST_NAME_FILE"));
            
            addressFactory = new AddressFactory(System.getProperty("ADDRESSES_FILE"));



            openConnection(bootFile);

            createSchema();


            int count = 1000;
            createData(count);

            String doQuery1 = "FROM Person "
                    + "WHERE LastName =~ 'M.*' "
                    + "AND FirstName =~ 'T.*' "
                    + "RETURN LastName, "
                    + "LivesAt.City as City, LivesAt.State as State";
            query(doQuery1);

            // Because Person contains a reference to Address called "LivesAt"
            // we can create a projection that includes attributes from both
            // Person and Address. Here, we are traversing the LivesAt relationship
            // to pick up the LivesAt.City attribute but naming it "City" in 
            // our projection.
            String doQuery2 = "FROM Person "
                    + "WHERE LastName =~ 'M.*' "
                    + "AND FirstName =~ 'T.*' "
                    + "RETURN LastName, FirstName, "
                    + "LivesAt.City as City, LivesAt.State as State";
            query(doQuery2);

            String doQuery3 = "FROM Person "
                    + "WHERE LastName =~ 'M.*' "
                    + "AND FirstName =~ 'T.*' "
                    + "RETURN $$ID as oid, "
                    + "LastName, FirstName, "
                    + "LivesAt.City as City, LivesAt.State as State";
            query(doQuery3);
            
            String doQuery4 = "FROM Person "
                    + "WHERE LivesAt.City =~ '^Sp.*' "
                    + "RETURN $$ID as oid, "
                    + "LastName, FirstName, "
                    + "LivesAt.City as City, LivesAt.State as State";
            query(doQuery4);


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
                
                // Create the "LivesHere" end of the bidirectional to-many reference.
//                cBuilder.addAttribute("LivesAt",
//                            new ListSpecificationBuilder()
//                                .setElementSpecification(
//                                new ReferenceSpecificationBuilder()
//                                        .setReferencedClass("Address")
//                                        .setInverseAttribute("LivesHere")
//                                        .build())
//                                    .build());
                
                cBuilder.addAttribute("LivesAt",
                            new ReferenceSpecificationBuilder()
                                        .setReferencedClass("Address")
                                        .setInverseAttribute("LivesHere")
                                        .build());

                // Actually build the the schema representation.
                com.objy.data.Class cPerson = cBuilder.build();

                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cPerson);
                
                
                
                // Use ClassBuilder to create the schema definition.
                cBuilder = new com.objy.data.ClassBuilder("Address");
                cBuilder.addAttribute(LogicalType.STRING, "Street1");
                cBuilder.addAttribute(LogicalType.STRING, "Street2");
                cBuilder.addAttribute(LogicalType.STRING, "City");
                cBuilder.addAttribute(LogicalType.STRING, "State");
                cBuilder.addAttribute(LogicalType.STRING, "ZIP");
                
                // Create the "LivesHere" end of the bidirectional to-many reference.
                cBuilder.addAttribute("LivesHere",
                            new ListSpecificationBuilder()
                                .setElementSpecification(
                                new ReferenceSpecificationBuilder()
                                        .setReferencedClass("Person")
                                        .setInverseAttribute("LivesAt")
                                        .build())
                                    .build());

                // Actually build the the schema representation.
                com.objy.data.Class cAddress = cBuilder.build();

                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cAddress);

                
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
                    com.objy.data.List livesHere = vLivesHere.listValue();
                    Variable vRef = new Variable(new Reference(iPerson));
                    livesHere.add(vRef);
                    

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

            LogicalType lt = at.getAttributeValueSpecification().getFacet().getLogicalType();

            switch (lt) {
                case STRING:
                    sb.append(String.format("%-15s    ", v.stringValue()));
                    break;
                case REFERENCE:
                    sb.append(String.format("%-15s    ", v.referenceValue().getObjectId().toString()));
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
        new Lab04d();
    }
}
