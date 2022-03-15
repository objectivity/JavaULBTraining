package com.objy.javaulb.labs.lab05;

import com.objy.data.Attribute;
import com.objy.data.DataSpecification;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.ListFacet;
import com.objy.data.LogicalType;
import com.objy.data.Variable;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.SessionLogging;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import com.objy.javaulb.utils.LabUtils;
import com.objy.statement.Statement;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab05b {
    private static Logger logger = LoggerFactory.getLogger(Lab05b.class);


    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;









    public Lab05b() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            bootFile = LabUtils.validateBootfile();            

            String sessionLogDir = System.getProperty("SESSION_LOG_DIR");
            if (sessionLogDir != null) {
                SessionLogging.setLoggingOptions(SessionLogging.LogAll, sessionLogDir);
            }

            LabUtils.openConnection(bootFile);            
                      
            DataFactory df = new DataFactory();            
            df.createData(20);            
            

            String doQuery1 = "MATCH path = (:Person) --> (:Address) RETURN path";
            query(doQuery1);
            
            String doQuery2 = "MATCH path = (:Person {LastName =~~ '^S.*'}) --> (:Address) RETURN path";
            query(doQuery2);
    
            String doQuery3 = "MATCH path = (:Person {FirstName == 'John' "
                                + "&& LastName == 'Doe'}) "
                                + "-->(:Address)-->(:Person) return path;";
            query(doQuery3);
            
            LabUtils.closeConnection();

        } catch (Exception ex) {
            logger.error("Error: ", ex);
            return;
        }

    }

    

    
    
    private void query(String doQuery) {

        print("");
        print("");
        print("========================================================");
        print("QUERY: " + doQuery);
        print("--------------------------------------------------------");

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                boolean headerPrinted = false;
               
                Statement statement = new Statement("DO", doQuery);
                Variable vStatementExecute = statement.execute();

                java.util.Iterator<Variable> it = vStatementExecute.sequenceValue().iterator();
                if (!it.hasNext()) {
                    logger.info("There were no results on query:\n\n" + doQuery);
                }

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


        try {
            String oid = ix.getIdentifier().toString();
            sb.append(String.format("        %12s: %s\n", "OID", oid));
            sb.append(String.format("        %12s: %s\n", "Type", ix.getClass(true).getName()));            
        } catch(NullPointerException npe) {
        }
        
        
        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            LogicalType lt = at.getAttributeValueSpecification().getLogicalType();
            
            sb.append(String.format("        %12s: ", at.getName()));
            switch (lt) {
                case STRING:
                    sb.append(String.format("%s", v.stringValue()));
                    break;
                case REAL:
                    sb.append(String.format("%10.5f", v.floatValue()));
                    break;
                case REFERENCE:
                    sb.append(String.format("%s", v.referenceValue().getIdentifier().toString()));
                    break;
                case LIST:
                    DataSpecification ds = v.getSpecification().collectionFacet().getElementSpecification();
                    
                    logger.info("ds.getFacet().getLogicalType().toString() " + ds.getFacet().getLogicalType().toString());
                    LogicalType listOfType = ds.getFacet().getLogicalType();
//                    if (listOfType == LogicalType.REFERENCE) {
//                        sb.append("LIST of " + listOfType.toString());
//
//                        ds.getFacet().
//
//                    } else {
//                        sb.append("LIST of "
//                                + listOfType.toString() );
//                    }
                    break;
                case WALK:
                    // The current attribute of the projection instance is a WALK. 
                    // Extract this attrbute and process it as a walk.
                    com.objy.data.Walk walk = ix.getAttributeValue(at.getName()).walkValue();
                    processWalk(walk);
                    break;
                default:
                    sb.append(String.format("[Not Handled: %s]", lt.toString()));
            }
            sb.append("\n");
        }

        print(sb.toString());
    }
    
    
    
    
    /**
     * A Walk consists of a sequence of com.objy.data.Edge objects. Each Edge 
     * object looks like:
    *   _Projection
    *       {
    *       p:WALK
    *       {
    *           EDGE
    *           {
    *               from:3-3-1-40,              // OID of from object
    *               fromClass:'Person',         // Class of from object
    *               attribute:'LivesAt',        // Name of attribute in from object that points to edge.
    *               edgeData:3-3-1-44,          // OID of edge object with edge data
    *               edgeDataClass:'LivesAtEdge',// Class of edge data object
    *               to:3-3-1-37,                // OID of to object
    *               toClass:'Address'           // Class of to object
    *           }
    *       }
    *   }
     * 
     * 
     * 
     * 
     * 
     * 
     * @param walk 
     */
    private void processWalk(com.objy.data.Walk walk) {

        boolean showFromObject = true; // Only show the from object the first time.


        System.out.println("=================================================");

        System.out.println("WALK:");

        for (Variable v : walk.edges()) {
            com.objy.data.Edge edge = v.edgeValue();

            if (showFromObject) {
                showFromObject = false;

                Instance iFrom = edge.from();

                System.out.println("-------------------------------------------------");
                System.out.println("Node:");

                displayInstance(iFrom);
                System.out.println();
            }

            System.out.println("Edge:");
            Instance iEdgeData = edge.edgeData();
            displayInstance(iEdgeData);
            System.out.println();           


            System.out.println("Node:");
            Instance iTo = edge.to();
            displayInstance(iTo);
            System.out.println();

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
        new Lab05b();
    }
}
