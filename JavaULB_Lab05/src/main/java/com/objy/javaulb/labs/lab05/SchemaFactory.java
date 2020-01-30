/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.objy.javaulb.labs.lab05;

import com.objy.data.LogicalType;
import com.objy.data.dataSpecificationBuilder.ListSpecificationBuilder;
import com.objy.data.dataSpecificationBuilder.ReferenceSpecificationBuilder;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class SchemaFactory {

    private static Logger logger = LoggerFactory.getLogger(SchemaFactory.class);


    private SchemaFactory() {

    }

    public static void createSchema() {

        logger.info("createSchema() - Begin...");

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                com.objy.data.ClassBuilder cBuilder;


                //--------------------------------------------------------------
                logger.info("Creating Person...");

                // Use ClassBuilder to create the schema definition.
                cBuilder = new com.objy.data.ClassBuilder("Person");
                cBuilder.addAttribute(LogicalType.STRING, "FirstName");
                cBuilder.addAttribute(LogicalType.STRING, "LastName");
                cBuilder.addAttribute(LogicalType.STRING, "MiddleName");

                cBuilder.addAttribute("LivesAt",
                            new ListSpecificationBuilder()
                                .setCollectionName("SegmentedArray")
                                .setElementSpecification(
                                    new ReferenceSpecificationBuilder()
                                            .setEdgeClass("LivesEdge")
                                            .setEdgeAttribute("ToAddress")
                                            .build())
                                .build());

                // Actually build the the schema representation.
                com.objy.data.Class cPerson = cBuilder.build();

                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cPerson);



                //--------------------------------------------------------------
                logger.info("Creating Address...");

                // Use ClassBuilder to create the schema definition.
                cBuilder = new com.objy.data.ClassBuilder("Address");
                cBuilder.addAttribute(LogicalType.STRING, "Street1");
                cBuilder.addAttribute(LogicalType.STRING, "Street2");
                cBuilder.addAttribute(LogicalType.STRING, "City");
                cBuilder.addAttribute(LogicalType.STRING, "State");
                cBuilder.addAttribute(LogicalType.STRING, "ZIP");

                cBuilder.addAttribute("LivesHere",
                            new ListSpecificationBuilder()
                                .setCollectionName("SegmentedArray")
                                .setElementSpecification(
                                    new ReferenceSpecificationBuilder()
                                            .setEdgeClass("LivesEdge")
                                            .setEdgeAttribute("ToPerson")
                                            .build())
                                .build());

                // Actually build the the schema representation.
                com.objy.data.Class cAddress = cBuilder.build();

                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cAddress);



                //--------------------------------------------------------------
                logger.info("Creating LivesEdge...");

                // Use ClassBuilder to create the schema definition.
                cBuilder = new com.objy.data.ClassBuilder("LivesEdge");
                cBuilder.addAttribute(LogicalType.DATE, "BeginDate");
                cBuilder.addAttribute(LogicalType.DATE, "EndDate");

                // Create the "ToAddress" end of the bidirectional to-many reference.
                cBuilder.addAttribute("ToAddress",
                            new ReferenceSpecificationBuilder()
                                        .setReferencedClass("Address")
                                        .setInverseAttribute("LivesHere")
                                        .build());

                // Create the "ToPerson" end of the bidirectional to-many reference.
                cBuilder.addAttribute("ToPerson",
                            new ReferenceSpecificationBuilder()
                                        .setReferencedClass("Person")
                                        .setInverseAttribute("LivesAt")
                                        .build());

                // Actually build the the schema representation.
                com.objy.data.Class cLivesEdge = cBuilder.build();

                // Represent the new class into the federated database.
                SchemaProvider.getDefaultPersistentProvider().represent(cLivesEdge);


                SchemaProvider.getDefaultPersistentProvider().activateEdits();

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

        logger.info("createSchema() - Begin...");
    }


}
