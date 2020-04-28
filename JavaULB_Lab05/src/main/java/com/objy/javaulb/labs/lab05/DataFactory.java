/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.objy.javaulb.labs.lab05;

import com.objy.data.Instance;
import com.objy.data.Reference;
import com.objy.data.Variable;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import com.objy.javaulb.utils.addresses.Address;
import com.objy.javaulb.utils.addresses.AddressFactory;
import com.objy.javaulb.utils.names.Name;
import com.objy.javaulb.utils.names.NameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class DataFactory {
    private static Logger logger = LoggerFactory.getLogger(DataFactory.class);
    
    private com.objy.data.Class cPerson;
    private com.objy.data.Class cAddress;
    private com.objy.data.Class cLivesAtEdge;
    
    
    private NameFactory nameFactory;
    private AddressFactory addressFactory;
    
    
    
    public DataFactory() {
        
    }
    
    
    public String createData(int count) throws Exception {
        
        nameFactory = new NameFactory();
        addressFactory = new AddressFactory();

        String oid = null;

        int transLCERetryCount = 0;
        boolean transactionSuccessful = false;
        while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {

                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Lookup the various classes from the schema in the ThingSpan federation.
                cPerson = com.objy.data.Class.lookupClass("Person");
                cAddress = com.objy.data.Class.lookupClass("Address");
                cLivesAtEdge = com.objy.data.Class.lookupClass("LivesAtEdge");


                // Create some addresses with people living at them.
                for (int i = 0; i < count; i++) {

                    Address address = addressFactory.getAddress();
                    Instance iAddress = creatAddressInstance(address);
                    
                    logger.info("-----------------------------------------------");
                    logger.info("Address: " + " :: " + iAddress.getObjectId().toString());

                    int pCount = (i == 0)? 4 : (int)(Math.random() * 5);

                    Name name;
                    Instance iPerson;

                    for (int p = 0; p < pCount; p++) {

                        if (i == 0 && p == 0) {
                            // Create a known Person vertex to aid in query demonstration.
                            name = new Name("Male", "John", "Alfred", "Doe");
                        } else {
                            name = nameFactory.createName();
                        }

                        iPerson = createPersonInstance(name);
                        
                        logger.info("Person: " + name.last + ", " + name.first + " :: " + iPerson.getObjectId().toString());

                        // We only have to set one end of the relationship.
                        // The other end is set automatically based on the schema
                        // definition.
                        Instance iLivesEdge = Instance.createPersistent(cLivesAtEdge);
                        
                        Variable vLEPerson = iLivesEdge.getAttributeValue("ToPerson");
                        vLEPerson.set(new Reference(iPerson));

                        Variable vLEAddress = iLivesEdge.getAttributeValue("ToAddress");
                        vLEAddress.set(new Reference(iAddress));
                    }

                    if ((i%100) == 0) {
                        logger.info("Address created: " + i);
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

    
    private Instance creatAddressInstance(Address address) {

        Instance iAddress = Instance.createPersistent(cAddress);

        Variable vStreet1 = iAddress.getAttributeValue("Street");
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

        return iAddress;
    }

    
    private Instance createPersonInstance(Name name) {

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

        return iPerson;
    }

    
}
