package com.objy.javaulb.labs.lab03;

import com.objy.data.Attribute;
import com.objy.data.Encoding;
import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Storage;
import com.objy.data.Variable;
import com.objy.data.dataSpecificationBuilder.IntegerSpecificationBuilder;
import com.objy.data.dataSpecificationBuilder.RealSpecificationBuilder;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab03c {

    private static Logger logger = LoggerFactory.getLogger(Lab03c.class);
    
    private static final boolean SIGNED = true;
    private static final boolean UNSIGNED = false;
    

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;





    public Lab03c() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            openConnection(bootFile);

            createNumberDemoSchema();
                        
            createNumberDemoInstances();

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
    
    
    
    
    private void createNumberDemoInstances() {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {
                
                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);
                
                // Lookup the Person class from the schema in the ThingSpan federation.
                com.objy.data.Class cND = com.objy.data.Class.lookupClass("NumbersDemo");
                
                // Using the cPerson Class object, create a Person Instance.
                Instance iND = Instance.createPersistent(cND);
                
                logger.info("iND OID: " + iND.getObjectId().toString());
                
                // We access the value of each attribute in the Instance using
                // a variable that we 'associate' with each attribute.
                Variable var;
                
                String sB8 = "11000000";
                long intB8 = parseBinary(sB8, SIGNED);
                logger.info("sB8:  <" + sB8 + ">[" + sB8.length() + "]  intB8 = " + intB8);
                var = iND.getAttributeValue("MyIntB8_Signed");
                var.set((Long)intB8);
                
                String sB16 = "0100000000000000";                
                long intB16 = parseBinary(sB16, SIGNED);
                logger.info("sB16:  <" + sB16 + ">[" + sB16.length() + "]  intB16 = " + intB16);
                var = iND.getAttributeValue("MyIntB16_Signed");
                var.set((Long)intB16);
                
                String sB32 = "01000000000000000000000000000000";                
                long intB32 = parseBinary(sB32, SIGNED);
                logger.info("sB32:  <" + sB32 + ">[" + sB32.length() + "]  intB32 = " + intB32);
                var = iND.getAttributeValue("MyIntB32_Signed");
                var.set((Long)intB32);
                
                String sB64 = "0100000000000000000000000000000000000000000000000000000000000000";                
                long intB64 = parseBinary(sB64, SIGNED);
                logger.info("sB64:  <" + sB64 + ">[" + sB64.length() + "]  intB64 = " + intB64);
                var = iND.getAttributeValue("MyIntB64_Signed");
                var.set((Long)intB64);
                
                
              
                
                
                


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
    
    private long parseBinary(String s, boolean signed) {
        
        long ix = 0;
        
        int start = 0;
        if (signed) {
            start = 1;
        }
        
        for (int i = start; i < s.length(); i++) {
            int c = s.charAt(i);
            ix <<= 1;
            ix += c - '0';
        }
        if (signed && s.charAt(0) == '1') {
            ix *= -1;
        }
        
        return ix;        
    }
    
 
    
    private void createNumberDemoSchema() {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {
                
                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Use ClassBuilder to create the schema definition.
                com.objy.data.ClassBuilder cBuilder = new com.objy.data.ClassBuilder("NumbersDemo");
                
                
                cBuilder.addAttribute(LogicalType.INTEGER, "SimpleInteger");
                
                
                cBuilder.addAttribute("MyIntB8_Signed", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B8)
                                            .setEncoding(Encoding.Integer.SIGNED)
                                            .build());
                cBuilder.addAttribute("MyIntB16_Signed", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B16)
                                            .setEncoding(Encoding.Integer.SIGNED)
                                            .build());
                cBuilder.addAttribute("MyIntB32_Signed", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B32)
                                            .setEncoding(Encoding.Integer.SIGNED)
                                            .build());
                cBuilder.addAttribute("MyInt64_Signed", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B64)
                                            .setEncoding(Encoding.Integer.SIGNED)
                                            .build());
                
                
                
                cBuilder.addAttribute("MyIntB8_Unsigned", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B8)
                                            .setEncoding(Encoding.Integer.UNSIGNED)
                                            .build());
                cBuilder.addAttribute("MyIntB16_Unsigned", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B16)
                                            .setEncoding(Encoding.Integer.UNSIGNED)
                                            .build());
                cBuilder.addAttribute("MyIntB32_Unsigned", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B32)
                                            .setEncoding(Encoding.Integer.UNSIGNED)
                                            .build());
                cBuilder.addAttribute("MyInt64_Unsigned", 
                                      new IntegerSpecificationBuilder(Storage.Integer.B64)
                                            .setEncoding(Encoding.Integer.UNSIGNED)
                                            .build());
                
                
                
                cBuilder.addAttribute(LogicalType.REAL, "SimpleReal");
                
                cBuilder.addAttribute("MyReal32_IEEE", 
                                      new RealSpecificationBuilder(Storage.Real.B32)
                                            .setEncoding(Encoding.Real.IEEE)
                                            .build());
                
                cBuilder.addAttribute("MyReal64_IEEE", 
                                      new RealSpecificationBuilder(Storage.Real.B64)
                                            .setEncoding(Encoding.Real.IEEE)
                                            .build());
                
                
                com.objy.data.Class cNumbersDemo = cBuilder.build();
                
                SchemaProvider.getDefaultPersistentProvider().represent(cNumbersDemo);
                
                
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


    
    
    
    
    


    public static void main(String[] args) {
        new Lab03c();
    }
}
