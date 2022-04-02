package com.objy.javaulb.labs.lab02;

import com.objy.data.schemaProvider.SchemaProvider;
import com.objy.data.Encoding;
import com.objy.data.dataSpecificationBuilder.*;

import com.objy.data.LogicalType;
import com.objy.data.Storage;
import com.objy.db.Connection;
import com.objy.db.LockConflictException;
import com.objy.db.TransactionMode;
import com.objy.db.TransactionScope;
import java.io.File;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab02cNumbers {

    private static Logger logger = LoggerFactory.getLogger(Lab02cNumbers.class);

    // The System.getProperties() value from which various things will be read.
    private Properties properties;

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;

    // The connection to the ThingSpan federation.
    private Connection connection;





    public Lab02cNumbers() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            validateProperties();

            openConnection(bootFile);

            createSchema();

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



    /**
     * 
     */
    private void createSchema() {

        int transLCERetryCount = 0;
	boolean transactionSuccessful = false;
	while (!transactionSuccessful) {
            // Create a new TransactionScope that is READ_UPDATE.
            try (TransactionScope tx = new TransactionScope(TransactionMode.READ_UPDATE)) {
                
                // Ensure that our view of the schema is up to date.
                SchemaProvider.getDefaultPersistentProvider().refresh(true);

                // Use ClassBuilder to create the schema definition.
                com.objy.data.ClassBuilder cBuilder = new com.objy.data.ClassBuilder("NumbersDemo");
                
                
                cBuilder.addAttribute("SimpleInteger", LogicalType.INTEGER);
                
                
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
                
                
                // Process the schema changes.
                SchemaProvider.getDefaultPersistentProvider().activateEdits();
                
                logger.info("Address class created in schema.");
                
                // Complete and close the transaction
                tx.complete();
                tx.close();
                
                logger.info("NumbersDemo class created in schema.");

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
        new Lab02cNumbers();
    }
}
