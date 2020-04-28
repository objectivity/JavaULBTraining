package com.objy.javaulb.labs.lab05;

import com.objy.db.SessionLogging;
import com.objy.javaulb.utils.LabUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniel
 */
public class Lab05a {

    private static Logger logger = LoggerFactory.getLogger(Lab05a.class);

    // The name (full path) of the ThingSpan bootfile.
    private String bootFile;




    public Lab05a() {

        logger.info("Running " + this.getClass().getSimpleName());

        try {
            bootFile = LabUtils.validateBootfile();  

            SessionLogging.setLoggingOptions(SessionLogging.LogAll, "D:/Root/temp");

            LabUtils.openConnection(bootFile);
            
            try {
                SchemaFactory.createSchema();
            }catch(Exception ex) {
                ex.printStackTrace();
                return;
            }

            LabUtils.closeConnection();

        } catch (Exception ex) {
            logger.error("Error: ", ex);
            return;
        }

    }

    


    public static void main(String[] args) {
        new Lab05a();
    }
}
