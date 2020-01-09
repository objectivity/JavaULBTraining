/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.objy.javaulb.utils;

import com.objy.data.Attribute;
import com.objy.data.Edge;
import com.objy.data.Instance;
import com.objy.data.LogicalType;
import com.objy.data.Variable;
import com.objy.data.Walk;
import java.util.Iterator;

/**
 *
 * @author Daniel
 */
public class InstanceFormatter {

    private InstanceFormatter() {

    }


    /**
     * This method is the primary entry point for formatting an Instance object.
     *
     * @param ix    The instance to be formatted.
     * @return The formatted text representation of the Instance object.
     */
    public static String format(Instance ix) {

        StringBuilder sb = new StringBuilder();

        return format(ix, sb);
    }




    /**
     * This method creates a String object that contains a formatted representation
     * of the Instance variable that was passed in. The resulting text is appended
     * to the StringBuilder parameter.
     *
     * @param ix    The instance to be formatted.
     * @param sb    The StringBuilder to which the formatted output is appended.
     * @return The formatted text representation of the Instance object.
     */
    public static String format(Instance ix, StringBuilder sb) {

        if (ix == null) System.err.println(" ix is null");
        if (sb == null) System.err.println(" sb is null");

        com.objy.data.Class cx = ix.getClass(true);
        if (cx == null) System.err.println(" cx is null");

        if (ix.getObjectId() != null) {
            sb.append(String.format("        %-15s:    %-15s\n", "OID", ix.getObjectId().toString()));
            sb.append(String.format("        %-15s:    %-15s\n", "Classname", ix.getClass(true).getName()));
            sb.append("        - - - - - - - - - - - - - - - - - - - - - - - - - - -\n");
        }
        for (int i = 0; i < cx.getNumberOfAttributes(); i++) {
            Attribute at = cx.getAttribute(i);
            Variable v = ix.getAttributeValue(at.getName());

            if (at.getAttributeValueSpecification().getFacet() == null) {
                sb.append(String.format("        %-15s:    [null facet - No Data]    \n", at.getName()));
                continue;
            }

//            if (at == null) System.err.println(" at is null");
//            if (v == null) System.err.println(" v is null");
//            System.out.println("at.getName() = " + at.getName());
//            if (at.getAttributeValueSpecification() == null) System.err.println(" at.getAttributeValueSpecification() is null");
//            if (at.getAttributeValueSpecification().getFacet() == null) System.err.println(" at.getAttributeValueSpecification().getFacet() is null");
//            if (at.getAttributeValueSpecification().getFacet().getLogicalType() == null) System.err.println(" at.getAttributeValueSpecification().getFacet().getLogicalType() is null");


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
                case LIST:
                    LogicalType atListOfLT = at.getAttributeValueSpecification().collectionFacet().getElementSpecification().getLogicalType();
//                    sb.append("LIST of " + atListOfLT + "\n");

                    sb.append(String.format("        %-15s:    [LIST of %s] \n                            {\n", at.getName(), atListOfLT));
                    switch(atListOfLT) {
                        case REFERENCE:
                            processListOfRefs(at, v, sb);
                            break;
                        default:
                            sb.append("                               Entries not shown.");
                    }
                    sb.append("\n                            }\n");

//                    processWalk(v, sb);
                    break;

                case WALK:
                    sb.append("==============================================\n");
                    sb.append("WALK...\n");
                    processWalk(v, sb);
                    break;
                default:
                    sb.append(String.format("%s : Type is %s      : %-15s    ",
                            at.getName(),
                            at.getAttributeValueSpecification().getLogicalType().toString(),
                            "Not Handled"));
            }
        }

        return sb.toString();
    }


    private static void processListOfRefs(Attribute at, Variable v, StringBuilder sb) {

        com.objy.data.List list = v.listValue();



        for (int i = 0; i < list.size(); i++) {
            sb.append(String.format("                               %s",
                    list.get(i).referenceValue().getObjectId().toString()));
            if (i != list.size()-1) {
                sb.append(",\n");
            }

        }


    }


    private static final String BREAK_BAR = "    --------------------------------------------------\n";
    private static final String NODE_BREAK_LABEL = BREAK_BAR + "    Node:\n";

    private static final String EDGE_BREAK_LABEL = BREAK_BAR + "    Edge:\n";


    /**
     * Walks the Walk... Sorry. This method process the nodes and edges in a walk,
     * calling format on each node and each edge.
     *
     * @param vWalk The Walk to be processed.
     * @param sb    The StringBuilder to which the formatted text representation
     * of the Walk is appended.
     */
    private static void processWalk(Variable vWalk, StringBuilder sb) {

        Walk walk = vWalk.walkValue();

        Instance iFrom = null;
        Instance iTo;

        int edgeCount = 0;
        Iterator<Variable> itEdges = walk.edges().iterator();
        while (itEdges.hasNext()) {

            Edge edge = itEdges.next().edgeValue();
            Instance iEdge = edge.edgeData();

            if (edgeCount++ == 0 && iFrom == null) {
                iFrom = edge.from();
                sb.append(NODE_BREAK_LABEL);
                format(iFrom, sb);
            }

            sb.append(EDGE_BREAK_LABEL)
                .append("      ClassName: ")
                .append(edge.edgeData().getClass(true).getName())
                .append("\n");

            format(iEdge, sb);


            iTo = edge.to();
            sb.append(NODE_BREAK_LABEL);
            format(iTo, sb);
        }
    }

}
