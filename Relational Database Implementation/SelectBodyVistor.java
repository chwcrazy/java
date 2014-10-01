package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.Datum.Type;

public class SelectBodyVistor implements SelectVisitor {

    HashMap<String, CreateTable> tables;
    File dataDir;
    final int block = 100000;
    // Datum[][] outPutArray;
    public ArrayList<ArrayList<Datum>> outPutArrList; // store the all tuples
    // which will be print
    // out
    public Column[] schema;
    ArrayList<Type> finalTypes;

    File readoutF;

	// public Operator oper;
	// SelectBody select;
    public SelectBodyVistor(File dataDir, HashMap<String, CreateTable> tables) {
        // this.select = select;
        this.dataDir = dataDir;
        this.tables = tables;
        outPutArrList = new ArrayList<ArrayList<Datum>>();
        schema = null;

    }

    @Override
    public void visit(PlainSelect pselect) {
		// TODO Auto-generated method stub
        // PlainSelect pselect = (PlainSelect) select;

        // is subjoin?
        List joinList = pselect.getJoins();

        int joinSize;
        if (joinList == null) {
            joinSize = 0;
        } else {
            joinSize = joinList.size();
        }
        FromScanner fromscan = new FromScanner(dataDir, tables, joinSize);
        pselect.getFromItem().accept(fromscan);

        Operator oper = fromscan.source;
        schema = fromscan.schema;
        ArrayList<Operator> operList = new ArrayList<Operator>();
        ArrayList<Column[]> schemaList = new ArrayList<Column[]>();
        Expression onCondition = null;
        operList.add(oper);
        schemaList.add(schema);
        for (int i = 0; i < joinSize; i++) {
            Join joinItem = (Join) joinList.get(i);
            FromScanner newFromscan = new FromScanner(dataDir, tables, joinSize);
            joinItem.getRightItem().accept(newFromscan);
            operList.add(newFromscan.source);
            schemaList.add(newFromscan.schema);

            onCondition = joinItem.getOnExpression();

        }

        //
        Main.totalOperList.addAll(operList);
        Main.totalSchemaList.addAll(schemaList);

        if (operList.size() > 1) {
            JoinOperator joinOper = new JoinOperator(operList, schemaList,
                    onCondition, pselect.getWhere());
            schema = joinOper.joinSchema;
            oper = joinOper;

        } else {
            if (pselect.getWhere() != null) {
                oper = new SelectionOperator(oper, schema, pselect.getWhere());

            }
        }

		// dump
        Datum[] row = oper.readOneTuple();
        // WRITE RESULT to disk before order
        BufferedWriter bufW = null;
        File beforeOrderf = null;
        if (Main.isSwap == 1) {
            if (Main.nowReadOutFile == null) {
                beforeOrderf = new File(Main.swapDir.toString()
                        + "/beforeOrder_final.dat");
            } else {
                String fileN = Main.nowReadOutFile.getName();
                beforeOrderf = new File(Main.swapDir.toString()
                        + fileN.split("\\.")[0] + "_1.dat");
            }
			// if(beforeOrderf.exists()){
            // String fileN = beforeOrderf.getName();
            // beforeOrderf = new File(Main.swapDir.toString()
            // + fileN.split("\\.")[0] + "_1.dat");
            // }
            try {
                bufW = new BufferedWriter(new FileWriter(beforeOrderf));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
		// order
        // orderby
        final List orderByList = pselect.getOrderByElements();
        ArrayList<ArrayList<Datum>> orderByDatumList = new ArrayList<ArrayList<Datum>>();

        List<SelectItem> selectItemList = pselect.getSelectItems();
		// hasAggregate(selectItemList);
        // group by (aggregate)
        List groupList = pselect.getGroupByColumnReferences();

        if (groupList != null
                || (groupList == null && hasAggregate(selectItemList))) {
            HashMap<String, ArrayList<Datum>> dataAfterGroup = new HashMap<String, ArrayList<Datum>>();

			// for averge functions,define hashmaps, groupby key reflect the sum
            // list and count list(every list contain the ave functions' sum or
            // count number)
            HashMap<String, ArrayList<Double>> sumHashMap = new HashMap<String, ArrayList<Double>>();
            HashMap<String, ArrayList<Integer>> countNumHashMap = new HashMap<String, ArrayList<Integer>>();
            HashMap<String, ArrayList<ArrayList<String>>> distinctHashMap = new HashMap<String, ArrayList<ArrayList<String>>>();
long begin_groupb = System.currentTimeMillis();
            while (row != null) {

                GroupByOperator grOper = new GroupByOperator(selectItemList,
                        row, groupList, schema, dataAfterGroup, sumHashMap,
                        countNumHashMap, distinctHashMap);
                grOper.groupOneTuple();
                dataAfterGroup = grOper.dataAfterGroup;
                sumHashMap = grOper.sumHashMap;
                countNumHashMap = grOper.countNumHashMap;
                distinctHashMap = grOper.distinctHashMap;
                row = oper.readOneTuple();
            }

            // get the datums of columns which are in group by statement.
            Set<String> keySet = dataAfterGroup.keySet();

            if (!keySet.isEmpty()) {

                Iterator<String> itr = keySet.iterator();
                int count = 0;
                while (itr.hasNext()) {

                    String str = itr.next();

                    Datum[] dataRow = GroupByOperator.groupStrToDatums(str); // group
                    // by
                    // key
                    // datum
                    // list
                    ArrayList<Datum> funcList = dataAfterGroup.get(str); // aggregate
                    // datum
                    // list
                    // Datum[] aggrDatas = new Datum[funcList.size()];
                    // for (int i = 0; i < funcList.size(); i++) {
                    // aggrDatas[i] = funcList.get(i);
                    // }
                    Datum[] grpTuple = new Datum[dataRow.length
                            + funcList.size()]; // tuple after group, only
                    // contain the groupby key
                    // column and the aggregate
                    // column
                    Column[] grpSchema = new Column[dataRow.length
                            + funcList.size()];
                    int dataRowLen = dataRow.length;
                    for (int m = 0; m < dataRowLen; m++) {
                        grpTuple[m] = dataRow[m];
                        grpSchema[m] = new Column(null, dataRow[m].colName);
                    }
                    for (int n = 0; n < funcList.size(); n++) {
                        grpTuple[dataRowLen + n] = funcList.get(n);
                        grpSchema[dataRowLen + n] = new Column(null,
                                funcList.get(n).colName);
                    }
                    // having condition

                    if (pselect.getHaving() != null) {

                        Evaluator havingEval = new Evaluator(grpSchema,
                                grpTuple);
                        pselect.getHaving().accept(havingEval);

                        if (!havingEval.getBool()) {
                            dataAfterGroup.remove(str);
                            continue;
                        }
                    }
                    schema = rerangeSchema(grpSchema, selectItemList);
                    ProjectOperator projOper = new ProjectOperator(grpSchema,
                            grpTuple, selectItemList);
                    projOper.projectOneTuple();
                    finalTypes = projOper.tupleTypeL;
                    // order

                    if (Main.isSwap == 0) {

                        if (orderByList != null) {
                            ArrayList<Datum> datumList = new ArrayList<Datum>();
                            datumList.addAll(projOper.outPutDatums);
                            // datumList.addAll(funcList);
                            orderByDatumList.add(datumList);
                        } else {
                            if (!projOper.outPutDatums.isEmpty()) {
                                ArrayList<Datum> outTupleList = new ArrayList<Datum>();
                                outTupleList.addAll(projOper.outPutDatums);
                                // outTupleList.addAll(funcList);

                                outPutArrList.add(outTupleList);
                            }
                        }
                    } else {
                        StringBuffer writeS = new StringBuffer();
                        ArrayList<Datum> dL = projOper.outPutDatums;
                        if (dL != null && dL.size() > 0) {
                            for (int i = 0; i < dL.size(); i++) {
                                writeS.append(dL.get(i).value);
                                writeS.append("|");
                            }
                            try {
                                bufW.write(writeS.toString());
                                bufW.newLine();
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }
                    count++;
                }
                long end_groupb = System.currentTimeMillis();
//                System.out.println("groupby  :::::::::::::" + (end_groupb- begin_groupb));
                try {
                    if (bufW != null) {

                        bufW.flush();
                        bufW.close();

						// External sort
                        ScanOperator scan_o = null;
                        if (oper instanceof SelectionOperator) {
                            scan_o = (ScanOperator) (((SelectionOperator) oper).input);
                        } else if (oper instanceof JoinOperator) {
                            scan_o = (ScanOperator) (((JoinOperator) oper).finalOpr);
                        }

						// ArrayList<ColumnDefinition> new_colPro =
                        // getFinalcolProperty();
                        readoutF = exSort(beforeOrderf, schema, orderByList,pselect.getLimit());
                        Main.nowReadOutFile = readoutF;

						// scan_o.input.close();
                        // oper = new ScanOperator(readoutF, new_colPro, null);
                        outPutArrList.clear();
						// outPutArrList = getoutPutArrList(oper,
                        // pselect.getLimit());

                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } else {
            // target
            int count = 0;
            while (row != null) {
                count++;
                ProjectOperator projOper = new ProjectOperator(schema, row,
                        selectItemList);
                projOper.projectOneTuple();
                finalTypes = projOper.tupleTypeL;
                if (Main.isSwap == 0) {

                    if (orderByList != null) {
                        orderByDatumList.add(projOper.outPutDatums);
                    } else {

                        if (!projOper.outPutDatums.isEmpty()) {

                            ArrayList<Datum> outTupleList = new ArrayList<Datum>();
                            outTupleList.addAll(projOper.outPutDatums);
                            outPutArrList.add(outTupleList);
                        }
                    }
                } else {

                    StringBuffer writeS = new StringBuffer();
                    ArrayList<Datum> dL = projOper.outPutDatums;
                    if (dL != null && dL.size() > 0) {
                        for (int i = 0; i < dL.size(); i++) {
                            writeS.append(dL.get(i).value);
                            writeS.append("|");
                        }
                        try {
                            bufW.write(writeS.toString());
                            bufW.newLine();
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }

                }
                row = oper.readOneTuple();
            }
            schema = getSelectedSchema(schema, selectItemList);
            try {
                if (bufW != null) {

                    bufW.flush();
                    bufW.close();

                    // external sort
                    ScanOperator scan_o = null;
                    if (oper instanceof SelectionOperator) {
                        scan_o = (ScanOperator) (((SelectionOperator) oper).input);
                    } else if (oper instanceof JoinOperator) {
                        scan_o = (ScanOperator) (((JoinOperator) oper).finalOpr);
                    }
					// ArrayList<ColumnDefinition> new_colPro =
                    // getFinalcolProperty();
                    readoutF = exSort(beforeOrderf, schema, orderByList,pselect.getLimit());
                    Main.nowReadOutFile = readoutF;
					// scan_o.input.close();
                    // oper = new ScanOperator(readoutF, new_colPro, null);
                    outPutArrList.clear();
					// outPutArrList = getoutPutArrList(oper,
                    // pselect.getLimit());
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

        // print data after order by.
        if (Main.isSwap == 0) {
            if (orderByList != null) {
                if (!orderByDatumList.isEmpty()) {
                    Datum[][] orderByArray = new Datum[orderByDatumList.size()][orderByDatumList
                            .get(0).size()];
                    for (int i = 0; i < orderByDatumList.size(); i++) {
                        ArrayList<Datum> oneTupleList = orderByDatumList.get(i);
                        Datum[] oneTupleArr = new Datum[oneTupleList.size()];
                        for (int j = 0; j < oneTupleList.size(); j++) {
                            oneTupleArr[j] = oneTupleList.get(j);
                        }
                        orderByArray[i] = oneTupleArr;
                    }

                    Arrays.sort(orderByArray,
                            publicUntils.generateCmptor(orderByList));

                    // limit
                    Limit lim = pselect.getLimit();
                    Long limRowNumber = null;
                    Datum[][] printDatumArr = null;
                    if (lim != null) {
                        limRowNumber = lim.getRowCount();
                        if (limRowNumber > orderByArray.length) {
                            printDatumArr = Arrays.copyOfRange(orderByArray, 0,
                                    orderByArray.length);
                        } else {
                            printDatumArr = Arrays.copyOfRange(orderByArray, 0,
                                    limRowNumber.intValue());
                        }
                    } else {
                        printDatumArr = orderByArray;
                    }

                    if (printDatumArr.length > 0) {

                        // System.out.print(printDatumArr[0].toString());
                        for (Datum[] rowArr : printDatumArr) {
                            if (rowArr == null) {
                                break;
                            }
                            ArrayList<Datum> oneTupleList = new ArrayList<Datum>();
                            for (int i = 0; i < rowArr.length; i++) {
                                Datum col = rowArr[i];
                                oneTupleList.add(col);
                            }

                            outPutArrList.add(oneTupleList);
                        }
                    }
                }
            } else {
                // orderby is null and isswap ==0;
                Limit lim = pselect.getLimit();
                Long limRowNumber = null;
                if (lim != null) {
                    limRowNumber = lim.getRowCount();
                    if (limRowNumber < outPutArrList.size()) {
                        for (int i = outPutArrList.size() - 1; i > limRowNumber - 1; i--) {
                            outPutArrList.remove(i);
                        }
                    }
                }
            }
        }

    }

    @Override
    public void visit(Union arg0) {
        // TODO Auto-generated method stub

    }

    public boolean hasAggregate(List<SelectItem> selectItemList) {
        for (SelectItem selItm : selectItemList) {
            String expStr = selItm.toString().toLowerCase();
            if ((expStr.contains("sum") && expStr.contains("("))
                    || (expStr.contains("avg(") && expStr.contains("("))
                    || (expStr.contains("count(") && expStr.contains("("))
                    || (expStr.contains("min(") && expStr.contains("("))) {
                return true;
            }
        }
        return false;

    }

    public Column[] getSchema() {
        return schema;
    }

    public Column[] getSelectedSchema(Column[] schema,
            List<SelectItem> selectItemList) {

        if (selectItemList.isEmpty()) {
            return new Column[0];
        }
        Column[] newSchema = new Column[selectItemList.size()];
        Table tab = schema[0].getTable();

        for (int m = 0; m < selectItemList.size(); m++) {
            SelectItem selItm = selectItemList.get(m);
            if (selItm instanceof AllTableColumns) {
                return schema;
            } else if (selItm instanceof SelectExpressionItem) {
                int index = 0;
                SelectExpressionItem sexpI = (SelectExpressionItem) selItm;
                String selColAlias = sexpI.getAlias();
                Expression exp = sexpI.getExpression();
                String expStr = exp.toString().toLowerCase();
				// if(selColAlias == null){
                // expStr = (tab.getName() + "." + expStr).toLowerCase();
                // }
                for (int n = 0; n < schema.length; n++) {
                    if (expStr.equals(schema[n].getColumnName().toLowerCase())) {
                        if (selColAlias == null) {
                            newSchema[m] = schema[n];
                        } else {
                            Column d_o = schema[n];
                            Column c_n = new Column();
                            c_n.setTable(d_o.getTable());
                            c_n.setColumnName(selColAlias);
                            newSchema[m] = c_n;
                        }
                        index = 1;
                        break;
                    }
                }
                if (index == 0) {
                    Table newTable = new Table();
					// String colTableName = null;
                    // if(tab.getAlias()!=null){
                    // colTableName = tab.getAlias();
                    // }else{
                    // colTableName = tab.getName();
                    // }

                    newTable.setAlias(tab.getAlias());

                    newTable.setName(tab.getName());
                    newTable.setSchemaName(tab.getSchemaName());
                    String newColName = null;
                    if (selColAlias == null) {
                        newColName = expStr;
                    } else {
                        newColName = selColAlias;
                    }
                    newSchema[m] = new Column(newTable, newColName);
                }

            }

        }

        return newSchema;
		// if (selectItemList.isEmpty()) {
        // return new Column[0];
        // }
        // Column[] newSchema = new Column[selectItemList.size()];
        // Table tab = schema[0].getTable();
        //
        // for (int m = 0; m < selectItemList.size(); m++) {
        // SelectItem selItm = selectItemList.get(m);
        // if (selItm instanceof AllTableColumns) {
        // return schema;
        // } else if (selItm instanceof SelectExpressionItem) {
        // // int index = 0;
        // SelectExpressionItem sexpI = (SelectExpressionItem) selItm;
        // String selColAlias = sexpI.getAlias();
        // Expression exp = sexpI.getExpression();
        // String expStr = exp.toString().toLowerCase();
        // // if(selColAlias == null){
        // // expStr = (tab.getName() + "." + expStr).toLowerCase();
        // // }
        // // for (int n = 0; n < schema.length; n++) {
        // // if (expStr.equals(schema[n].getColumnName().toLowerCase())) {
        // // newSchema[m] = schema[n];
        // // index = 1;
        // // break;
        // // }
        // // }
        // // if (index == 0) {
        // Table newTable = new Table();
        // if(exp instanceof Column){
        // tab = ((Column) exp).getTable();
        // }
        // newTable.setAlias(tab.getAlias());
        // String tabAlias;
        // if(tab.getAlias() == null){
        // tabAlias = tab.getName();
        // }else{
        // tabAlias = tab.getAlias();
        // }
        //
        // newTable.setName(tab.getName());
        // newTable.setSchemaName(tab.getSchemaName());
        // String newColName = null;
        // if(exp instanceof Column){
        // Column c = (Column)exp;
        // if (selColAlias == null) {
        // newColName = tabAlias + "." + c.getColumnName();
        // } else {
        // newColName = tabAlias + "." + selColAlias;
        // }
        // }else{
        //
        // if (selColAlias == null) {
        // newColName = tabAlias + "." + expStr;
        // } else {
        // newColName = tabAlias + "." + selColAlias;
        // }
        // }
        // newSchema[m] = new Column(newTable, newColName);
        // }
        //
        // // }
        //
        // }
        //
        // return newSchema;
    }

    public ArrayList<Datum> aggregateTuples(
            ArrayList<ArrayList<Datum>> totalTupleList,
            List<SelectItem> selectItemList, int count) {
        ArrayList<Datum> l = new ArrayList<>();

		// ArrayList<String> selectExpNameList = new ArrayList<>();
        for (int i = 0; i < selectItemList.size(); i++) {
            Expression exp = ((SelectExpressionItem) selectItemList.get(i))
                    .getExpression();
            // selectExpNameList.add(((Function)exp).getName());
            Datum getAggResult = getAggregateResult(exp.toString(),
                    totalTupleList, count, i);
            l.add(getAggResult);
        }
        return l;
    }

    public Datum getAggregateResult(String funName,
            ArrayList<ArrayList<Datum>> totalTuples, int count, int index) {

        String colName = null;
        Type type = null;
        Datum data = null;
        Datum d;

        if (funName.contains("min") && funName.contains("(")) {
            String min = null;
            for (int i = 0; i < totalTuples.size(); i++) {
                ArrayList<Datum> tuple = totalTuples.get(i);
                d = tuple.get(index);
                if (i == 0) {
                    min = d.value;
                    colName = d.colName;
                    type = d.type;
                    continue;
                } else {
                    int comR = min.compareTo(d.value);
                    if (comR > 0) {
                        min = d.value;
                    }
                }
            }

            switch (type) {
                case INT:
                    data = new Datum.INT(colName, min);
                    break;
                case FLOAT:
                    data = new Datum.FLOAT(colName, min);
                    break;
                case LONG:
                    data = new Datum.LONG(colName, min);
                    break;
                case STRING:
                    data = new Datum.STRING(colName, min);
                    break;
                case DATE:
                    data = new Datum.DATE(colName, min);
                    break;
                default:
                    break;
            }
        }
        return data;
    }

    public ArrayList<ArrayList<Datum>> getoutPutArrList(Operator opr, Limit lim) {
        int limitN = 0;
        if (lim != null) {

            Long l = (lim.getRowCount());
            limitN = l.intValue();
        }
        ArrayList<ArrayList<Datum>> outPutArr = new ArrayList<>();
        int count = 1;
        Datum[] onetuple = opr.readOneTuple();

        while (onetuple != null) {
            if (limitN > 0 && count > limitN) {
                break;
            }
            ArrayList<Datum> lineList = new ArrayList<>();
            for (Datum d : onetuple) {
                lineList.add(d);
            }
            outPutArr.add(lineList);

            onetuple = opr.readOneTuple();
            count++;
        }
        return outPutArr;
    }

    public ArrayList<ColumnDefinition> getFinalcolProperty() {
        ArrayList<ColumnDefinition> new_colDefs = new ArrayList<>();
        for (int j = 0; j < schema.length; j++) {
            Column col = schema[j];
            String colName = col.getColumnName();
            String new_colN;
            Table tab = col.getTable();
            if (tab == null) {
                new_colN = colName;
            } else {

                String tabAlias = tab.getAlias();

                if (tab.getAlias() != null) {
                    if (colName.contains(".")) {
                        new_colN = tabAlias + "." + colName.split(".")[0];
                    } else {
                        new_colN = tabAlias + "." + colName;
                    }
                } else {
                    new_colN = colName;
                }
            }

            ColumnDefinition colD_n = new ColumnDefinition();
            ColDataType colDT = new ColDataType();
            colDT.setDataType(finalTypes.get(j).toString());
            colD_n.setColDataType(colDT);
            colD_n.setColumnName(new_colN);
            new_colDefs.add(colD_n);
        }

        return new_colDefs;
    }

    public Column[] rerangeSchema(Column[] grpSchema,
            List<SelectItem> selectItemList) {
        int schL = grpSchema.length;

        for (int i = 0; i < schL; i++) {
            SelectItem selI = selectItemList.get(i);
            if (selI instanceof SelectExpressionItem) {

                SelectExpressionItem selIexp = (SelectExpressionItem) selI;
                String selIAlias = selIexp.getAlias();
                String colN = selIexp.toString().toLowerCase();
                if (selIAlias != null) {
                    colN = selIAlias.toLowerCase();
                }
                for (int j = i; j < schL; j++) {
                    if (grpSchema[j].getColumnName().toLowerCase()
                            .contains(colN)) {
                        Column temp = grpSchema[j];
                        grpSchema[j] = grpSchema[i];
                        grpSchema[i] = temp;

                    }
                }

            } else if (selI instanceof SelectExpressionItem) {
                return grpSchema;
            }
        }
        return grpSchema;
    }

    public static Comparator<String> ordCmptor(final Vector<Integer> ind,
            final List ordByList) {
        Comparator<String> new_c = new Comparator<String>() {

            @Override
            public int compare(String a, String b) {
                // TODO Auto-generated method stub
                String[] spla, splb;
                spla = a.split("\\|");
                splb = b.split("\\|");
                String suba, subb;
                int ans = 0;
                for (int m = 0; m < ordByList.size(); m++) {
                    suba = spla[ind.get(m)];
                    subb = splb[ind.get(m)];
                    ans = cmpSingle(suba, subb,
                            (OrderByElement) ordByList.get(m));
                    if (ans != 0) {
                        return ans;
                    } else {
                        continue;
                    }
                }
                return 0;
            }
        };
        return new_c;
    }

    public File exSort(File input, Column[] schema, List orderByList, Limit lim)
            throws FileNotFoundException, IOException {
        long limN = Integer.MAX_VALUE;
        if(lim != null)
            limN = lim.getRowCount();
        String preDir = Main.swapDir.toString() + "\\";
        File ans = null;
        if (orderByList == null) {
            ans = input;
        } else {
            Vector ind = new Vector();
            OrderByElement ordE = null;
            for (int i = 0; i < orderByList.size(); i++) {
                ordE = (OrderByElement) orderByList.get(i);
                String ordName = ordE.toString().toLowerCase()
                        .replaceAll("asc", "").replaceAll("desc", "")
                        .replaceAll(" ", "");
                boolean found = false;
                for (int j = 0; j < schema.length; j++) {
                    if (schema[j].getColumnName().toLowerCase()
                            .contains(ordName)) {
                        ind.add(j);
                        if (found) {
                            System.out.println("Err in exSort: match schema");
                        }
                        found = true;
                    }
                }
                if (!found) {
                    System.out.println("Err in exSort: match schema");
                }
            }

            Comparator cmp = ordCmptor(ind, orderByList);
            BufferedReader a = new BufferedReader(new FileReader(input));
            int i = 0;
            int j = 0;
            String now;
            ArrayList<String> tmp = new ArrayList();
            while ((now = a.readLine()) != null) {
				// if (!now.contains("1")) {
                // continue;
                // }
                j++;
                tmp.add(now);
                if (j >= block) {
                    Collections.sort(tmp, cmp);
                    BufferedWriter b = new BufferedWriter(new FileWriter(preDir
                            + "test_0_" + String.valueOf(i)));
                    i++;
                    for (int k = 0; k < tmp.size() && k < limN; k++) {
                        b.write(tmp.get(k).toString());
                        b.newLine();
                    }
                    j = 0;
                    tmp.clear();
                    b.close();
                }
            }
            Collections.sort(tmp, cmp);
            if (j != 0) {
                BufferedWriter b = new BufferedWriter(new FileWriter(preDir
                        + "test_0_" + String.valueOf(i)));
                i++;
                for (int k = 0; k < tmp.size() && k < limN; k++) {
                    b.write(tmp.get(k).toString());
                    b.newLine();
                }
                j = 0;
                b.close();
            }
            tmp.clear();
            a.close();
            int layer = 0;
            while (i > 1) {
                // System.out.println("i : " + i);
                for (j = 0; 2 * j + 1 < i; j++) {
                    String in_1 = "test_" + String.valueOf(layer) + "_"
                            + String.valueOf(2 * j);
                    String in_2 = "test_" + String.valueOf(layer) + "_"
                            + String.valueOf(2 * j + 1);
                    BufferedReader in1 = new BufferedReader(new FileReader(
                            preDir + in_1));
                    BufferedReader in2 = new BufferedReader(new FileReader(
                            preDir + in_2));
                    BufferedWriter out = new BufferedWriter(new FileWriter(
                            preDir + "test_" + String.valueOf(layer + 1) + "_"
                            + String.valueOf(j)));
                    String f, ff;
                    f = in1.readLine();
                    ff = in2.readLine();
					// System.out.println(f);
                    // System.out.println(Integer.valueOf(now));
                    int cnt = 0;
                    while (true) {
                        if (cnt >= limN) {
                            break;
                        }
                        cnt++;
                        if (cmp.compare(f, ff) < 0) {
                            out.write(f);
                            out.newLine();
                            if ((f = in1.readLine()) != null) {

                                continue;
                            } else {
                                out.write(ff);
                                out.newLine();
                                break;
                            }
                        } else {
                            out.write(ff);
                            out.newLine();
                            if ((ff = in2.readLine()) != null) {
                                continue;
                            } else {
                                out.write(f);
                                out.newLine();
                                break;
                            }
                        }
                    }

                    while ((f = in1.readLine()) != null) {
                        if(cnt >= limN) break;
                        cnt++;
                        // System.out.println("t : " + t);
                        out.write(f);
                        out.newLine();
                    }
                    while ((ff = in2.readLine()) != null) {
                        if(cnt >= limN) break;
                        cnt++;
                        // System.out.println("tt : " + tt);
                        out.write(ff);
                        out.newLine();
                    }
                    in1.close();
                    in2.close();
                    out.close();
                }
                if ((i & 1) == 1) {
                    String inn = "test_" + String.valueOf(layer) + "_"
                            + String.valueOf(i - 1);
                    String outt = "test_" + String.valueOf(layer + 1) + "_"
                            + String.valueOf((i - 1) / 2);

                    FileChannel in = null;
                    FileChannel out = null;
                    FileInputStream inStream = null;
                    FileOutputStream outStream = null;
                    try {
                        inStream = new FileInputStream(preDir + inn);
                        outStream = new FileOutputStream(preDir + outt);
                        in = inStream.getChannel();
                        out = outStream.getChannel();
                        in.transferTo(0, in.size(), out);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        inStream.close();
                        in.close();
                        outStream.close();
                        out.close();
                    }
                    i = (i + 1) / 2;
                    layer++;
                } else {
                    i = i / 2;
                    layer++;
                }
            }
            ans = new File(preDir + "test_" + String.valueOf(layer) + "_0");
        }
        return ans;
    }

    public static int cmpSingle(String a, String b, OrderByElement E) {
        int ans = 0;
        if (a.matches("[0-9]+(.[0-9]+)?")) {
            if (Double.valueOf(a) < Double.valueOf(b)) {
                ans = -1;
            } else if (Double.valueOf(a).equals(Double.valueOf(b))) {
                ans = 0;
            } else {
                ans = 1;
            }
            if (E.isAsc()) {
                ans = -ans;
            }
        } else {
            ans = a.compareTo(b);
            if (E.isAsc()) {
                ans = -ans;
            }
        }
        return -ans;
    }

    public File getReadoutFile() {
        return this.readoutF;
    }

    public ArrayList<Type> getFinalTypes() {
        return this.finalTypes;
    }

}
