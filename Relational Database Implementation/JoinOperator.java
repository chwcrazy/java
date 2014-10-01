package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.Datum.Type;

public class JoinOperator implements Operator {

    ArrayList<Operator> operList;
    ArrayList<Column[]> schemaList;
    Expression onCondition;
    Expression whereCondition;
    ArrayList<Expression> whereConlist = new ArrayList<Expression>(); // list of
    // all
    // expression
    // in
    // whereCondition
    ArrayList<Expression> joinConlist = new ArrayList<Expression>();
    // the list of select condition for each schema. if one schema does not have
    // select condition, the condition will be null
    ArrayList<Expression> selConList;
    Operator finalOpr;

    ArrayList<Datum[]> datumList;
    int index = 0;
    public Column[] joinSchema;

    ArrayList<ArrayList<Datum[]>> allDatasL;
    ArrayList<Datum[]> finalDataTable;
    int readTupInd = 0;

    ArrayList<Set<Integer>> condColNameL;
    Set<Integer> oneTableConColL;

    // String path;
    public JoinOperator(ArrayList<Operator> operList,
            ArrayList<Column[]> schemaList, Expression onCondition,
            Expression whereCondition) {
        this.operList = operList;
        this.schemaList = schemaList;
        this.onCondition = onCondition;
        this.whereCondition = whereCondition;
        if (Main.isSwap == 0) {
            allDatasL = new ArrayList<>();
        }
        condColNameL = new ArrayList<>();
        oneTableConColL = new HashSet<>();
        // joinSchema = getJoinSchema();
        datumList = new ArrayList<Datum[]>();
        // path = ((ScanOperator) operList.get(0)).f.getParent();
        getJoinConList(whereCondition); // get join condition expression list
        selConList = getSelConList();
        // long begin_sele = System.currentTimeMillis();
        oprAfterSel();
        // long end_sele = System.currentTimeMillis();
        // System.out.println("SELECT:::::::::::::" + (end_sele - begin_sele));
        // long begin_join = System.currentTimeMillis();
        normalJoinExe();
        // long end_join = System.currentTimeMillis();
        // System.out.println("join:::::::::::::" + (end_join - begin_join));
    }

    // replace the operators after selected
    public void oprAfterSel() {
        for (int i = 0; i < operList.size(); i++) {
            Expression selCon = selConList.get(i);
            Set<Integer> selColIndL = condColNameL.get(i);
            ScanOperator opr = (ScanOperator) operList.get(i);
            if (Main.isSwap == 0) {
                allDatasL.add(readOneTable(opr, selCon, i));
            } else {
                if (selCon == null) {
                    continue;
                }
                File new_file = null;
                File f_old = opr.f;
                String file_name_odd = f_old.getName();
                int last_index = file_name_odd.lastIndexOf(".");
                String file_name_new = file_name_odd.substring(0, last_index)
                        + "_1.dat";
                new_file = new File(Main.swapDir + "/" + file_name_new);
                selectExe(i, selCon, opr, new_file, selColIndL);
            }

        }
    }

    // execute selection, modify operator list
    public void selectExe(int i, Expression selCon, ScanOperator opr,
            File new_file, Set<Integer> selColIndL) {
        BufferedWriter bufW;
        try {
            bufW = new BufferedWriter(new FileWriter(new_file));
            File f = opr.f;
            Column[] thisSchema = schemaList.get(i);
            String line = opr.input.readLine();

            // Datum[] tuple = opr.readOneTuple();
            while (line != null) {
                String[] l_arr = line.split("\\|");
                Datum[] tuple = new Datum[selColIndL.size()];
                int tupI = 0;
                for (Iterator<Integer> itr = selColIndL.iterator(); itr
                        .hasNext();) {
                    Integer ind = itr.next();

                    String type = opr.colProperty.get(ind).getColDataType()
                            .getDataType().toLowerCase();
                    String v = l_arr[ind];
                    String colName = thisSchema[ind].getColumnName();
                    Datum d = null;
                    switch (type) {
                        case "int":
                            d = new Datum.INT(colName, v);
                            d.type = Type.INT;
                            break;
                        case "float":
                        case "decimal":
                            d = new Datum.FLOAT(colName, v);
                            d.type = Type.FLOAT;
                            break;
                        case "long":
                            d = new Datum.LONG(colName, v);
                            d.type = Type.LONG;
                            break;
                        case "string":
                        case "char":
                        case "varchar":
                            d = new Datum.STRING(colName, v);
                            d.type = Type.STRING;
                            break;
                        case "date":
                            d = new Datum.DATE(colName, v);
                            d.type = Type.DATE;
                            break;
                        default:
                            break;
                    }
                    tuple[tupI] = d;
                    tupI++;
                }
                Evaluator evalSel = new Evaluator(thisSchema, tuple);
                selCon.accept(evalSel);
                if (evalSel.getBool()) {
                    // StringBuffer tupleStr = tupleToStr(tuple);
                    // bufW.write(tupleStr.toString());
                    bufW.write(line);
                    bufW.newLine();
                }
                // tuple = opr.readOneTuple();
                line = opr.input.readLine();
            }
            bufW.flush();
            bufW.close();
            Operator newOper = new ScanOperator(new_file, opr.colProperty,
                    opr.tableAlias);
            operList.remove(i);
            operList.add(i, newOper);
            opr.input.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public StringBuffer tupleToStr(Datum[] tuple) {
        StringBuffer tupleStr = new StringBuffer();
        for (int j = 0; j < tuple.length; j++) {
            Datum t = tuple[j];
            tupleStr.append(t.toString());
            if (j < tuple.length) {
                tupleStr.append("|");
            }
        }
        return tupleStr;
    }

    // get join condition list;
    public void getJoinConList(Expression exp) {
        if (exp == null) {
            return;
        }
        if (exp instanceof BinaryExpression) {

            Expression leftExp = ((BinaryExpression) exp).getLeftExpression();
            Expression rightExp = ((BinaryExpression) exp).getRightExpression();

            if (!(leftExp instanceof BinaryExpression)) {
                // // leftexp is not binaryExpression,then the rightexp is also
                // not the binaryExpression.
                whereConlist.add(exp);
                if (isJoinCon(exp)) {
                    joinConlist.add(exp);
                }
                return;
            } else {
                getJoinConList(leftExp);
                getJoinConList(rightExp);
            }
        }
    }

    // judge whether the expression is join contion
    public boolean isJoinCon(Expression exp) {
        String leftExp = ((BinaryExpression) exp).getLeftExpression()
                .toString();
        String rightExp = ((BinaryExpression) exp).getRightExpression()
                .toString();
        int leftInd = -1; // index that which schema the left expression belongs
        // to
        int rightInd = -1; // index that which schema the right expression
        // belongs to
        for (int i = 0; i < schemaList.size(); i++) {
            Column[] schm = schemaList.get(i);
            for (Column col : schm) {
                if (leftExp.equals(col.getColumnName())) {
                    leftInd = i;
                } else if (rightExp.equals(col.getColumnName())) {
                    rightInd = i;
                }
            }
        }

        if ((leftInd == rightInd) && (leftInd != -1)) {
            return false;
        }

        // in the subselct query, the join condition expression may contain the
        // column in the outer select statement
        if (leftInd == -1) {
            for (int j = 0; j < Main.totalSchemaList.size(); j++) {
                Column[] schm = Main.totalSchemaList.get(j);
                if (leftInd != -1) {
                    break;
                }
                for (Column col : schm) {
                    if (leftExp.equals(col.getColumnName())) {
                        leftInd = j;
                        break;
                    }
                }
            }
            if (leftInd == -1) {
                return false;
            } else {
                schemaList.add(Main.totalSchemaList.get(leftInd));
                ScanOperator o = (ScanOperator) Main.totalOperList.get(leftInd);
                Operator new_o = new ScanOperator(o.f, o.colProperty,
                        o.tableAlias);
                operList.add(new_o);
            }
        }
        if (rightInd == -1) {
            for (int j = 0; j < Main.totalSchemaList.size(); j++) {
                if (rightInd != -1) {
                    break;
                }
                Column[] schm = Main.totalSchemaList.get(j);
                for (Column col : schm) {
                    if (rightExp.equals(col.getColumnName())) {
                        rightInd = j;
                        break;
                    }
                }
            }
            if (rightInd == -1) {
                return false;
            } else {
                schemaList.add(Main.totalSchemaList.get(rightInd));
                ScanOperator o = (ScanOperator) Main.totalOperList
                        .get(rightInd);
                Operator new_o = new ScanOperator(o.f, o.colProperty,
                        o.tableAlias);
                operList.add(new_o);
            }
        }

        return true;
    }

    // get all select condition for each schema
    public ArrayList<Expression> getSelConList() {
        ArrayList<Expression> conList = new ArrayList<Expression>();
        if (whereCondition == null) {
            return conList;
        }
        for (int i = 0; i < schemaList.size(); i++) {
            Column[] schema = schemaList.get(i);
            oneTableConColL = new HashSet<>();
            Expression selCon = getConExpForSchema(whereCondition, schema);
            conList.add(selCon);
            condColNameL.add(oneTableConColL);
        }

        return conList;
    }

    public Expression getConExpForSchema(Expression exp, Column[] schema) {
        Expression e = null;
        if (exp == null) {
            return null;
        }
        if ((exp instanceof AndExpression) || (exp instanceof OrExpression)) {
            Expression leftExp;
            Expression rightExp;
            if (exp instanceof AndExpression) {
                e = new AndExpression(null, null);
                AndExpression exp_binary = (AndExpression) exp;
                leftExp = exp_binary.getLeftExpression();
                rightExp = exp_binary.getRightExpression();
            } else {
                e = new OrExpression(null, null);
                OrExpression exp_binary = (OrExpression) exp;
                leftExp = exp_binary.getLeftExpression();
                rightExp = exp_binary.getRightExpression();
            }

            Expression l_e = getConExpForSchema(leftExp, schema);
            Expression r_e = getConExpForSchema(rightExp, schema);
            if (l_e == null && r_e == null) {
                return null;
            } else if (l_e == null) {
                return r_e;
            } else if (r_e == null) {
                return l_e;
            } else {
                ((BinaryExpression) e).setLeftExpression(l_e);
                ((BinaryExpression) e).setRightExpression(r_e);
                return e;
            }
        } else if (exp instanceof BinaryExpression) {
			// exp is the a=b form,return exp

            // if the right expression is subselect, then query the subselect
            // and replace the right expression by the result
            Expression b_e = ((BinaryExpression) exp).getRightExpression();
            if (b_e instanceof SubSelect) {

                SubSelect subS = (SubSelect) b_e;
                SelectBodyVistor selBodyVistor = new SelectBodyVistor(
                        Main.dataDir, Main.tables);
                subS.getSelectBody().accept(selBodyVistor);

                ArrayList<ArrayList<Datum>> outP = selBodyVistor.outPutArrList;
                Datum d = outP.get(0).get(0);
                Type t = d.type;
                String v = d.value;
                Expression new_r;
                if (t == Type.DATE) {
                    new_r = new DateValue(v);
                }
                if (t == Type.STRING) {
                    new_r = new StringValue(v);
                }
                if (t == Type.FLOAT) {
                    new_r = new DoubleValue(v);
                } else {
                    new_r = new LongValue(v);
                }
                ((BinaryExpression) exp).setRightExpression(new_r);
            }
            if (isJoinConLater(exp)) {
                return null;
            } else {
                ArrayList<Integer> schmIndL = isSelCon(exp, schema);
                if (!schmIndL.isEmpty()) {
                    for (Integer in : schmIndL) {
                        oneTableConColL.add(in);
                    }
                    return exp;
                } else {
                    return null;
                }

            }
        } else if (exp instanceof Parenthesis) {
            return getConExpForSchema(((Parenthesis) exp).getExpression(),
                    schema);
        } else {
            return exp;
        }
    }

    public boolean isJoinConLater(Expression exp) {
        for (int i = 0; i < joinConlist.size(); i++) {
            Expression joinE = joinConlist.get(i);
            if (exp.equals(joinE)) {
                return true;
            }
        }
        return false;
    }

    // judge whether a condtion expression is belongs to the schema
    public ArrayList<Integer> isSelCon(Expression exp, Column[] schema) {
        ArrayList<Integer> colIndL = new ArrayList<>();
        String leftExp = ((BinaryExpression) exp).getLeftExpression()
                .toString();
        String rightExp = ((BinaryExpression) exp).getRightExpression()
                .toString();

        for (int i = 0; i < schema.length; i++) {
            Column col = schema[i];
            if (leftExp.equals(col.getColumnName())) {
                colIndL.add(i);
            } else if (rightExp.equals(col.getColumnName())) {
                colIndL.add(i);
            }
        }
        return colIndL;
    }

    public void normalJoinExe() {
        // first do the equal join
        euqalJoinExe();
        joinSchema = getJoinSchema();
        if (operList.size() == 1) {
            finalOpr = operList.get(0);
            if (Main.isSwap == 0) {
                finalDataTable = allDatasL.get(0);
            }
            return;
        }
        if (Main.isSwap == 0) {
            finalDataTable = allDatasL.get(0);
            for (int ts = 1; ts < allDatasL.size(); ts++) {
                finalDataTable = nestloopJoinTwoTables(finalDataTable,
                        allDatasL.get(ts));
            }
            return;
        }

        // joinSchema = getJoinSchema();
        ScanOperator opr = (ScanOperator) operList.get(1);
        BufferedWriter burW;
        try {
            burW = new BufferedWriter(new FileWriter(Main.swapDir
                    + "/final.dat"));

            // normal join
            Datum[] joinTuple = ReadJoinedOneTuple();
            if (joinTuple == null) {
                burW.flush();
                burW.close();
                for (Operator o : operList) {
                    ((ScanOperator) o).input.close();
                }
            }
            if (onCondition != null) {
                Evaluator eval = new Evaluator(joinSchema, joinTuple);
                onCondition.accept(eval);
                if (eval.getBool()) {
                    burW.write(tupleToStr(joinTuple).toString());
                    burW.newLine();
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // generate ArrayList<ColumnDefinition>
        ArrayList<ColumnDefinition> colProList = getJoinColProperty();
        finalOpr = new ScanOperator(new File(Main.swapDir + "/final.dat"),
                colProList, null);
    }

    public ArrayList<ColumnDefinition> getJoinColProperty() {
        ArrayList<ColumnDefinition> colProList = new ArrayList<ColumnDefinition>();
        for (int i = 0; i < operList.size(); i++) {
            ScanOperator oprr = (ScanOperator) operList.get(i);
            ArrayList<ColumnDefinition> oprr_colList = oprr.colProperty;
            Column[] sch = schemaList.get(i);
            for (int j = 0; j < oprr_colList.size(); j++) {
                ColumnDefinition colD = oprr_colList.get(j);
                colD.setColumnName(sch[j].getColumnName());
                colProList.add(colD);
            }
        }
        return colProList;
    }

    public ArrayList<ColumnDefinition> getJoinColProperty(Operator op1,
            Operator op2, Column[] sch1, Column[] sch2) {
        ArrayList<ColumnDefinition> colProList = new ArrayList<ColumnDefinition>();
        ArrayList<ColumnDefinition> op1_colProList = ((ScanOperator) op1).colProperty;
        ArrayList<ColumnDefinition> op2_colProList = ((ScanOperator) op2).colProperty;
        for (int i = 0; i < op1_colProList.size(); i++) {
            ColumnDefinition colD = op1_colProList.get(i);
            ColumnDefinition new_c = new ColumnDefinition();
            new_c.setColumnName(sch1[i].getColumnName());
            new_c.setColDataType(colD.getColDataType());
            colProList.add(new_c);
        }
        for (int j = 0; j < op2_colProList.size(); j++) {
            ColumnDefinition colD = op2_colProList.get(j);
            ColumnDefinition new_c = new ColumnDefinition();
            new_c.setColumnName(sch2[j].getColumnName());
            new_c.setColDataType(colD.getColDataType());
            colProList.add(new_c);
        }
        return colProList;
    }

    // execute equil join, replace the operator list and schema list respond to
    // operator
    public void euqalJoinExe() {
        if (joinConlist.isEmpty()) {
            return;
        }
        for (int i = 0; i < joinConlist.size(); i++) {
            Expression joinCon = joinConlist.get(i);
            Integer[] oprInd = getEquJoinOprIndex(joinCon);
            if (oprInd[0] == oprInd[1]) {
                if (Main.isSwap == 0) {
                    ArrayList<Datum[]> tabT = allDatasL.get(oprInd[0]);
                    int remTupleNum = 0;
                    for (int tupI = 0; tupI < tabT.size(); tupI++) {
                        Datum[] oneT = tabT.get(tupI);
                        Evaluator evalSel = new Evaluator(
                                schemaList.get(oprInd[0]), oneT);
                        joinCon.accept(evalSel);
                        if (!evalSel.getBool()) {
                            tabT.remove(tupI - remTupleNum);
                            remTupleNum++;
                        }
                    }
                } else {
                    ScanOperator oprr = (ScanOperator) operList.get(oprInd[0]);
                    File f_o = oprr.f;
                    int last_ind = f_o.getName().lastIndexOf(".");
                    String new_name_Str = f_o.getName().substring(0, last_ind);
                    File new_f = new File(Main.swapDir + "/" + new_name_Str
                            + "_1.dat");
                    ArrayList<Integer> selIndL = isSelCon(joinCon,
                            schemaList.get(oprInd[0]));
                    Set<Integer> selIndSet = new HashSet<>();
                    for (Integer ind : selIndL) {
                        selIndSet.add(ind);
                    }
                    selectExe(oprInd[0], joinCon, oprr, new_f, selIndSet);
                }
                continue;
            }
            // get the operator after equal join
            checkSize(oprInd[0], oprInd[1]);

            if (Main.isSwap == 1) {
                Operator newOpr = null;
                try {
                    newOpr = hashJoin(operList.get(oprInd[0]),
                            operList.get(oprInd[1]), schemaList.get(oprInd[0]),
                            schemaList.get(oprInd[1]), joinCon);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                // remove two operators before join and add the operator after
                // join
                // to position 0;
                // change schema list at the same way
                operList.remove(oprInd[0].intValue());
                if (oprInd[0] < oprInd[1]) {
                    operList.remove(oprInd[1].intValue() - 1);
                } else {
                    operList.remove(oprInd[1].intValue());
                }

                operList.add(0, newOpr);
            } else {
                ArrayList<Datum[]> newJoinTable = hashJoinTwoTablesInMem(
                        allDatasL.get(oprInd[0]), allDatasL.get(oprInd[1]),
                        joinCon, schemaList.get(oprInd[0]),
                        schemaList.get(oprInd[1]));
                allDatasL.remove(oprInd[0].intValue());
                if (oprInd[0] < oprInd[1]) {
                    allDatasL.remove(oprInd[1].intValue() - 1);
                } else {
                    allDatasL.remove(oprInd[1].intValue());
                }

                allDatasL.add(0, newJoinTable);
            }

            // Column[] newSchema = mergeTwoSchema(schemaList.get(oprInd[0]),
            // schemaList.get(oprInd[1]));
            Column[] newSchema = mergeTwoSchema(schemaList.get(oprInd[0]),
                    schemaList.get(oprInd[1]));
            schemaList.remove(oprInd[0].intValue());
            if (oprInd[0] < oprInd[1]) {
                schemaList.remove(oprInd[1].intValue() - 1);
            } else {
                schemaList.remove(oprInd[1].intValue());
            }
            schemaList.add(0, newSchema);
        }
    }

    // hash join
    public Operator hashJoin(Operator op1, Operator op2, Column[] sch1,
            Column[] sch2, Expression con) throws IOException {

        ScanOperator scan_op1 = (ScanOperator) op1;
        ScanOperator scan_op2 = (ScanOperator) op2;

        String[] keyARR = con.toString().split("=");
        keyARR[0] = keyARR[0].replaceAll(" ", "");
        keyARR[1] = keyARR[1].replaceAll(" ", "");
        int left_ind = -1;
        int right_ind = -1;
        for (int i = 0; i < sch1.length; i++) {
            if (sch1[i].getColumnName().equals(keyARR[0])
                    || sch1[i].getColumnName().equals(keyARR[1])) {
                left_ind = i;
                break;
            }
        }
        for (int j = 0; j < sch2.length; j++) {
            if (sch2[j].getColumnName().equals(keyARR[1])
                    || sch2[j].getColumnName().equals(keyARR[0])) {
                right_ind = j;
                break;
            }
        }

        // RandomAccessFile input = new RandomAccessFile(scan_op1.f, "rw");
        BufferedReader reder1 = new BufferedReader(new FileReader(scan_op1.f));
        String file1 = (scan_op1).f.getName();
        String file1_name = file1.substring(0, file1.lastIndexOf("."));

        Map map = new HashMap();
        Object key;
        String line;
        String[] left_data;

        String file2 = (scan_op2).f.getName();
        int name_ind = file2.lastIndexOf(".");
        String file2_name = file2.substring(0, name_ind);
        File n_f = new File(Main.swapDir + "/" + file1_name + "_" + file2_name
                + ".dat");
        BufferedWriter bufW = new BufferedWriter(new FileWriter(n_f));

        int cnt = 0;
        int sizecnt = 0;

        while ((line = reder1.readLine()) != null) {
            cnt = 0;
            left_data = line.split("\\|");
            key = left_data[left_ind];
            if (!map.containsKey(key)) {
                ArrayList<String> tmp = new ArrayList<>();
                tmp.add(line);
                map.put(key, tmp);
            } else {
                ((ArrayList) map.get(key)).add(line);
            }
            if (sizecnt == 0) {
                sizecnt = (1 << 26) / line.getBytes().length;
            }

            while ((line = reder1.readLine()) != null) {
                cnt++;
                left_data = line.split("\\|");
                key = left_data[left_ind];
                if (!map.containsKey(key)) {
                    ArrayList<String> tmp = new ArrayList<>();
                    tmp.add(line);
                    map.put(key, tmp);
                } else {
                    ((ArrayList) map.get(key)).add(line);
                }
                if (cnt >= sizecnt) {
                    break;
                }
            }

            BufferedReader reader2 = new BufferedReader(new FileReader(
                    scan_op2.f));

            String now;
            String ff;
            Object find;
            String[] right_data;
            ArrayList<String> hashoff = new ArrayList();

            while ((now = reader2.readLine()) != null) {
                right_data = now.split("\\|");
                find = right_data[right_ind];
                if (map.containsKey(find)) {
                    hashoff = (ArrayList) map.get(find);
                    int arrCnt = hashoff.size();
                    for (int i = 0; i < arrCnt; i++) {
                        ff = hashoff.get(i);
                        String newLine;
                        if (ff.endsWith("|")) {
                            newLine = ff + now;
                        } else {
                            newLine = ff + "|" + now;
                        }
                        bufW.write(newLine);
                        bufW.newLine();
                    }

                }
            }

            map.clear();
            reader2.close();

        }
        bufW.flush();
        bufW.close();
        reder1.close();

        scan_op1.input.close();
        scan_op2.input.close();

        Operator equalOpr = new ScanOperator(n_f, getJoinColProperty(scan_op1,
                scan_op2, sch1, sch2), null);
        // TODO code application logic here

        return equalOpr;

    }

    public Column[] mergeTwoSchema(Column[] s1, Column[] s2) {
        int s1_len = s1.length;
        int s2_len = s2.length;

        Column[] s = new Column[s1_len + s2_len];
        for (int i = 0; i < s1_len; i++) {
            s[i] = s1[i];
        }

        for (int j = 0; j < s2_len; j++) {
            s[j + s1_len] = s2[j];
        }
        return s;
    }

    // get the operator index in oprlist
    public Integer[] getEquJoinOprIndex(Expression exp) {

        String leftExp = ((BinaryExpression) exp).getLeftExpression()
                .toString();
        String rightExp = ((BinaryExpression) exp).getRightExpression()
                .toString();
        int leftInd = -1; // index that which schema the left expression belongs
        // to
        int rightInd = -1; // index that which schema the right expression
        // belongs to
        for (int i = 0; i < schemaList.size(); i++) {
            Column[] schm = schemaList.get(i);
            for (Column col : schm) {
                if (leftExp.equals(col.getColumnName())) {
                    leftInd = i;
                } else if (rightExp.equals(col.getColumnName())) {
                    rightInd = i;
                }
            }
        }
        Integer[] intArr = {leftInd, rightInd};
        return intArr;

    }

    @Override
    public Datum[] readOneTuple() {
        // TODO Auto-generated method stub
        if (Main.isSwap == 1) {
            return finalOpr.readOneTuple();
        } else {
            // int c = 0;
            // for(int i = 0; i < finalDataTable.size(); i++){
            // Datum[] t = finalDataTable.get(i);
            // if(t[3].value.equals("Brand#12")&&
            // t[4].value.equals("PROMO ANODIZED STEEL") &&
            // t[5].value.equals("46")){
            // c++;
            // }
            // }

            if (finalDataTable.isEmpty()) {
                return null;
            }

            if (readTupInd == finalDataTable.size()) {
                readTupInd = 0;
                return null;
            } else {
                Datum[] tup = finalDataTable.get(readTupInd);
                readTupInd++;
                return tup;
            }

        }

        // Datum[] joinTuple = null;
        // if (onCondition == null) {
        // joinTuple = ReadJoinedOneTuple();
        // } else {
        // while (joinTuple == null) {
        // joinTuple = ReadJoinedOneTuple();
        // if (joinTuple == null)
        // return null;
        // if (onCondition != null) {
        // Evaluator eval = new Evaluator(joinSchema, joinTuple);
        // onCondition.accept(eval);
        // if (!eval.getBool()) {
        // joinTuple = null;
        // }
        // }
        //
        // // wherecondition
        // // if (joinTuple != null && whereCondition != null) {
        // // Evaluator evalWhere = new Evaluator(joinSchema, joinTuple);
        // // whereCondition.accept(evalWhere);
        // // if (!evalWhere.getBool()) {
        // // joinTuple = null;
        // // }
        // // }
        // // }
        // }
        // return joinTuple;
    }

    // get join schema
    public Column[] getJoinSchema() {
        // joinSchema = null;
        Vector<Column> vec = new Vector<Column>();
        for (int m = 0; m < schemaList.size(); m++) {
            Column[] columnArray = schemaList.get(m);
            for (int n = 0; n < columnArray.length; n++) {
                vec.add(columnArray[n]);
            }
        }

        joinSchema = new Column[vec.size()];
        for (int i = 0; i < vec.size(); i++) {
            joinSchema[i] = vec.get(i);
        }
        return joinSchema;
    }

    // read one joined tuple from operators
    public Datum[] ReadJoinedOneTuple() {

        Datum[] joinTuple = null;
        // a,b,c
        if (index == 0) {
            for (int i = 0; i < operList.size(); i++) {
                // Datum[] oneTuple = operList.get(i).readOneTuple();
                // if(){
                //
                // }
                datumList.add(operList.get(i).readOneTuple());
            }
            index = 1;
        } else {
            Datum[] tuple = operList.get(operList.size() - 1).readOneTuple();
            datumList.remove(datumList.size() - 1);
            datumList.add(datumList.size(), tuple);
            int j = datumList.size() - 1;
            while (datumList.get(j) == null && j >= 0) {
                if (j == 0) {
                    datumList = null;
                    break;
                }
                datumList.remove(j);
                operList.get(j).reset();
                datumList.add(j, operList.get(j).readOneTuple());
                if (j != 0) {
                    datumList.remove(j - 1);
                    datumList.add(j - 1, operList.get(j - 1).readOneTuple());
                }
                j--;
            }
        }
        if (datumList == null) {
            return null;
        }
        Vector<Datum> vec = new Vector<Datum>();
        for (int m = 0; m < datumList.size(); m++) {
            Datum[] datumArray = datumList.get(m);
            for (int n = 0; n < datumArray.length; n++) {
                vec.add(datumArray[n]);
            }
        }
        if (vec.isEmpty()) {
            return null;
        }
        joinTuple = new Datum[vec.size()];
        for (int i = 0; i < vec.size(); i++) {
            joinTuple[i] = vec.get(i);
        }
        return joinTuple;
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub
        for (Operator oper : operList) {
            oper.reset();
        }

    }

    public void checkSize(int ind1, int ind2) {
        ScanOperator scan_o1 = (ScanOperator) operList.get(ind1);
        ScanOperator scan_o2 = (ScanOperator) operList.get(ind2);
        if (scan_o1.f.length() < scan_o2.f.length()) {
            return;
        }

        Operator temp_o = operList.get(ind1);
        operList.set(ind1, operList.get(ind2));
        operList.set(ind2, temp_o);

        Column[] temp_s = schemaList.get(ind1);
        schemaList.set(ind1, schemaList.get(ind2));
        schemaList.set(ind2, temp_s);

        if (Main.isSwap == 0) {
            ArrayList<Datum[]> tem_Tab = allDatasL.get(ind1);
            allDatasL.set(ind1, allDatasL.get(ind2));
            allDatasL.set(ind2, tem_Tab);
        }
    }

    public ArrayList<Datum[]> readOneTable(Operator oneOpr,
            Expression SelectCon, int tableInd) {
        ArrayList<Datum[]> oneTableTuples = new ArrayList<>();
        Datum[] tuple = oneOpr.readOneTuple();
        if (SelectCon == null) {
            while (tuple != null) {
                oneTableTuples.add(tuple);
                tuple = oneOpr.readOneTuple();
            }
        } else {
            while (tuple != null) {
                Evaluator evalSel = new Evaluator(schemaList.get(tableInd),
                        tuple);
                SelectCon.accept(evalSel);
                if (evalSel.getBool()) {
                    oneTableTuples.add(tuple);
                }
                tuple = oneOpr.readOneTuple();
            }
        }
        return oneTableTuples;
    }

    public ArrayList<Datum[]> nestloopJoinTwoTables(ArrayList<Datum[]> tab1,
            ArrayList<Datum[]> tab2) {
        if (tab1.isEmpty()) {
            return tab2;
        }
        if (tab2.isEmpty()) {
            return tab1;
        }
        ArrayList<Datum[]> joinTab = new ArrayList<>();
        for (int i = 0; i < tab1.size(); i++) {
            Datum[] tup1 = tab1.get(i);
            for (int j = 0; j < tab2.size(); j++) {
                Datum[] tup2 = tab2.get(j);

                Datum[] newTup = joinTwoTuples(tup1, tup2);

                joinTab.add(newTup);
            }
        }

        return joinTab;
    }

    public Datum[] joinTwoTuples(Datum[] tup1, Datum[] tup2) {
        Datum[] newTup = new Datum[tup1.length + tup2.length];
        System.arraycopy(tup1, 0, newTup, 0, tup1.length);
        System.arraycopy(tup2, 0, newTup, tup1.length, tup2.length);
        return newTup;
    }

    public ArrayList<Datum[]> hashJoinTwoTablesInMem(ArrayList<Datum[]> tab1,
            ArrayList<Datum[]> tab2, Expression con, Column[] sch1,
            Column[] sch2) {
        if (tab1.isEmpty()) {
            return tab2;
        }
        if (tab2.isEmpty()) {
            return tab1;
        }
        // get hash column index
        String[] keyARR = con.toString().split("=");
        keyARR[0] = keyARR[0].replaceAll(" ", "");
        keyARR[1] = keyARR[1].replaceAll(" ", "");
        int left_ind = -1;
        int right_ind = -1;
        for (int i = 0; i < sch1.length; i++) {
            if (sch1[i].getColumnName().equals(keyARR[0])
                    || sch1[i].getColumnName().equals(keyARR[1])) {
                left_ind = i;
                break;
            }
        }
        for (int j = 0; j < sch2.length; j++) {
            if (sch2[j].getColumnName().equals(keyARR[1])
                    || sch2[j].getColumnName().equals(keyARR[0])) {
                right_ind = j;
                break;
            }
        }

        // hash map, key = condition value, value = the list of index of tuples
        // with the same key
        HashMap<String, ArrayList<Integer>> hashm = new HashMap<>();
        for (int i = 0; i < tab1.size(); i++) {
            Datum[] tup = tab1.get(i);
            String key = tup[left_ind].value;
            if (hashm.containsKey(key)) {
                hashm.get(key).add(i);
            } else {
                ArrayList<Integer> newArr = new ArrayList<>();
                newArr.add(i);
                hashm.put(key, newArr);
            }
        }
        ArrayList<Datum[]> joinTab = new ArrayList<>();

        for (int j = 0; j < tab2.size(); j++) {
            Datum[] tup = tab2.get(j);
            String cmpKey = tup[right_ind].value;
            if (hashm.containsKey(cmpKey)) {
                ArrayList<Integer> indexList = hashm.get(cmpKey);
                for (int m = 0; m < indexList.size(); m++) {
                    Datum[] tupCmp = tab1.get(indexList.get(m).intValue());
                    Datum[] newTup = joinTwoTuples(tupCmp, tup);
                    joinTab.add(newTup);
                }
            }
        }

        return joinTab;
    }

}
