package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.Datum.Type;

public class subSelectOperator implements Operator {

	BufferedReader input;
	File dataDir;
	ArrayList<ColumnDefinition> colProperty;
	String tableAlias;
	HashMap<String, CreateTable> tables;
	public ArrayList<ArrayList<Datum>> outPutArrList;
	SubSelect subSel;
	int readIndex;

	long limN = 0;
	Operator oper;

	long countN = 0;

	public Column[] schema;

	public subSelectOperator(File dataDir, HashMap<String, CreateTable> tables,
			SubSelect subSel) {
		this.dataDir = dataDir;
		this.tables = tables;
		this.subSel = subSel;

		reset();
	}

	@Override
	public Datum[] readOneTuple() {

		Datum[] tuple = null;
		if (readIndex < outPutArrList.size()) {
			ArrayList<Datum> oneTupleList = outPutArrList.get(readIndex);
			tuple = new Datum[oneTupleList.size()];
			for (int i = 0; i < oneTupleList.size(); i++) {
				tuple[i] = oneTupleList.get(i);
			}
			readIndex++;
			return tuple;
		}
		return null;

		// else{
		//
		// Datum[] tuple = oper.readOneTuple();
		//
		// if(tuple == null) return null;
		// for (int n = 0; n < tuple.length; n++) {
		// tuple[n].setColName(schema[n].getColumnName());
		// // d.setColName(schema[i].getColumnName());
		// }
		// countN++;
		// if(limN != 0 && countN > limN) return null;
		// else return tuple;
		// }
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		Limit l = ((PlainSelect) subSel.getSelectBody()).getLimit();
		if (l != null) {
			limN = l.getRowCount();
		}

		SelectBodyVistor selBodyVistor = new SelectBodyVistor(dataDir, tables);
		(((PlainSelect) subSel.getSelectBody())).accept(selBodyVistor);

		Column[] subSelSchema = selBodyVistor.getSchema();

		String aliaStr = subSel.getAlias();
		schema = new Column[subSelSchema.length];
		String newColName = null;

		if (aliaStr != null) {
			for (int i = 0; i < subSelSchema.length; i++) {
				Table tableName = subSelSchema[i].getTable();
				String colStr = subSelSchema[i].getColumnName().toLowerCase();
				String tableAlias = tableName.getAlias();
				if (tableAlias != null
						&& colStr.contains(tableAlias.toLowerCase())
						&& colStr.contains(".")) {
					String[] strArr = colStr.split("\\.");
					newColName = aliaStr + "." + strArr[1];
				} else {
					newColName = aliaStr + "." + colStr;
				}
				Table newTable = new Table();
				newTable.setName(aliaStr);

				// newTable.setName(tableName.getName());
				// newTable.setSchemaName(tableName.getSchemaName());
				schema[i] = new Column(newTable, newColName);
			}
		} else {
			schema = subSelSchema;
		}

		ArrayList<ArrayList<Datum>> PutArrList;
		if (Main.isSwap == 0) {
			PutArrList = selBodyVistor.outPutArrList;
			for (int m = 0; m < PutArrList.size(); m++) {
				ArrayList<Datum> datumL = PutArrList.get(m);
				// ArrayList<Datum> newTuple = new ArrayList<Datum>();
				for (int n = 0; n < datumL.size(); n++) {
					datumL.get(n).setColName(schema[n].getColumnName());
					// d.setColName(schema[i].getColumnName());
				}
			}
			outPutArrList = PutArrList;
		} else {
			ArrayList<Type> finalTypes = selBodyVistor.getFinalTypes();
			File file = selBodyVistor.getReadoutFile();

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
			oper = new ScanOperator(file, new_colDefs, null);
		}
		readIndex = 0;
	}

}
