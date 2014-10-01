package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromScanner implements FromItemVisitor {

	File basePath;
	HashMap<String, CreateTable> tables;
	int joinListSize; // add the alias of table

	public Column[] schema;
	public Operator source;
	public ArrayList<ColumnDefinition> colProperty;

	public FromScanner(File basePath, HashMap<String, CreateTable> tables,
			int joinListSize) {
		this.basePath = basePath;
		this.tables = tables;
		colProperty = new ArrayList<ColumnDefinition>();
		this.joinListSize = joinListSize;
	}

	@Override
	public void visit(Table tableName) {
		// TODO Auto-generated method stub
		// tableName is table
		String tableAlias = "";
		if (tableName.getAlias() != null) {
			tableAlias = tableName.getAlias() + ".";
		} else {
			if (joinListSize != 0) {
				tableAlias = tableName.getName() + ".";

			}
		}
		CreateTable table = tables.get(tableName.getName().toLowerCase());

		List cols = table.getColumnDefinitions();
		schema = new Column[cols.size()];

		for (int i = 0; i < cols.size(); i++) {
			ColumnDefinition col = (ColumnDefinition) cols.get(i);
			colProperty.add(col);
			schema[i] = new Column(tableName, tableAlias + col.getColumnName());
		}

		if (tableName.getName().contains("_")) {
			source = new ScanOperator(new File(Main.swapDir + "/"
					+ tableName.getName() + ".dat"), colProperty, tableAlias);
		} else {

			source = new ScanOperator(new File(basePath + "/"
					+ tableName.getName() + ".dat"), colProperty, tableAlias);
		}
		// source = new ScanOperator(basePath, colProperty, tableAlias);

	}

	@Override
	public void visit(SubSelect subSel) {

		subSelectOperator subSelSource = new subSelectOperator(basePath,
				tables, subSel);
		Column[] subSelSchema = subSelSource.schema;

		schema = subSelSchema;
		if(Main.isSwap == 0){
			source = subSelSource;
		}else{
			source = subSelSource.oper;
		}

	}

	@Override
	public void visit(SubJoin subJoinExp) {
		// TODO Auto-generated method stub
		basePath = null;
		// throw new EmptyStackException();
	}

}
