package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ScanOperator implements Operator {

	BufferedReader input;
	File f;
	ArrayList<ColumnDefinition> colProperty;
	String tableAlias;

	public ScanOperator(File f, ArrayList<ColumnDefinition> colProperty,
			String tableAlias) {
		this.f = f;
		this.colProperty = colProperty;
		this.tableAlias = tableAlias;
		reset();
	}

	// public BufferedReader getBufReader(){
	// return this.input;
	// }
	// public File getFile(){
	// return this.f;
	// }
	// public ArrayList<ColumnDefinition> getColProp(){
	// return this.colProperty;
	// }
	// public String getTableAlias(){
	// return this.tableAlias;
	// }

	@Override
	public Datum[] readOneTuple() {
		// TODO Auto-generated method stub
		if (input == null)
			return null;
		String line = null;
		try {
			line = input.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println("line :::::::::::::::::::::"+line);
		if (line == null || !line.contains("|"))
			return null;
		Datum[] ret = publicUntils.generateDatumArrays(line, colProperty,
				tableAlias);
		return ret;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try {
			input = new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			input = null;
		}

	}

}
