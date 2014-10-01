package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class Main {

	public static File dataDir;
	public static File swapDir;
	public static HashMap<String, CreateTable> tables;
	public static ArrayList<Operator> totalOperList;
	public static ArrayList<Column[]> totalSchemaList;
	public static int isSwap;
	public static int bufferSize;
	public static File nowReadOutFile;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long beginT = System.currentTimeMillis();
		totalOperList = new ArrayList<Operator>();
		totalSchemaList = new ArrayList<Column[]>();
		dataDir = null; // the root of all files
		isSwap = 0;
		swapDir = null;
		bufferSize = 1 << 13;
		nowReadOutFile = null;

		ArrayList<File> sqlFiles = new ArrayList<File>(); // the file name

		tables = new HashMap<String, CreateTable>();

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--data")) {
				dataDir = new File(args[i + 1]);
				i++;
			} else if (args[i].equals("--swap")) {
				isSwap = 1;
				swapDir = new File(args[i + 1]);
				i++;
			} else {
				sqlFiles.add(new File(args[i]));
			}
		}

		for (File sql : sqlFiles) {
			try {
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);

				Statement stmt;
				while ((stmt = parser.Statement()) != null) {
					if (stmt instanceof CreateTable) {
						CreateTable ct = (CreateTable) stmt;

						// System.out.println("tablename: "
						// + ct.getTable().getName());
						tables.put(ct.getTable().getName().toLowerCase(), ct);

					} else if (stmt instanceof Select) {
						// SelectBody select = ((Select) stmt).getSelectBody();
						// System.out.println("now: " + select);
						SelectBodyVistor selBodyVistor = new SelectBodyVistor(
								dataDir, tables);
						((Select) stmt).getSelectBody().accept(selBodyVistor);

						if (isSwap == 0) {

							publicUntils
									.printOutPutTuples(selBodyVistor.outPutArrList);
						} else {
							BufferedReader printBuff = new BufferedReader(
									new FileReader(nowReadOutFile));
							String line = printBuff.readLine();

							while (line != null) {
								int len = line.length();
								if (line.endsWith("|")) {
									line = line.substring(0, len - 1);
								}
								System.out.println(line);
								line = printBuff.readLine();
							}
						}
						// System.out.println("count ====" +
						// selBodyVistor.outPutArrList.size());

						long endT = System.currentTimeMillis();
						// System.out.println("time ::::::::::::" +
						// (endT-beginT));
					} else {
						System.out
								.println("still do not know how to handle it!!");
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}
	}

}
