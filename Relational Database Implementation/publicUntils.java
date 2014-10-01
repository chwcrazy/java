package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.Datum.Type;

public class publicUntils {
	public static void printOutPutTuples(ArrayList<ArrayList<Datum>> outputList) {
		if (!outputList.isEmpty()) {
			for (ArrayList<Datum> oneTupleList : outputList) {
				if (!oneTupleList.isEmpty()) {
					System.out.print(oneTupleList.get(0));
					for (int i = 1; i < oneTupleList.size(); i++) {
						System.out.print("|" + oneTupleList.get(i).toString());
					}
				}
				System.out.println("");
			}
		}

		// System.out.print(outputList.size());
	}

	public static Datum[] generateDatumArrays(String line,
			ArrayList<ColumnDefinition> colProperty, String tableAlias) {
		String[] cols = line.split("\\|");
		Datum[] ret = new Datum[cols.length];
		for (int i = 0; i < cols.length; i++) {
			String colType = colProperty.get(i).getColDataType().getDataType()
					.toLowerCase();

			String colName;
			if (tableAlias != null) {
				colName = tableAlias + colProperty.get(i).getColumnName();
			} else {
				colName = colProperty.get(i).getColumnName();
			}
			switch (colType) {
			case "int":
				ret[i] = new Datum.INT(colName, cols[i]);
				ret[i].type = Type.INT;
				break;
			case "float":
			case "decimal":
				ret[i] = new Datum.FLOAT(colName, cols[i]);
				ret[i].type = Type.FLOAT;
				break;
			case "long":
				ret[i] = new Datum.LONG(colName, cols[i]);
				ret[i].type = Type.LONG;
				break;
			case "string":
			case "char":
			case "varchar":
				ret[i] = new Datum.STRING(colName, cols[i]);
				ret[i].type = Type.STRING;
				break;
			case "date":
				ret[i] = new Datum.DATE(colName, cols[i]);
				ret[i].type = Type.DATE;
				break;
			default:
				break;
			}
		}
		return ret;
	}

	public static Comparator<Datum[]> generateCmptor(final List orderByList) {
		Comparator<Datum[]> new_c = new Comparator<Datum[]>() {

			@Override
			public int compare(Datum[] o1, Datum[] o2) {
				// TODO Auto-generated method stub
				int comprInt = 0;
				OrderByElementVistor orderVistor = new OrderByElementVistor(o1,
						o2);
				for (int m = 0; m < orderByList.size(); m++) {
					OrderByElement orderEle = (OrderByElement) orderByList
							.get(m);

					orderEle.accept(orderVistor);

					comprInt = orderVistor.getCmpInt();
					if (comprInt != 0) {
						return comprInt;
					} else
						continue;
				}
				return 0;
			}
		};
		return new_c;
	}
}
