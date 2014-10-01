package edu.buffalo.cse562;

public class OperatorTest {

	// public static void main(String[] args) {
	// // TODO Auto-generated method stub
	// ArrayList<ColumnDefinition> colProperty = new ArrayList<>();
	// Operator oper = new ScanOperator(new
	// File("NBA/nba11.expected.dat"),colProperty);
	// // A<B,A and B are columns' name
	//
	// MinorThan cmp = new MinorThan();
	// cmp.setLeftExpression(new Column(null, "A"));
	// cmp.setRightExpression(new Column(null, "B"));
	//
	// oper = new SelectionOperator(oper, new Column[] {
	// new Column(null, "A"), new Column(null, "B") }, cmp);
	//
	//
	// dump(oper);
	// }

	public static void dump(Operator oper) {
		Datum[] row = oper.readOneTuple();

		while (row != null) {
			for (Datum col : row) {
				System.out.print(col.value + "|");
			}
			System.out.println("");
			row = oper.readOneTuple();
		}
	}

}
