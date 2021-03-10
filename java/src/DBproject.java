/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.function.Predicate;

import javax.sql.rowset.serial.SerialStruct;

//import jdk.javadoc.internal.doclets.formats.html.resources.standard;

import java.util.ArrayList;
/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	

	public static void println(String... lines) {
		for(String s : lines) {
			System.out.println(s);
		}
	}
	public static void println2(String... lines) {
		println(lines);
		println("");
	}

	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}
	public int getNextVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select nextval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				println("MAIN MENU");
				println("---------");
				println("1. Add Ship");
				println("2. Add Captain");
				println("3. Add Cruise");
				println("4. Book Cruise");
				println("5. List number of available seats for a given Cruise.");
				println("6. List total number of repairs per Ship in descending order");
				println("7. Find total number of passengers with a given status");
				println("8. < EXIT");
				
				switch (readChoice()){
					case 1: AddShip(esql); break;
					case 2: AddCaptain(esql); break;
					case 3: AddCruise(esql); break;
					case 4: BookCruise(esql); break;
					case 5: ListNumberOfAvailableSeats(esql); break;
					case 6: ListsTotalNumberOfRepairsPerShip(esql); break;
					case 7: FindPassengersCountWithStatus(esql); break;
					case 8: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					println("Disconnecting from database...");
					esql.cleanup ();
					println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			println("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static int getInt() {
		// returns only if a correct value is given.
		do {
			try { // read the integer, parse it and break.
				int input = Integer.parseInt(in.readLine());
				return input;
			}catch (Exception e) {
				println("Please enter a whole number.");
				continue;
			}//end try
		}while (true);
	}

	public static int getInt(int min, int max) {
		// returns only if a correct value is given.
		do {
			try {
				int input = Integer.parseInt(in.readLine());
				if(input < min) {
					println(String.format("Number must not be lower than %d", min));
				} else if(input > max) {
					println(String.format("Number must not be greater than %d", max));
				} else {
					return input;
				}
			}catch (Exception e) {
				continue;
			}
			println("Please enter a whole number: ");
		}while (true);
	}
	public static int getInt(int min) {
		// returns only if a correct value is given.
		do {
			try {
				int input = Integer.parseInt(in.readLine());
				if(input < min) {
					println(String.format("Number must not be lower than %d", min));
				} else {
					return input;
				}
			}catch (Exception e) {
				continue;
			}
			println("Please enter a whole number: ");
		}while (true);
	}
	public static String getString(int maxLength) {
		do {
			try {
				String line;
				line = in.readLine();
				
				if(line.length() == 0) {
					println(String.format("String must not be empty.", maxLength));
				} else if(line.length() > maxLength) {
					println(String.format("String must not be longer than %d characters.", maxLength));
				} else {
					return line;
				}
				println("Please enter a string: ");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} while(true);
	}

	public static void AddShip(DBproject esql) {//1
		/*
	id INTEGER NOT NULL,
	make CHAR(32) NOT NULL,
	model CHAR(64) NOT NULL,
	age _YEAR_1970 NOT NULL,
	seats _SEATS NOT NULL,
	PRIMARY KEY (id)
		*/
		println2("Enter ship's make (1 - 32 characters): ");
		String make = getString(32);
		println2("Enter ship's model (1 - 64 characters): ");
		String model = getString(64);
		println2("Enter ship's age (at least 0): ");
		int age = getInt(0);
		println2("Enter ship's seats (1 - 499): ");
		int seats = getInt(1, 499);

		try {
			esql.executeUpdate(String.format("INSERT INTO Ship VALUES(%d, %s, %s, %d, %d)",
				esql.getNextVal("ship_seq"),
				make,
				model,
				age,
				seats
			));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void AddCaptain(DBproject esql) {//2
		/*
	id INTEGER NOT NULL,
	fullname CHAR(128),
	nationality CHAR(24),
	PRIMARY KEY (id)
		*/
		println2("Enter captain's full name (1 - 128 characters): ");
		String fullname = getString(128);
		println2("Enter captain's nationality (1 - 24 characters): ");
		String nationality = getString(24);
		try {
			esql.executeUpdate(String.format("INSERT INTO Captain VALUES(%d, %s, %s)",
				esql.getNextVal("captain_seq"),
				fullname,
				nationality
			));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void AddCruise(DBproject esql) {//3
		println2("Enter cost: ");
		int cost = getInt(0);
		int num_sold = 0;
		println2("Enter number of stops: ");
		int num_stops = getInt(0);

		//Not sure if we need to include actual_departure_date and actual_arrival_date
		//println2("Enter actual departure date");
		//println2("Enter actual arrival date");

		println2("Enter arrival port code: ");
		String arrival_port = getString(5);
		println2("Enter departure port code: ");
		String departure_port = getString(5);
		try {
			esql.executeUpdate(String.format("INSERT INTO Cruise VALUES(%d, %d, %d, %d, %s, %s, %s, %s)",
				esql.getNextVal("cruise_seq"),
				cost,
				num_sold,
				num_stops,
				"N/A",
				"N/A",
				arrival_port,
				departure_port
			));
		} catch(Exception e) {
			e.printStackTrace();
		}
/*
	cnum INTEGER NOT NULL,
	cost _PINTEGER NOT NULL,
	num_sold _PZEROINTEGER NOT NULL,
	num_stops _PZEROINTEGER NOT NULL,
	actual_departure_date DATE NOT NULL,
	actual_arrival_date DATE NOT NULL,
	arrival_port CHAR(5) NOT NULL,-- PORT CODE --
	departure_port CHAR(5) NOT NULL,-- PORT CODE --
	PRIMARY KEY (cnum)
	*/


	}


	public static void BookCruise(DBproject esql) {//4
		// Given a customer and a Cruise that he/she wants to book, add a reservation to the DB
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//5

		// For Cruise number and date, find the number of availalbe seats (i.e. total Ship capacity minus booked seats )
	}

	public static void ListsTotalNumberOfRepairsPerShip(DBproject esql) {//6
		// Count number of repairs per Ships and list them in descending order
		try {
			esql.executeQueryAndPrintResult("SELECT * FROM Repairs GROUP BY ship_id ORDER BY COUNT(*)");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static void FindPassengersCountWithStatus(DBproject esql) {//7
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.

		try {
			String line = in.readLine();
			if(line.isEmpty()) {
				esql.executeQueryAndPrintResult("SELECT status, COUNT(*) FROM Reservation GROUP BY status");
			} else {
				char status = line.charAt(0);
				esql.executeQueryAndPrintResult("SELECT COUNT(*) FROM Reservation WHERE status = '" + status + "'");
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}