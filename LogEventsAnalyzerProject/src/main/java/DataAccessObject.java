import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class DataAccessObject {

	private Connection conn;  
	
	public DataAccessObject(String db_file_name_prefix)  {   

		// Load the HSQL Database Engine JDBC driver
		try {
			Class.forName("org.hsqldb.jdbcDriver");
		} catch (ClassNotFoundException e) {
			EventsProccessorService.log.error("Exception loading database class driver ... "+e);
		}

		// connect to the database.   
		try {
			conn = DriverManager.getConnection("jdbc:hsqldb:"
					+ db_file_name_prefix,    // filenames
					"sa",                     // username
					"");					// password
		} catch (SQLException e) {
			EventsProccessorService.log.error("Exception connecting to database ... "+e);
		}       
		EventsProccessorService.log.info("Events Processor App Constructor - Data Access Object - initialized successfuly with DB file name = "+db_file_name_prefix);
	}
	
	//============================================HSQL methods ======================================
	public void shutdown() {

		Statement st;
		try {
			st = conn.createStatement();
			st.execute("SHUTDOWN");
			conn.close();
		} catch (SQLException e) {
			EventsProccessorService.log.error("Exception shuting down database ... "+e);
		}
	}

	public synchronized void query(String expression) {

		Statement st = null;
		ResultSet rs = null;

		try {
			st = conn.createStatement(); 
			rs = st.executeQuery(expression);
			dump(rs);
			st.close(); 
		} catch (SQLException e) {
			EventsProccessorService.log.error("Exception in database due to query = "+expression+"\n"+e);
		}   
		   
	}

	public synchronized void update(String expression)  {

		Statement st = null;

		try {
			st = conn.createStatement();
			int i = st.executeUpdate(expression);  

			if (i == -1) {
				System.out.println("db error : " + expression);
			}

			st.close();
		} catch (SQLException e) {
			EventsProccessorService.log.error("Exception in database due to update = "+expression+"\n"+e);
		}  

	} 

	public static void dump(ResultSet rs)  {

		ResultSetMetaData meta;
		try {
			meta = rs.getMetaData();
			int colmax = meta.getColumnCount();
			int i;
			Object o = null;
			StringBuilder sb = new StringBuilder();

			for (; rs.next(); ) {
				for (i = 0; i < colmax; ++i) {
					o = rs.getObject(i + 1);  
					sb.append(o.toString() + " ");
					
				}
				EventsProccessorService.log.info(sb.toString()+" - Added to database");
				sb.delete(0, sb.length());
			}
		} catch (SQLException e) {
			EventsProccessorService.log.error("Exception in database while dumping ResultSet = "+rs+"\n"+e);
		}
		
	}                                      
}
