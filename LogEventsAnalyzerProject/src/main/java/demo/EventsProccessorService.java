package demo;
/**
 * Author : Sebastian Wizert
 * 
 * Example of Parsing JSON input file using
 *  - fasterxml  jackson parser
 *  - HSQL file DB
 *  - Gradle
 */

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class EventsProccessorService {

	private DataAccessObject dao = null;
	private ArrayList<LogEvent> eventsList = null;
	private List<Set<LogEvent>> resultValues = null;
	private final org.apache.log4j.Logger log = Logger.getLogger(EventsProccessorService.class);
	
	/**
	 * 1.initialize application object with specified database parameter
	 * 2.Removes database residue from possible previous runs
	 * 3.Creates empty table within the database
	 * 4.Reads in JSON file either using args parameter or uses its own backup test copy
	 * 5.Calculates events duration using JSON extracted data and - if needed - for events
	 * longer than 4 ms - writes entries into HSQL database with alert value marked true
	 * 6.Displays resulting rows to the console
	 * 7.Database shutdown 
	 * @param args
	 * @param dbFileName 
	 */
	public EventsProccessorService( String dbFileName, String[] args ) {
		//configure internal log4j logger
		PropertyConfigurator.configure("log4j.properties");
		float start, end, executionTime;
		start = new Timestamp(System.currentTimeMillis()).getTime();

		// 1.Initialize DB
		this.dao = new DataAccessObject( dbFileName );
		// |
		// 2.clean database for fresh results 
		this.cleanDB();
		// |
		// 3.create empty table
		this.createEmptyTable();
		// |
		// 4.read in JSON
		this.readInJSONfileOrUseDefault(args);
		// |
		// 5.process JSON
		this.calculateEventsDuration(eventsList);
		// |
		// 6
		this.insertResultIntoToDb(resultValues);
		// |
		// 7. Display result from data base
		this.showResultFromDB();
		// |
		// 8. Shutdown
		this.shutdownDB();
		
		end = new Timestamp(System.currentTimeMillis()).getTime();
		executionTime = end - start;
		log.info("Finished in "+executionTime);
	} // main method ends

	// ================================ data access object helper methods ==============================

	/**
	 * Clean Database helper - data access object method
	 */
	private void cleanDB()  {

		dao.update("DROP TABLE IF EXISTS long_events_table");
		log.info("Successfuly cleaned database residue ... ");
	}

	/**
	 * Create Empty Table - helper data access object method
	 */
	private void createEmptyTable() {

		dao.update("CREATE TABLE long_events_table ( "
				+ "id VARCHAR(256), "
				+ "duration INTEGER, "
				+ "type VARCHAR(256),"
				+ "host VARCHAR(256),"
				+ "alert BIT)");

		log.info("Successfuly created empty table for further work ... ");
	}

	/**
	 * Show Result From DB - helper data access object method
	 */
	private void showResultFromDB()  {
		dao.query("SELECT * FROM long_events_table");
		log.info("Database query OK ...");;
	}

	/**
	 * Shut Down DB - helper data access object method
	 */
	private void shutdownDB() {
		dao.shutdown();	
		log.info("Database shutting down ... ");
	}

	// ================================ end of dao methods ================================

	/** Read In JSON file Or Use Default- method
	 * 
	 * Support method handles exceptions
	 * 
	 * @param args
	 */
	private void readInJSONfileOrUseDefault(String[] args) {
		try {
			if( args.length <= 0 || Arrays.toString(args).length() <= 3 ) {
				log.info("No arguments detected - Loading default test file ... ");
				eventsList = JsonReader.readInJson("resources/TestData.json");
				log.info("TestData file loaded successfuly ... ");
			} else {
				log.info("Arguments detected - Loading file ... ");
				eventsList = JsonReader.readInJson( args[0] );
				log.info(args[0]+" file loaded successfuly ... ");
			}
		} catch ( IOException e ) {
			log.error("Exception reading in JSON file ", e);
		}

	}

	/** calculate Events Duration method
	 *  Processes log events list looking for
	 *  events longer than 4 ms
	 * 
	 * @param eventsList
	 */
	private void calculateEventsDuration(ArrayList<LogEvent> eventsList) {

		log.info(" Proccessing file with total of - "+eventsList.size()+" - log elements, looking for events longer than 4ms ....");
	 
		resultValues = eventsList.parallelStream()
			.collect(Collectors.groupingBy(LogEvent::getId, Collectors.toSet()))
			.values().parallelStream().filter(x -> helper(x)).collect(Collectors.toList());

	}
	
	private boolean helper(Set<LogEvent> eventsSet ) {
		long startTime ,finishTime ,duration = 0;


		for(LogEvent logEvent : eventsSet) {
			if(logEvent.getState().equals("STARTED")) {
				startTime = logEvent.getTimestamp();
				for(LogEvent logEventFinish : eventsSet) {
					if(logEventFinish.getState().equals("FINISHED")) {
						finishTime = logEventFinish.getTimestamp();

						duration = finishTime - startTime;
						logEventFinish.setDuration(duration);
						log.debug("Proccesed Log Element ID = "+logEventFinish.getId()+" - startTime = "+
								startTime+" - finishTime = "+finishTime + " Event duration = "+duration);
						if(duration > 4) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Based on inputing list of sets of LogEvent objects
	 * Creates a query String for execution further in data access object
	 * 
	 * @param logEvents
	 */
	private void insertResultIntoToDb (List<Set<LogEvent>> logEvents) {

		StringBuilder sb = new StringBuilder();
		sb.append(" INSERT INTO long_events_table(id,duration,type,host,alert) VALUES ");
		logEvents.stream().forEach(logEventSet -> {
			logEventSet.stream().forEach(logEvent -> {
				sb.append("('" + logEvent.getId() + "', "
				+ logEvent.getDuration()+", "
				+ "'"+logEvent.getType()+"',"
				+ " '"+logEvent.getHost()+"',"
				+ " 1),");
				
			});
		});
		
		String query = sb.toString();
		query = query.substring(0, query.length()-1);
		dao.update(query);
		
	}
}  

