package demo;
/**
 * Author : Sebastian Wizert
 * 
 * Example of Parsing JSON input file using
 *  - fasterxml  jackson parser
 *  - HSQL file DB
 *  - Gradle
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EventsProccessorService {

	private DataAccessObject dao = null;
	private static ArrayList<LogEvent> eventsList = null;
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
		this.calculateEventsDuration2(eventsList);
		// |
		// 6. Display result from data base
		this.showResultFromDB();
		// |
		// 7. Shutdown
		this.shutdownDB();

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
				eventsList = readInJson("resources/TestData.json");
				log.info("TestData file loaded successfuly ... ");
			} else {
				log.info("Arguments detected - Loading file ... ");
				eventsList = readInJson( args[0] );
				log.info(args+" file loaded successfuly ... ");
			}
		} catch ( IOException e ) {
			log.error("Exception reading in JSON file - "+e);
		}

	}

	/** Read In Json - method
	 * 
	 * Reads JSON file using faster xml parsing
	 * and Event object model
	 * Allows missing quotations around field names
	 * Prints to the console entire file
	 * Loads Event list with objects originated in our JSON file
	 * 
	 * @param path - path to the JSON file
	 * @return ArrayList<LogEvent> - list of all log events
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 */
	@SuppressWarnings("deprecation")
	private ArrayList<LogEvent> readInJson(String path) throws FileNotFoundException, IOException {
		ArrayList<LogEvent> eventsList = new ArrayList<LogEvent>();
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = new JsonFactory();
		factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		JsonParser jp = factory.createJsonParser(new FileInputStream(path));

		log.debug(" Printing entire file ...");
		while(jp.nextToken() == JsonToken.START_OBJECT) {
			LogEvent event = mapper.readValue(jp, LogEvent.class);
			log.debug("Event = "+event);
			eventsList.add(event);
		}

		return eventsList;
	}

	/** calculate Events Duration method
	 *  Processes log events list looking for
	 *  events longer than 4 ms
	 *  Converts to map of events sorted by id
	 *  than iterates triggering helper method
	 * 
	 * @param eventsList
	 */
	private void calculateEventsDuration2(ArrayList<LogEvent> eventsList) {

		log.info(" Proccessing file with total of - "+eventsList.size()+" - log elements, looking for events longer than 4ms ....");
		//convert to map by id
		Map<String,Set<LogEvent>> eventsByIdMap = eventsList.parallelStream()
				.collect(Collectors.groupingBy(LogEvent::getId, Collectors.toSet()));
		synchronized (eventsByIdMap) {
			//iterate map
			eventsByIdMap.forEach((String key, Set<LogEvent> eventsSet) -> helper(key, eventsSet) );
		}
	}

	private void helper(String key, Set<LogEvent> eventsSet ) {
		long startTime ,finishTime ,duration = 0;


		for(LogEvent logEvent : eventsSet) {
			if(logEvent.getState().equals("STARTED")) {
				startTime = logEvent.getTimestamp();
				for(LogEvent logEventFinish : eventsSet) {
					if(logEventFinish.getState().equals("FINISHED")) {
						finishTime = logEventFinish.getTimestamp();

						duration = finishTime - startTime;

						if(duration > 4) {

							dao.update(" INSERT INTO long_events_table(id,duration,type,host,alert) VALUES ("
									+ "'" + logEventFinish.getId() + "', "
									+ duration+", "
									+ "'"+logEventFinish.getType()+"',"
									+ " '"+logEventFinish.getHost()+"',"
									+ " 1)");

							log.debug("Proccesed Log Element ID = "+logEventFinish.getId()+" - startTime = "+startTime+" - finishTime = "+finishTime + " Event duration = "+duration);
						}
					}
				}
			}
		}
	}


}  

