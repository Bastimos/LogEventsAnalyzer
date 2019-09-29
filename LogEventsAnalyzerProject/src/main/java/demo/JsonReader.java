package demo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonReader {
	
	private final static org.apache.log4j.Logger log = Logger.getLogger(JsonReader.class);

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
	public static ArrayList<LogEvent> readInJson(String path) throws FileNotFoundException, IOException {
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
}
