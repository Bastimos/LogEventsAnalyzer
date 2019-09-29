package demo;
/**
 * JSON data to file  generator
 * 
 * @author Sebastian Wizert
 *
 */

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Random;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class DataGenerator {
	
	protected final static org.apache.log4j.Logger log = Logger.getLogger( DataGenerator.class);

	public DataGenerator(int limit, String fileName) {
		super();
		
		try {
			generateData( limit , fileName );
		} catch (IOException e) {
			log.error("Exception generating data ...", e);
		}
		log.info("Generated mock data file worth of = "+readableFileSize( new File(fileName).length()) );
	}

	private void generateData(int limit, String fileName) throws IOException {
		
		JsonFactory factory = new JsonFactory();
		JsonGenerator generator = factory.createGenerator(
		    new File(fileName), JsonEncoding.UTF8);
		Random random = new Random();

		for(int i = 0; i < limit; ++i) {
			
			generator.writeStartObject();
			generator.writeNumberField("id", i);
			generator.writeStringField("state", "STARTED");
			generator.writeStringField("host", "host_"+i);
			generator.writeStringField("type", "type_"+i);
			generator.writeNumberField("timestamp", new Timestamp(System.currentTimeMillis()).getTime() );
			generator.writeEndObject();
			
			generator.writeRaw('\n');

			generator.writeStartObject();
			generator.writeNumberField("id", i);
			generator.writeStringField("state", "FINISHED");
			generator.writeStringField("host", "host_"+i);
			generator.writeStringField("type", "type_"+i);
			generator.writeNumberField("timestamp", new Timestamp(System.currentTimeMillis()).getTime() + (random.nextInt(9) + 1)  );
			generator.writeEndObject();
			generator.writeRaw('\n');
		}
		generator.close();
	}

	public static String readableFileSize(long size) {
	    if(size <= 0) return "0";
	    final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
	    int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
	    return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
	}
}
