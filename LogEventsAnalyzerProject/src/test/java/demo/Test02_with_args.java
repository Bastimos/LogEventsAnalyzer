package demo;
import static org.junit.Assert.*;

import org.junit.Test;

import demo.EventsProccessorService;

public class Test02_with_args {

	@Test
	public void testEventsProccessorService() {
		EventsProccessorService epa = null;
		try {
			epa = new EventsProccessorService("db_file" , new String[] {"resources/GeneratedTestData.json"});
		} catch(Exception e) {
			e.printStackTrace();
		}
		assertNotNull(epa);
	}

}
