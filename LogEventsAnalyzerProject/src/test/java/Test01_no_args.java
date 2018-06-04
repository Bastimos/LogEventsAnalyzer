import static org.junit.Assert.*;

import org.junit.Test;

public class Test01_no_args {

	@Test
	public void testEventsProccessorService1() {
		EventsProccessorService epa = null;
		try {
			epa = new EventsProccessorService("db_file" , new String[0]);
		} catch(Exception e) {
			e.printStackTrace();
		}
		assertNotNull(epa);
		
	}
	
	@Test
	public void testEventsProccessorService2() {
		EventsProccessorService epa = null;
		try {
			epa = new EventsProccessorService("db_file" , new String[] {""});
		} catch(Exception e) {
			//e.printStackTrace();
		}
		assertNotNull(epa);
	}
}
