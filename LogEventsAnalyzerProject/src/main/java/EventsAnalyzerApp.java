/**
 * Events Analyzer App
 * @author Sebastian Wizert
 *
 */
public class EventsAnalyzerApp {

	public static void main(String[] args) {
		EventsProccessorService eps = new EventsProccessorService("db_file", args);
	}
}
