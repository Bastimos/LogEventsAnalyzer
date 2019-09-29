package demo;
/**
 * Events Analyzer App
 * @author Sebastian Wizert
 *
 */
public class EventsAnalyzerApp {

	public static void main(String[] args) {
		
		EventsAnalyzerApp eaa = new EventsAnalyzerApp();
		eaa.start(args);
	}
	
	private void start(String[] args) {
		new EventsProccessorService("db_file", args);
	}
}
