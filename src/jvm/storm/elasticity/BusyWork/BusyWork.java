package storm.elasticity.BusyWork;

public class BusyWork {

	public static void doWork(int num) {
		double calc=0.0;
		  for(double i=0; i<num; i++) {
			  calc+=i/3.0*4.5+1.3;
		  }
	}
}
