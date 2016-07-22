package Slb.Cfs.Mrb;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class EventProcessDataflowHelper {
	
	static ArrayList<String> GetBillableEventList() {
		ArrayList<String> EVENTNAMES = new ArrayList<String>(Arrays.asList("StratigraphyStarted",
				"StratigraphyEnded", "WellTrajectoryUpdateStarted", "WellTrajectoryUpdateEnded",
				// use the Reservoir... as Duration event
				"ReservoirPropertyDistributionStarted", "ReservoirPropertyDistributionEnded", "FinalizeMudStarted",
				"FinalizeMudEnded", "ProductionHistoryStarted", "ProductionHistoryEnded", "StructureStarted",
				"StructureEnded", "WorkflowUpgrade"));
		return EVENTNAMES;
	}
	
	static double CalculateCostForEventGroup(String sid, String ename, String wname) {
		return 1050;
	}
	
	static String GetBillingCycleIdForEvent(String sid, String ename, String wname, Long timestamp)	{
		String result = "07-2016";
		Date date = new Date(timestamp);
		Format fmt = new SimpleDateFormat("MM-yyyy");
		return fmt.format(date);
	}

}
