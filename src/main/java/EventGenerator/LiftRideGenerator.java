package EventGenerator;

import Event.AddLiftRideEvent;
import io.swagger.client.model.LiftRide;

import java.util.Random;

/**
 * @author zoetian
 * @create 2/1/24
 */
public class LiftRideGenerator {
    private static final int MIN  = 1;
    private static final int MAX_SKIER_ID = 100000;
    private static final int MAX_RESORT_ID = 100000;
    private static final int MAX_LIFT_ID = 40;
    private static final String SEASON_ID = "2024";
    private static final String DAY_ID = "1";
    private static final int MAX_TIME = 360;
    private static Random random = new Random();



    public static AddLiftRideEvent generateEvent(){
        int skierId = random.nextInt(MIN,MAX_SKIER_ID + 1);
        int resortId = random.nextInt(MIN,MAX_RESORT_ID + 1);
        int liftId = random.nextInt(MIN,MAX_LIFT_ID + 1);
        int time = random.nextInt(MIN,MAX_TIME + 1);
        LiftRide liftRide = new LiftRide();
        liftRide.setLiftID(liftId);
        liftRide.setTime(time);
        return new AddLiftRideEvent(liftRide,resortId,SEASON_ID,DAY_ID,skierId);

    }

}
