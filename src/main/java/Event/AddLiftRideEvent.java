package Event;

import io.swagger.client.model.LiftRide;

/**
 * @author zoetian
 * @create 2/1/24
 */
public class AddLiftRideEvent {
    LiftRide liftRide;
    Integer resortID;
    String seasonID;
    String dayID;
    Integer skierID;

    public AddLiftRideEvent() {
    }

    public AddLiftRideEvent(LiftRide liftRide, Integer resortID, String seasonID, String dayID, Integer skierID) {
        this.liftRide = liftRide;
        this.resortID = resortID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.skierID = skierID;
    }

    public LiftRide getLiftRide() {
        return liftRide;
    }

    public Integer getResortID() {
        return resortID;
    }

    public String getSeasonID() {
        return seasonID;
    }

    public String getDayID() {
        return dayID;
    }

    public Integer getSkierID() {
        return skierID;
    }

    public void setLiftRide(LiftRide liftRide) {
        this.liftRide = liftRide;
    }

    public void setResortID(Integer resortID) {
        this.resortID = resortID;
    }

    public void setSeasonID(String seasonID) {
        this.seasonID = seasonID;
    }

    public void setDayID(String dayID) {
        this.dayID = dayID;
    }

    public void setSkierID(Integer skierID) {
        this.skierID = skierID;
    }
}
