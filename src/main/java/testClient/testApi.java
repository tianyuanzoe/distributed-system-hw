package testClient;

import Event.AddLiftRideEvent;
import EventGenerator.LiftRideGenerator;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;

import java.sql.Timestamp;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author zoetian
 * @create 2/1/24
 */
public class testApi {
    private static final String BASE_PATH = "http://34.211.231.123:8080/skierApp";
    private static final int NUM_OF_REQUESTS = 10000;

    private static final int RETRY_LIMIT = 5;
    private static final int NUM_OF_LIFT_RIDE = 10000;
    private final static AtomicInteger success_request= new AtomicInteger();
    private final static AtomicInteger failure_request= new AtomicInteger();
    private static  long total_response_time = 0;







    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<AddLiftRideEvent> blockingQueue = new LinkedBlockingQueue();
        ExecutorService postExecutorService = Executors.newSingleThreadExecutor();
        Timestamp start = new Timestamp(System.currentTimeMillis());
//        single thread : create event
        ExecutorService eventGenerateExecutorService = Executors.newSingleThreadExecutor();
        eventGenerateExecutorService.submit(() -> {
            for (int i = 0; i < NUM_OF_LIFT_RIDE; i++) {
                AddLiftRideEvent liftRideEvent = LiftRideGenerator.generateEvent();
                blockingQueue.offer(liftRideEvent);
            }
        });

//        single-thread : send post request

            postExecutorService.submit(() -> {
                try {
                    sendRequest(blockingQueue);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });




        postExecutorService.shutdown();
        try{
            postExecutorService.awaitTermination(Long.MAX_VALUE,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        eventGenerateExecutorService.shutdown();
        try{
            eventGenerateExecutorService.awaitTermination(Long.MAX_VALUE,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Timestamp end = new Timestamp(System.currentTimeMillis());
        long wallTime = end.getTime() - start.getTime();
        double average_response_time  = total_response_time/NUM_OF_REQUESTS;
        double throughput = (success_request.get())/((wallTime)/1000);
        System.out.println("The average response time for post request is " + average_response_time + " milliseconds");
        System.out.println("The total time for post request is " + wallTime + " milliseconds");
        System.out.println("The total success request is " + success_request.get());
        System.out.println("The total unsuccessful request is " + failure_request.get());
        System.out.println("The total  throughput in requests per second " + throughput);

    }


    private static void sendRequest(BlockingQueue<AddLiftRideEvent> blockingQueue) throws InterruptedException {
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(BASE_PATH);
        SkiersApi skiersApi = new SkiersApi(apiClient);
        for(int i = 0 ; i < NUM_OF_REQUESTS; i++){
            AddLiftRideEvent event = blockingQueue.take();
            int retry_left = RETRY_LIMIT;
            boolean success = false;
            while(retry_left > 0 && !success){
                try {
                    Timestamp start = new Timestamp(System.currentTimeMillis());
                    skiersApi.writeNewLiftRide(event.getLiftRide(), event.getResortID(), event.getSeasonID(),event.getDayID(),event.getSkierID());
                    success = true;
                    Timestamp end = new Timestamp(System.currentTimeMillis());
                    total_response_time += end.getTime() - start.getTime();
                } catch (ApiException e) {
                    System.out.println("Sending post request failed for Add New Life Ride, Retry");
                    retry_left--;
                    failure_request.getAndIncrement();
                }
            }
            if(!success){
                failure_request.getAndIncrement();
                throw new RuntimeException();
            }else{
                success_request.getAndIncrement();
            }

        }





    }
}
