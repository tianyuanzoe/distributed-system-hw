package clientApi1;

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
public class AddNewLifeRideApi1 {
//    private static final String BASE_PATH = "http://44.232.238.225:8080/skierApp";
//private static final String BASE_PATH = "http://localhost:8080/skierApp";
private static final String BASE_PATH = "http://SkierServerLoadBalancer-1197386842.us-west-2.elb.amazonaws.com/skierApp";

    private static final int NUM_OF_REQUESTS = 1000;

    private static final int NUM_THREADS = 32;
    private static final int RETRY_LIMIT = 5;
    private static final int NUM_OF_LIFT_RIDE = 200000;

    private static final int THREAD_POOL_SIZE = 200;
    private final static AtomicInteger success_request= new AtomicInteger();
    private final static AtomicInteger failure_request= new AtomicInteger();
    private final static CountDownLatch completed = new CountDownLatch(1);







    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<AddLiftRideEvent> blockingQueue = new LinkedBlockingQueue();
        ExecutorService postExecutorService = Executors.newFixedThreadPool(NUM_THREADS);
        Timestamp start = new Timestamp(System.currentTimeMillis());
//        single thread : create event
        ExecutorService eventGenerateExecutorService = Executors.newSingleThreadExecutor();
        eventGenerateExecutorService.submit(() -> {
            for (int i = 0; i < NUM_OF_LIFT_RIDE; i++) {
                AddLiftRideEvent liftRideEvent = LiftRideGenerator.generateEvent();
                blockingQueue.offer(liftRideEvent);
            }
        });

//        multi-thread : send post request
        for(int j = 0 ; j < NUM_THREADS; j++) {
            postExecutorService.submit(() -> {
                try {
                    sendRequest(blockingQueue);
                    completed.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }


//        post the rest of request
        ExecutorService postExecutorService2 = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        try{
            completed.await();
        }catch (InterruptedException e){
            e.printStackTrace();
        }finally {
            for(int m = 0 ; m < THREAD_POOL_SIZE; m++) {
                postExecutorService2.submit(() -> sendRequest2(blockingQueue));
            }
        }

        postExecutorService2.shutdown();
        try {
            postExecutorService2.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
        double throughput = (success_request.get())/((wallTime)/1000);
        System.out.println("The total time for post request is " + wallTime + " milliseconds");
        System.out.println("The total success request is " + success_request.get());
        System.out.println("The total unsuccessful request is " + failure_request.get());
        System.out.println("The total  throughput in requests per second " + throughput);

    }

    private static void sendRequest2(BlockingQueue<AddLiftRideEvent> blockingQueue) {
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(BASE_PATH);
        SkiersApi skiersApi = new SkiersApi(apiClient);
        while (!blockingQueue.isEmpty()){
            AddLiftRideEvent event = blockingQueue.poll();
            int retry_left = RETRY_LIMIT;
            boolean success = false;
            while(retry_left > 0 && !success){
                try {
                    skiersApi.writeNewLiftRide(event.getLiftRide(), event.getResortID(), event.getSeasonID(),event.getDayID(),event.getSkierID());

                    success = true;
                } catch (ApiException e) {
                    System.out.println("Sending post request failed for Add New Life Ride, Retry");
                    retry_left--;
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
                    skiersApi.writeNewLiftRide(event.getLiftRide(), event.getResortID(), event.getSeasonID(),event.getDayID(),event.getSkierID());
                    success = true;
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
