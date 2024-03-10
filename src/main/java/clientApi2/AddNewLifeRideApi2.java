package clientApi2;

import Event.AddLiftRideEvent;
import EventGenerator.LiftRideGenerator;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author zoetian
 * @create 2/1/24
 */
public class AddNewLifeRideApi2 {
    private static final String BASE_PATH = "http://44.232.238.225:8080/skierApp";
    private static final int NUM_OF_REQUESTS = 1000;

    private static final int NUM_THREADS = 32;
    private static final int RETRY_LIMIT = 5;
    private static final int NUM_OF_LIFT_RIDE = 200000;
    private final static AtomicInteger success_request= new AtomicInteger();
    private final static AtomicInteger failure_request= new AtomicInteger();
    private final static CountDownLatch completed = new CountDownLatch(1);
    private final static BlockingQueue<String> infoQueue = new LinkedBlockingQueue<>();

    private static CountDownLatch numOfInfo = new CountDownLatch(NUM_OF_LIFT_RIDE);

    private static BlockingQueue<Long> latencyQueue = new LinkedBlockingQueue();




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
        ExecutorService postExecutorService2 = Executors.newFixedThreadPool(200);
        try{
            completed.await();
        }catch (InterruptedException e){
            e.printStackTrace();
        }finally {
            for(int m = 0 ; m < 200; m++) {
                postExecutorService2.submit(() -> sendRequest2(blockingQueue));
            }
        }
//         write the String in infoQueue to a file(info.csv)
        ExecutorService writeExecutorService = Executors.newSingleThreadExecutor();
        writeExecutorService.submit(() -> {
            try {
                writeToFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

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

        writeExecutorService.shutdown();
        try {
            writeExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Timestamp end = new Timestamp(System.currentTimeMillis());
        long wallTime = end.getTime() - start.getTime();
        double throughput = (success_request.get())/((wallTime)/1000);
        long maxLatency = calculateLatency()[0];
        long minLatency = calculateLatency()[1];
        long avgLatency = calculateLatency()[2];
        long median = calculateLatency()[3];
        long p99 = calculateLatency()[4];
        System.out.println("The max latency is " + maxLatency + " milliseconds");
        System.out.println("The min latency is " + minLatency + " milliseconds");
        System.out.println("The avg latency is " + avgLatency + " milliseconds");
        System.out.println("The median latency is " + median + " milliseconds");
        System.out.println("The p99  is " + p99 + " milliseconds");
        System.out.println("The total time for post request is " + wallTime + " milliseconds");
        System.out.println("The total success request is " + success_request.get());
        System.out.println("The total unsuccessful request is " + failure_request.get());
        System.out.println("The total  throughput in requests per second " + throughput);

    }

    private static long[] calculateLatency() {
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        long sum = 0;
        long[] toArray = latencyQueue.stream().mapToLong(l -> l).toArray();
        Arrays.sort(toArray);
        for(long latency:latencyQueue){
            max = Math.max(max,latency);
            min = Math.min(min,latency);
            sum += latency;
        }
        long avg = sum/latencyQueue.size();
        long median = toArray[toArray.length/2];
        long p99 = toArray[(int) (Math.ceil(toArray.length * 0.99))];
        return new long[]{max,min,avg,median,p99};

    }

    private static void writeToFile() throws IOException, InterruptedException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("requestInfo.csv"));
        while(!infoQueue.isEmpty() || numOfInfo.getCount() > 0){
            String info = infoQueue.take();
            if(info != null){
                writer.write(info);
                numOfInfo.countDown();
            }
        }
        writer.close();
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
                    Timestamp start = new Timestamp(System.currentTimeMillis());
                    ApiResponse<Void> response =  skiersApi.writeNewLiftRideWithHttpInfo(event.getLiftRide(), event.getResortID(), event.getSeasonID(),event.getDayID(),event.getSkierID());
                    Timestamp end = new Timestamp(System.currentTimeMillis());
                    long latency = end.getTime() - start.getTime();
                    long throughput = 1000/latency;
                    String info = start.getTime()+ "," + "POST," + latency + "," +response.getStatusCode() + "," + throughput + "\n";
                    latencyQueue.offer(latency);
                    infoQueue.offer(info);
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
                    Timestamp start = new Timestamp(System.currentTimeMillis());
                    ApiResponse<Void> response =  skiersApi.writeNewLiftRideWithHttpInfo(event.getLiftRide(), event.getResortID(), event.getSeasonID(),event.getDayID(),event.getSkierID());
                    Timestamp end = new Timestamp(System.currentTimeMillis());
                    long latency = end.getTime() - start.getTime();
                    long throughput = 1000/latency;
                    String info = start.getTime()+ "," + "POST," + latency + "," +response.getStatusCode() + "," + throughput + "\n";
                    latencyQueue.offer(latency);
                    infoQueue.offer(info);
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
