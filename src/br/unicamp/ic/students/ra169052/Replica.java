package br.unicamp.ic.students.ra169052;

import java.util.LinkedList;

public class Replica {
    public static final int CLIENT_PID = -1;
    public static final int DB_SIZE = 32;
    public static final int REPLICAS_COUNT = 5;

    public Network network;
    public Clock clock;

    //matrix of queues, each column is a list of queues for a db value
    private LinkedList<Message>[][] dbQueues;
    private int[] db = new int[DB_SIZE];

    public Replica(){
        dbQueues = new LinkedList[DB_SIZE][];
        for (int i = 0; i < DB_SIZE; i++){
            //every db value has its list of queues
            dbQueues[i] = new LinkedList[REPLICAS_COUNT];
            for (int j = 0; j < REPLICAS_COUNT; j++){
                dbQueues[i][j] = new LinkedList<>();
            }
        }
    }

    /**
     * Checks if any queue is empty
     * @return -1 if no queue is empty, otherwise the pid of the empty queue is returned
     */
    private synchronized int findEmptyQueue(LinkedList<Message>[] queues){
        for (int i = 0; i < queues.length; i++){
            if(queues[i].isEmpty()){
                return i;
            }
        }

        return -1;
    }

    /**
     * Sends a POKE message to every replica with an empty queue
     */
    private void pokeEmptyReplicas(LinkedList<Message>[] queues) {
        int emptyQueuePid;
        while ((emptyQueuePid = findEmptyQueue(queues)) != -1){
            Message message = new Message(clock, Message.Action.POKE);
            network.sendTo(message, emptyQueuePid);
            clock.increment();

            while(true){
                synchronized (queues) {
                    if (!queues[emptyQueuePid].isEmpty()) {
                        break;
                    }
                }
                sleepFor(500);
            }
        }
    }

    /**
     * Checks if all queues are populated
     * @return true if all queues populated, false if at least one queue is empty
     */
    private synchronized boolean allQueuesPopulated(LinkedList<Message>[] queues){
        return findEmptyQueue(queues) == -1;
    }

    /**
     * Finds the PID of the queue that has the lowest clocked head
     * @return pid of lowest head
     */
    private synchronized int findLowestPID(LinkedList<Message>[] queues){
        //assume the lowest clocked head is that of pid 0
        Clock lowestClock = queues[0].get(0).clock;

        //check if any other queue has a lower clocked head and replace
        //lowestClock if there is.
        for (int i = 1; i < queues.length; i++){
            Message head = queues[i].get(0);
            lowestClock = Clock.GetLowestClock(head.clock, lowestClock);
        }

        return lowestClock.pid;
    }

    /**
     * Updates our clock based on another replicas' clock
     * @param remoteClock remote replica clock
     */
    private void updateClock(int remoteClock){
        clock.value = Math.max(clock.value, remoteClock) + 1;
    }

    /**
     * Adds message to the queue specified by it's clock pid
     * @param message
     */
    private void addMessageToQueue(Message message, LinkedList<Message>[] queues){
        synchronized (queues[message.clock.pid]){
            queues[message.clock.pid].add(message);
        }
    }

    /**
     * Starts thread for handling database operations
     */
    public void startDatabaseThread(){
        new Thread(() -> {
            while(true){
                for(LinkedList<Message>[] queues : dbQueues) {
                    //ensure all queues are populated
                    pokeEmptyReplicas(queues);

                    //process messages while no queue is empty
                    while (allQueuesPopulated(queues)) {
                        int pid = findLowestPID(queues);
                        Message message;
                        synchronized (queues[pid]) {
                            message = queues[pid].remove(0);
                        }

                        switch (message.action) {
                            case SREAD:
                            case DREAD:
                                message.action = Message.Action.REPLY;
                                message.payload = db[message.index];
                                network.sendTo(message, message.clock.pid);
                                break;
                            case WRITE:
                                db[message.index] = message.payload;
                                break;
                        }
                    }
                }

                sleepFor(100);
            }
        }).start();
    }

    /**
     * Starts thread for handling incoming messages
     */
    public void startMessengerThread(){
        new Thread(() -> {
            while(true) {
                //try to get a message for this replica
                Message message = network.getMessageFor(clock.pid);
                if(message != null){
                    //check if the message comes from a client or a replica
                    if(message.clock.pid == CLIENT_PID){
                        handleClientMessage(message);
                    }else{
                        handleReplicaMessage(message);
                    }
                }

                sleepFor(500);
            }
        }).start();
    }

    /**
     * Handles a message sent by a client
     * @param message
     */
    private void handleClientMessage(Message message) {
        switch (message.action){
            case SREAD:
                message.clock = clock;
                network.sendTo(message, clock.pid);
                clock.increment();
                break;
            case DREAD:
            case WRITE:
                message.clock = clock;
                network.broadcast(message);
                clock.increment();
                break;
        }
    }

    /**
     * Handles a message sent by a replica
     * @param message
     */
    private void handleReplicaMessage(Message message) {
        switch (message.action){
            case SREAD:
            case DREAD:
            case WRITE:
                updateClock(message.clock.value);
                addMessageToQueue(message, dbQueues[message.index]);
                break;
            case POKE:
                int destination = message.clock.pid;
                updateClock(message.clock.value);
                message.action = Message.Action.ACK;
                message.clock = clock;
                network.sendTo(message, destination);
                break;
            case ACK:
                updateClock(message.clock.value);
                LinkedList<Message> queue = dbQueues[message.index][message.clock.pid];
                synchronized (queue){
                    if(queue.isEmpty()){
                        queue.add(message);
                    }
                }
                break;
            case REPLY:
                updateClock(message.clock.value);
                network.sendTo(message, CLIENT_PID);
                break;
        }
    }

    /**
     * Sleeps for around milis ms
     * @param milis
     */
    private void sleepFor(int milis){
        try {
            Thread.sleep(milis + (int) ((Math.random() - 0.5) * 0.5 * milis));
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {

    }
}
