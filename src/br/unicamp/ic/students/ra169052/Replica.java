package br.unicamp.ic.students.ra169052;

import java.util.ArrayList;
import java.util.LinkedList;

public class Replica {
    public static final int CLIENT_PID = -1;

    public Network network;
    public Clock clock;

    private LinkedList<Message>[] queues;
    private int db = Integer.MAX_VALUE;

    /**
     * Checks if any queue is empty
     * @return -1 if no queue is empty, otherwise the pid of the empty queue is returned
     */
    private synchronized int findEmptyQueue(){
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
    private void pokeEmptyReplicas() throws InterruptedException {
        int emptyQueuePid;
        while ((emptyQueuePid = findEmptyQueue()) != -1){
            Message message = new Message(clock, Message.Action.POKE);
            network.sendTo(message, emptyQueuePid);
            clock.increment();

            while(true){
                synchronized (queues) {
                    if (!queues[emptyQueuePid].isEmpty()) {
                        break;
                    }
                }
                Thread.sleep(500);
            }
        }
    }

    /**
     * Checks if all queues are populated
     * @return true if all queues populated, false if at least one queue is empty
     */
    private synchronized boolean allQueuesPopulated(){
        return findEmptyQueue() == -1;
    }

    /**
     * Finds the PID of the queue that has the lowest clocked head
     * @return pid of lowest head
     */
    private synchronized int findLowestPID(){
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

    private void addMessageToQueue(Message message){
        synchronized (queues[message.clock.pid]){
            queues[message.clock.pid].add(message);
        }
    }

    public void startDatabaseThread(){
        new Thread(() -> {
            while(true){
                //ensure all queues are populated
                try {
                    pokeEmptyReplicas();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                //process messages while no queue is empty
                while(allQueuesPopulated()){
                    int pid = findLowestPID();
                    Message message;
                    synchronized (queues[pid]) {
                        message = queues[pid].remove(0);
                    }

                    switch (message.action){
                        case SREAD:
                        case DREAD:
                            message.action = Message.Action.REPLY;
                            message.payload = db;
                            network.sendTo(message, message.clock.pid);
                            break;
                        case WRITE:
                            db = message.payload;
                            break;
                    }
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }).start();
    }

    public void startMessengerThread(){
        new Thread(() -> {
            while(true) {
                Message message = network.getMessageFor(clock.pid);
                if(message != null){
                    if(message.clock.pid == CLIENT_PID){
                        handleClientMessage(message);
                    }else{
                        handleReplicaMessage(message);
                    }
                }
            }
        }).start();
    }

    private void handleClientMessage(Message message) {
        switch (message.action){
            case SREAD:
                message.clock = clock;
                network.sendTo(message, clock.pid);
                clock.increment();
                break;
            case DREAD:
                message.clock = clock;
                network.broadcast(message);
                clock.increment();
                break;
            case WRITE:
                message.clock = clock;
                network.broadcast(message);
                clock.increment();
                break;
        }
    }

    private void handleReplicaMessage(Message message) {
        switch (message.action){
            case SREAD:
            case DREAD:
            case WRITE:
                updateClock(message.clock.value);
                addMessageToQueue(message);
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
                LinkedList<Message> queue = queues[message.clock.pid];
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



    public static void main(String[] args) {
	// write your code here
    }
}
