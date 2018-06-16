package br.unicamp.ic.students.ra169052;

import java.util.ArrayList;
import java.util.LinkedList;

public class Network {
    private ArrayList<Replica> peers = new ArrayList<>();
    private ArrayList<LinkedList<Message>> messageQueues;

    public void addPeer(Replica replica){
        replica.network = this;
        replica.clock = new Clock(peers.size(), 0);
        messageQueues.add(new LinkedList<>());
        peers.add(replica);
    }

    /**
     * Sends a message to specified replica
     * @param message
     * @param pid destination identifier
     */
    public void sendTo(Message message, int pid){
        if(pid == Replica.CLIENT_PID){
            System.out.println("Client received " + message.payload + " from pid " + message.clock.pid);
            return;
        }

        LinkedList<Message> messageQueue = messageQueues.get(pid);
        synchronized (messageQueue){
            messageQueue.add(message);
        }
    }

    /**
     * Gets message for a replica
     * @param pid replica's pid
     * @return oldest message if there is one, null otherwise
     */
    public Message getMessageFor(int pid) {
        LinkedList<Message> messageQueue = messageQueues.get(pid);
        synchronized (messageQueue) {
            if (!messageQueue.isEmpty()) {
                return messageQueue.removeFirst();
            }else{
                return null;
            }
        }
    }

    /**
     * Sends message to every peer on the network
     * @param message
     */
    public synchronized void broadcast(Message message) {
        for(LinkedList<Message> messageQueue : messageQueues){
            messageQueue.add(message);
        }
    }
}
