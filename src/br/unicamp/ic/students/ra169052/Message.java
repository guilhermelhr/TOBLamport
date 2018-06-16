package br.unicamp.ic.students.ra169052;

public class Message {
    public enum Action {
        DREAD, SREAD, WRITE, REPLY, POKE, ACK
    }

    public Clock clock;
    public Action action;
    public int payload;

    public Message(Clock clock, Action action){
        this.clock = clock;
        this.action = action;
    }
}
