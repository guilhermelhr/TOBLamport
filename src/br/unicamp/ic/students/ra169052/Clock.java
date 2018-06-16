package br.unicamp.ic.students.ra169052;

public class Clock {
    public Clock(int pid, int value){
        this.pid = pid;
        this.value = value;
    }

    public int value;
    public int pid;

    public synchronized void increment() {
        value++;
    }

    /**
     * Compares two clocks
     * @param a first clock
     * @param b second clock
     * @return the lowest of both clocks
     */
    public static Clock GetLowestClock(Clock a, Clock b){
        if(a.value < b.value){
            return a;
        }

        if(b.value < a.value){
            return b;
        }

        //clocks are tied. judge by lowest pid
        if(a.pid < b.pid){
            return a;
        }else{
            return b;
        }
    }
}
