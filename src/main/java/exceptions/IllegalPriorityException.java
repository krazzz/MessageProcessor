package exceptions;

public class IllegalPriorityException extends RuntimeException{

    public IllegalPriorityException() {
        super("Wrong priority has been setup. It must be > 0 and <=4.");
    }
}
