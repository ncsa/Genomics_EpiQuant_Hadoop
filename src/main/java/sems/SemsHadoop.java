package sems;

public class SemsHadoop {
    public static void main(String[] args) throws Exception {
        AssignJobs jobManager = new AssignJobs();
        jobManager.run(args);
        System.out.println("Hello World");
        System.exit(0);
    }
}