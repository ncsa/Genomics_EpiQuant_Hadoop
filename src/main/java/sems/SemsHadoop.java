package sems;

public class SemsHadoop {
    public static void main(String[] args) throws Exception {
        JobManager jobManager = new JobManager();
        jobManager.run(args);
        System.out.println("Hello World");
        System.exit(0);
    }
}