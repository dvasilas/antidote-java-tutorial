import java.lang.RuntimeException;
import java.lang.NullPointerException;

import java.util.List;

import java.util.Random;
import java.io.IOException;
import java.io.FileReader;
import java.io.FileWriter;
import java.awt.print.Book;
import java.io.BufferedReader;

import java.net.InetSocketAddress;

import eu.antidotedb.client.*;

public class Tester {

  final private String taskBucket = "Tbuck";
  final private String task01Counter = "Tcounter";
  final private String task01Set = "Tset";

  final private String stateBucket = "tutorial";
  final private int taskNum = 10;
  private String saveObject;
  private AntidoteClient tutorialStateSession1;
  private AntidoteClient tutorialStateSession2;
  private AntidoteClient taskSession;

  public void test() throws IOException {
    saveObject = "save_" + System.getenv("ANTIDOTE_TUTORIAL_SAVE");

    tutorialStateSession1 = new AntidoteClient(new InetSocketAddress("antidote1", 8087));
    tutorialStateSession2 = new AntidoteClient(new InetSocketAddress("antidote2", 8087));

    int state = 0; boolean passed;
    while (state < taskNum) {
      passed = false;
      state = getSavedState();
      switch (state) {
        case 0:
          welcome();
          advanceSavedState();
          System.exit(0);
          break;
        case 1:
          printTask01();
          passed = testTask01();
          break;
        case 2:
          printTask02();
          passed = testTask02();
          break;
        case 3:
          printTask03();
          passed = testTask03();
          break;
        case 4:
          printTask04();
          passed = testTask04();
          break;
        case 5:
          printTask05();
          passed = testTask05();
          break;
        case 6:
          printTask06();
          passed = testTask06();
          break;
        case 7:
          printTask07();
          passed = testTask07();
          break;
        case 8:
          printTask08();
          passed = testTask08();
          break;
        case 9:
          printTask09();
          passed = testTask09();
          break;
        case taskNum:
          finished();
          break;
        default:
          Random rand = new Random(System.currentTimeMillis());
          saveObject = "save_" + Integer.toString(rand.nextInt());
      }
      if (passed) {
        if (state > 0 && state < taskNum)
          printPass();
          advanceSavedState();
      } else {
        pringFail();
        System.exit(0);
      }
    }
  }

  private boolean testTask01() {
    int counter= DemoCommandsExecutor.getCounter(tutorialStateSession1, taskBucket, task01Counter);
    List<String> set = DemoCommandsExecutor.getSet(tutorialStateSession1, taskBucket, task01Set);
    if (counter > 3 && set.size() > 2)
      return true;
    return false;
  }

  private boolean testTask02() {
    int counter1 = DemoCommandsExecutor.getCounter(tutorialStateSession1, taskBucket, task01Counter);
    int counter2 = DemoCommandsExecutor.getCounter(tutorialStateSession2, taskBucket, task01Counter);
    List<String> set1 = DemoCommandsExecutor.getSet(tutorialStateSession1, taskBucket, task01Set);
    List<String> set2 = DemoCommandsExecutor.getSet(tutorialStateSession2, taskBucket, task01Set);
    if (counter1 > counter2 && set1.size() < set2.size())
      return true;
    return false;
  }

  private boolean testTask03() {
    int counter1 = DemoCommandsExecutor.getCounter(tutorialStateSession1, taskBucket, "task02counter");
    int counter2 = DemoCommandsExecutor.getCounter(tutorialStateSession2, taskBucket, "task02counter");
    List<String> set1 = DemoCommandsExecutor.getSet(tutorialStateSession1, taskBucket, "task02set");
    List<String> set2 = DemoCommandsExecutor.getSet(tutorialStateSession2, taskBucket, "task02set");
    if (counter1 == counter2 && set1.size() == set2.size())
      return true;
    return false;
  }

  private boolean testTask04() {
    try {
      taskSession = new BookCommands().connect("antidote1", 8087);
    } catch (java.lang.RuntimeException e) {
      System.out.println(e);
      return false;
    }
    return true;
  }

  private boolean testTask05() {
    BookCommands bookCmds = new BookCommands();
    taskSession = bookCmds.connect("antidote1", 8087);
    try {
      bookCmds.assignToRegister(taskSession, taskBucket, "testRegister", "testValue");
    } catch (Exception e) {
      System.out.println(e);
      return false;
    }
    String regValue = Bucket.bucket(taskBucket).read(taskSession.noTransaction(),  Key.register("testRegister"));
    if (regValue.equals(new String("testValue")))
      return true;
    return false;
  }

  private boolean testTask06() {
    BookCommands bookCmds = new BookCommands();
    taskSession = bookCmds.connect("antidote1", 8087);
    try {
      bookCmds.updateMapRegister(taskSession, taskBucket, "task06map", "key", "value");
    } catch (Exception e) {
      System.out.println(e);
      return false;
    }
    MapKey.MapReadResult mapReadResult = Bucket.bucket(taskBucket).read(taskSession.noTransaction(), Key.map_rr("task06map"));
    if (mapReadResult.get(Key.register("key")).equals("value"))
      return true;
    return false;
  }

  private boolean testTask07() {
    taskSession = new BookCommands().connect("antidote1", 8087);
    try {
      MapKey.MapReadResult userInfo = Bucket.bucket(BookCommands.userBucket).read(taskSession.noTransaction(), Key.map_rr("Bob"));
      if (userInfo.get(BookCommands.emailMapField).equals("bob@mail.com"))
        return true;
      return false;
    } catch (Exception e) {
      System.out.println(e);
      return false;
    }
  }

  private boolean testTask08() {
    taskSession = new BookCommands().connect("antidote1", 8087);
    try {
      MapKey.MapReadResult ownedBooksMapReadResult = Bucket.bucket(BookCommands.userBucket).read(taskSession.noTransaction(), Key.map_rr("Bob"));
      List<String> ownedBooks = ownedBooksMapReadResult.get(BookCommands.ownBooksMapField);
      if (ownedBooks.contains(new String("book1")))
        return true;
      return false;
    } catch(Exception e) {
      System.out.println(e);
      return false;
    }
  }

  private boolean testTask09() {
    taskSession = new BookCommands().connect("antidote1", 8087);
    try {
      MapKey.MapReadResult fromUserReadResult = Bucket.bucket(BookCommands.userBucket).read(taskSession.noTransaction(), Key.map_rr("Bob"));
      List<String> fromUserBooks = fromUserReadResult.get(BookCommands.ownBooksMapField);
      MapKey.MapReadResult toUserReadResult = Bucket.bucket(BookCommands.userBucket).read(taskSession.noTransaction(), Key.map_rr("Alice"));
      List<String> toUserBooks = toUserReadResult.get(BookCommands.borrowedBooksMapField);
      if (!fromUserBooks.contains(new String("book1")) && toUserBooks.contains(new String("book1")))
        return true;
      return false;
    } catch (Exception e) {
      System.out.println(e);
      return false;
    }
  }

  private void welcome() {
    String welcomeMsg = "Welcome to the Antidote Java Tutorial!\n\n" +
      "The aim of this tutorial is to familiarize the user with the basic" +
      "feautures, data modeland API\nof AntidoteDB, and to demonstrate" +
      "the use of AntidoteDB as backend database.\n" +
      "Tasks 1 - 3 introduce the data model of AntidoteDB, and demonstrate that" +
      "the database remains\navailable under network partitions and the state of" +
      "replicas converges when connectivity is restored.\n" +
      "Tasks 4 - 6 introduce the java AntidoteDB client interface.\n" +
      "Tasks 7 - 10 demonstrate how to use AntioteDB as a backend database by" +
      "building a simple Bookstore\napplication.\n" +
      "============================\n\n" +
      "Run this executable again to proceed to the first task.\n";

    System.out.println(welcomeMsg);
  }


  private void printTask01() {
    String msg = "Task 01 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      "Open two new shells. In both shells nagivate to antidote-java-tutorial/setup/\n" +
      "In the first shell start the first demo application:\n> ./app1.sh\n" +
      "and then connect the application to replica 1 of AntidoteDB\n" +
      "bookstore@antidote1> connect antidote1 8087\n\n" +
      "Follow the same steps for the second application:\n" +
      "> ./app2.sh\nbookstore@antidote2> connect antidote2 8087\n\n" +
      "This demo application is a small shell that uses AntidoteDB as a backend database. Type:\n" +
      "nbookstore@antidote> ?l\nto see the list of available commands, and:\n" +
      "bookstore@antidote> ?help <some_command>\nto see details about a command\n\n" +
      "To pass this task the counter \"" + task01Counter + "\" needs to have a value greater that 3 " +
      "and the set \"" + task01Set + "\" needs to have at least 3 elements,\n" +
      "both in bucket \"" + taskBucket + "\".\n" +
      "You can try update the same objects in both shells to see how AntidoteDB replicates udpate between\nreplicas.\n" +
      "================================\n";
    System.out.println(msg);
  }

  private void printTask02() {
    String msg = "Task 02 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      //TODO: complete this description
      "# in setup/\n./disconnect.sh\n" +
      "To pass this task the counter \"" + task01Counter + "\" needs to have a lower value in bookstore@antidote1 than\n" +
      "in bookstore@antidote2 and the set \"" + task01Set + "\" needs to have a greater value in bookstore@antidote1 than\n" +
      "in bookstore@antidote (both in bucket \"" + taskBucket + "\").\n" +
      "================================\n";
    System.out.println(msg);
  }

  private void printTask03() {
    String msg = "Task 03 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      //TODO: complete this description
      "just run ./connect.sh\n" +
      "================================\n";
    System.out.println(msg);
  }

  private void printTask04() {
    String msg = "Task 04 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      //TODO: complete this description
      "Implement the connect() methond in BookCommands.java.\n" +
      "================================\n";
    System.out.println(msg);
  }

  private void printTask05() {
    String msg = "Task 05 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      //TODO: complete this description
      "Implement the assignToRegister() methond in BookCommands.java.\n" +
      "================================\n"
      ;
    System.out.println(msg);
  }

  private void printTask06() {
    String msg = "Task 06 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      //TODO: complete this description
      "Implement the updateMapRegister() methond in BookCommands.java.\n" +
      "================================\n";
    System.out.println(msg);
  }

  private void printTask07() {
    String msg = "Task 07 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      // TODO: complete this description
      "Implement the addUser() methond in BookCommands.java\n" +
      "And use the adduser command to add a user with username \"Bob\" and emai bob@mail.com in the shell.\n"
      + "================================\n";
    System.out.println(msg);
  }

  private void printTask08() {
    String msg = "Task 07 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      // TODO: complete this description
      "Implement the addOwnedBooks() methond in BookCommands.java\n"
      + "And use the ownbook command to add a book \"book1\" to user \"Bob\".\n"
      + "================================\n";
    System.out.println(msg);
  }

  private void printTask09() {
    String msg = "Task 09 of the AntidoteDB Java Tutorial\n" +
      "--------------------------------\n" +
      // TODO: complete this description
      "Implement the borrowBook() methond in BookCommands.java\n" +
      "And use the borrowbook command to indicate that user \"Alice\" borrows \"book1\" from user \"Bob\".\n"
      + "================================\n";
    System.out.println(msg);
  }

  private void printPass() { System.out.println("TASK PASSED\n"); }

  private void pringFail() {
    System.out.println("TASK FAILED\nYou can retry by running this executable again.\n\n"); }

  private void finished() {
    System.out.println("You have completed the AntidoteDB Java Tutorial. Well done!");
    System.exit(0);
  }

  private int getSavedState() {
    return DemoCommandsExecutor.getCounter(tutorialStateSession1, stateBucket, saveObject);
  }
  private void advanceSavedState() {
    DemoCommandsExecutor.incCounter(tutorialStateSession1, stateBucket, saveObject);
  }
}
