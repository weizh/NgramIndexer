package gngramindexer;

public class test {

  public static void main(String arvg[]) {

    for (int i = 'a'; i < 'z' + 1; i++)
      for (int j = 'a'; j < 'z' + 1; j++)
        System.out
                .println("wget http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-5gram-20120701-"
                        + new StringBuilder().append((char)i).append((char)j).toString() + ".gz");
  }
}
