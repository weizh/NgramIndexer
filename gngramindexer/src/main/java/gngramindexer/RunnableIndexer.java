package gngramindexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;

public class RunnableIndexer implements Callable<Boolean> {

  ConcurrentLinkedQueue<String> files;

  IndexWriter writer;

  RunnableIndexer(ConcurrentLinkedQueue<String> files, IndexWriter iw) {
    this.files = files;
    this.writer = iw;
  }

  public Boolean call() {

    String filename = files.poll();
    if (filename == null)
      return new Boolean(false);
    if (filename.startsWith("googlebooks-eng-all-5gram-20120701") || filename.endsWith("_"))
      return new Boolean(false);
    System.out.println("Thread running file " + filename);
    Document d = new Document();
    StringField[] ns = new StringField[5];

    ns[0] = new StringField("n1", "", Field.Store.YES);
    ns[1] = new StringField("n2", "", Field.Store.YES);
    ns[2] = new StringField("n3", "", Field.Store.YES);
    ns[3] = new StringField("n4", "", Field.Store.YES);
    ns[4] = new StringField("n5", "", Field.Store.YES);

    StringField[] ps = new StringField[5];
    ps[0] = new StringField("p1", "", Field.Store.YES);
    ps[1] = new StringField("p2", "", Field.Store.YES);
    ps[2] = new StringField("p3", "", Field.Store.YES);
    ps[3] = new StringField("p4", "", Field.Store.YES);
    ps[4] = new StringField("p5", "", Field.Store.YES);

    IntField freq = new IntField("freq", 0, Field.Store.YES);
    d.add(ns[0]);
    d.add(ns[1]);
    d.add(ns[2]);
    d.add(ns[3]);
    d.add(ns[4]);

    d.add(ps[0]);
    d.add(ps[1]);
    d.add(ps[2]);
    d.add(ps[3]);
    d.add(ps[4]);

    d.add(freq);

    BufferedReader br = null;

    try {
      br = new BufferedReader(new FileReader(new File(filename)));
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      System.err.println("File not opened in Runner" + filename);
      ;
    }
    String line = "";
    String temp = "";
    int temptotal = 0;

    try {
      line = br.readLine();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.err.println("read first line error!");
    }
    String[] toks = line.split("\t");
    temp = toks[0];
    temptotal = Integer.parseInt(toks[2]);

    int i = 0;

    try {
      while ((line = br.readLine()) != null) {
        toks = line.split("\t");
        if (temp.equals(toks[0])) {
          temptotal += Integer.parseInt(toks[2]);
        } else {

          // create a new document, and write it to the index with indexwriter.
          if (toks[0].contains("_") == false)
            continue;
          String[] tok = toks[0].split(" ");
          for (i = 0; i < 5; i++) {
            if (tok[i].contains("_")) {
              String[] atom = tok[i].split("_");
              ns[i].setStringValue(atom[0]);
              if (atom.length > 1)
                ps[i].setStringValue(atom[1]);
            } else {
              ns[i].setStringValue(tok[i]);
              ps[i].setStringValue("");

            }
          }
          freq.setIntValue(temptotal);

          writer.addDocument(d);

          temptotal = Integer.parseInt(toks[2]);
          temp = toks[0];

        }
      }
    } catch (NumberFormatException e) {
      // TODO Auto-generated catch block
      System.err.println("Parsing number error!");
      ;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.err.println("Can not read line!");
      ;
    }
    if (br == null) {
      System.out.println("Error opening file " + filename);
      return new Boolean(false);
    } else {
      System.out.println("Indexing finished: " + filename);
      return new Boolean(true);
    }

  }

}
