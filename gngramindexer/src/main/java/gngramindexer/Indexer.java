package gngramindexer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

public class Indexer {

  // argv[0] to notify files to index
  // argv[1] to notify where to store the files
  public static void main(String argv[]) {

	  argv[0] = "src/main/resources/";
	  argv[1] = "index";
    final File folder = new File(argv[0]);

    ConcurrentLinkedQueue<String> files = listFilesForFolder(folder);

    int filesize = files.size();

    IndexWriter writer = getIndexWriter(argv[1], 6000);
    if (writer == null)
      System.err.println("Writer creation error!");

    ExecutorService es = Executors.newFixedThreadPool(filesize);

    Collection<Callable<Boolean>> calls = new ArrayList<Callable<Boolean>>();
    for (int i = 0; i < filesize; i++) {
      calls.add(new RunnableIndexer(files, writer));
    }
    List<Future<Boolean>> f = null;

    try {
      f = es.invokeAll(calls);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    es.shutdown();

    boolean finshed=false;
    try {
      finshed = es.awaitTermination(1000000, TimeUnit.MINUTES);
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    if (finshed) {
      try {
        writer.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public static IndexWriter getIndexWriter(String indexdirectory, double buffersize) {
    Directory dir = null;
    if (OSUtil.isWindows())
      try {
        dir = FSDirectory.open(new File(indexdirectory));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    else
      try {
        dir = NIOFSDirectory.open(new File(indexdirectory));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);

    config.setOpenMode(OpenMode.CREATE_OR_APPEND);
    config.setRAMBufferSizeMB(buffersize);
    LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    mergePolicy.setMergeFactor(3);
    config.setMergePolicy(mergePolicy);

    IndexWriter writer = null;
    try {
      writer = new IndexWriter(dir, config);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return writer;
  }

  public static ConcurrentLinkedQueue<String> listFilesForFolder(final File folder) {
    ConcurrentLinkedQueue<String> files = new ConcurrentLinkedQueue<String>();
    if (folder.listFiles()==null){
    	files.add(folder.getAbsolutePath());
    	return files;
    }
    for (final File fileEntry : folder.listFiles()) {
      if (fileEntry.isDirectory()) {
        listFilesForFolder(fileEntry);
      } else {
        files.add(fileEntry.getAbsolutePath());
        System.out.println(fileEntry.getAbsolutePath());
      }
    }
    return files;
  }

}
