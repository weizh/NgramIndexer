package gngramindexer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IllegalFormatException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;

public class IndexerQuerier {

  private IndexSearcher stringSearcher;

  public IndexerQuerier(String indexname, String loadmode, int maxreturn) {
    try {
      stringSearcher = getIndexSearcher(indexname, loadmode);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      System.err.println("String searcher creation failed.");
      ;
    }
    BooleanQuery.setMaxClauseCount(maxreturn);
  }

  public Integer getCountsByPhrase(String... paras) throws Exception {
    if (paras == null || paras.length == 0)
      throw new NullPointerException();
    if (paras.length % 2 != 0)
      throw new Exception("Odd Number of Parameters not accepted for getCountsByPhrase");

    BooleanQuery query = new BooleanQuery();
    for (int i = 0; i < paras.length; i+=2) {
      String q = paras[i];
      String fname = paras[i + 1];
      TermQuery tq = new TermQuery(new Term(fname, q));
      query.add(tq, Occur.MUST);
    }

    TopDocs res = null;
    try {
      res = stringSearcher.search(query, Integer.MAX_VALUE);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if (res == null)
      return null;

    int total = 0;

    for (ScoreDoc doc : res.scoreDocs) {
      IndexableField ifield = stringSearcher.doc(doc.doc).getField("freq");
      // System.out.println(stringSearcher.doc(doc.doc));
      total += ifield.numericValue().intValue();
    }
    return total;

  }

  public static void main(String argv[]) {
    IndexerQuerier iq1 = new IndexerQuerier("index1", "disk", 2400);
    IndexerQuerier iq2= new IndexerQuerier("index2", "disk", 2400);
    IndexerQuerier iq3 = new IndexerQuerier("index3", "disk", 2400);
    IndexerQuerier iq4 = new IndexerQuerier("index4", "disk", 2400);
    IndexerQuerier iq5 = new IndexerQuerier("index5", "disk", 2400);
    IndexerQuerier iqnum = new IndexerQuerier("index_num", "disk", 2400);
    //"Azerbaijan", FIELD.n1, "declared", FIELD.n2
    try {
      int n1 = iq1.getCountsByPhrase(argv);
      int n2 = iq2.getCountsByPhrase(argv);
      int n3 = iq3.getCountsByPhrase(argv);
      int n4 = iq4.getCountsByPhrase(argv);
      int n5 = iq5.getCountsByPhrase(argv);
      int n_num = iqnum.getCountsByPhrase(argv);

      System.out.println(n1+n2+n3+n4+n5+n_num);
      
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static IndexSearcher getIndexSearcher(String filename, String diskOrMem) throws Exception {
    IndexReader reader;
    Directory d;
    if (diskOrMem.equals("disk"))
      d = FSDirectory.open(new File(filename));
    else if (diskOrMem.equals("mmap"))
      d = new RAMDirectory(FSDirectory.open(new File(filename)), IOContext.READ);
    else
      throw new Exception("parameter for directory type not defined.");

    if (OSUtil.isWindows())
      reader = IndexReader.open(FSDirectory.open(new File(filename)));
    else
      reader = IndexReader.open(NIOFSDirectory.open(new File(filename)));
    IndexSearcher searcher = new IndexSearcher(reader);
    return searcher;
  }

}
