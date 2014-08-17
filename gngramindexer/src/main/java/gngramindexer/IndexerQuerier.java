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
      res = stringSearcher.search(query, 2500);
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
    IndexerQuerier iq = new IndexerQuerier("index", "disk", 2400);
    try {
      System.out.println(iq.getCountsByPhrase("Azerbaijan", FIELD.n1, "declared", FIELD.n2));
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
