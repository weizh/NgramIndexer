package gngramindexer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;



public class IndexerQuerier {

	


	public static void main(String arbg[]) {

		ExecutorService es = Executors.newFixedThreadPool(6);
		Collection<Callable<Integer>> calls = new ArrayList<Callable<Integer>>();
		calls.add(new RunnableQuerier("index1", "disk", 2400,arbg));
		calls.add(new RunnableQuerier("index2", "disk", 2400,arbg));
		calls.add(new RunnableQuerier("index3", "disk", 2400,arbg));
		calls.add(new RunnableQuerier("index4", "disk", 2400,arbg));
		calls.add(new RunnableQuerier("index5", "disk", 2400,arbg));
		calls.add(new RunnableQuerier("index_num", "disk", 2400,arbg));
		List<Future<Integer>> f = null;
		try {
			f = es.invokeAll(calls);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		es.shutdown();

		boolean finshed = false;
		try {
			finshed = es.awaitTermination(1000000, TimeUnit.MINUTES);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (finshed) {
			int total = 0;
			for (Future<Integer> fut : f) {
				try {
					total += fut.get();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println(total);
		}

	}

	

}
