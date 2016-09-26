import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import mpi.MPI;

public class MPIPageRank {
	
	static Map<Integer,Double> rankValues = new TreeMap<>();
	/** Sorts the TreeMap by values in descending order */
	public static <K, V extends Comparable<V>> Map<K, V> 
	    sortByValues(final Map<K, V> map) {
	    Comparator<K> valueComparator = 
	             new Comparator<K>() {
	      public int compare(K k1, K k2) {
	        int compare = 
	              map.get(k2).compareTo(map.get(k1));
	        if (compare == 0) 
	          return 1;
	        else 
	          return compare;
	      }
	    };
	 
	    Map<K, V> sortedByValues = 
	      new TreeMap<K, V>(valueComparator);
	    sortedByValues.putAll(map);
	    return sortedByValues;
	  }
	   public static void mpi_printValues(String outputFile) throws IOException {
	        try
	        {
	                PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
	                int count =0;
	                Iterator<Map.Entry<Integer, Double>> indet = rankValues.entrySet().iterator();
	                System.out.println("The top 10 URLS are :");
	                while (indet.hasNext() && count++ < 10)
	                {
	                        Map.Entry<Integer,Double> ipar = indet.next();
	                        double val =ipar.getValue();
	                        writer.println("Rank of page "+ipar.getKey() + " is = " + val);
	                        System.out.println("Rank of page "+ipar.getKey() + " is = " + val);
	                }
	                writer.close();
	        }
	        catch(Exception e)
	        {
	                e.printStackTrace();
	        }

	    }
	public static void read_mpi(String inputFile,int chunkSize,Map<Integer,ArrayList<Integer>>[] localIndegreeMapArray,
			Map<Integer,Integer> outdegreeCount)
	{
		Map<Integer,ArrayList<Integer>> localIndegreeMap = new HashMap<>();
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		Map<Integer,ArrayList<Integer>> indegree=new HashMap<>();
		Map<Integer,ArrayList<Integer>> outdegree=new HashMap<>();
		if(rank==0)
		{
			try
			{
				FileReader reader = new FileReader(new File(inputFile));
                BufferedReader bufferedReader = new BufferedReader(reader);
                String line;
                while((line = bufferedReader.readLine())!=null)
                {
                        String[] columns = line.split(" ");
                        int link = Integer.parseInt(columns[0]);
                        if (!outdegree.containsKey(link))
                        {
                                outdegree.put(link,new ArrayList<Integer>());
                        }
                        // edgelist : outdegree list
                        ArrayList<Integer> edgeList = new ArrayList<>();
                        for ( int i=1;i<columns.length;i++)
                                edgeList.add(Integer.parseInt(columns[i]));
                        outdegree.put(link, edgeList);
                }
                /** Populates the dangling node values(which is an empty list) by making the node point to all other nodes
                *  in the graph */

                Iterator<Map.Entry<Integer,ArrayList<Integer>>> iterDanglingNode = outdegree.entrySet().iterator();
                while(iterDanglingNode.hasNext())
                {
                        Map.Entry<Integer,ArrayList<Integer>> eachPair = iterDanglingNode.next();
                        ArrayList<Integer> tempValue = eachPair.getValue();
                        /** If the size of arraylist of edges is zero, add all the keys to the graph */
                        if(tempValue.size()==0)
                        {
                                outdegree.put(eachPair.getKey(),new ArrayList<Integer>(outdegree.keySet()));
                        }

                }
                /** Populates the indegree Map */

                Iterator<Map.Entry<Integer,ArrayList<Integer>>> it = outdegree.entrySet().iterator();
                while (it.hasNext()) {
                        Map.Entry<Integer,ArrayList<Integer>> pair =it.next();
                        outdegreeCount.put(pair.getKey(),pair.getValue().size());
                        int nodeUnderConsideration = pair.getKey();
                        if(!indegree.containsKey(nodeUnderConsideration))
                        {
                                indegree.put(pair.getKey(), new ArrayList<>());
                        }
                        Iterator<Map.Entry<Integer,ArrayList<Integer>>> temp_it=outdegree.entrySet().iterator();
                        ArrayList<Integer> indegreeList = new ArrayList<>();
                        while(temp_it.hasNext())
                        {
                                Map.Entry<Integer,ArrayList<Integer>> inner_pair=temp_it.next();
                                int currentNode=inner_pair.getKey();
                                List<Integer> valuesOfOtherNodes = inner_pair.getValue();
                                Iterator<Integer> outDegreeIterator = valuesOfOtherNodes.iterator();
                                while(outDegreeIterator.hasNext())
                                {
                                        int value = outDegreeIterator.next();
                                        if(value==nodeUnderConsideration)
                                                indegreeList.add(currentNode);
                                }
                        }
                        indegree.put(pair.getKey(),indegreeList);
                }
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		ArrayList<Integer> totalListOfNodes = new ArrayList<>(indegree.keySet());
		if(rank==0)
		{
			int k=chunkSize, start=0;
			for(int process = 1;process<size;process++)
			{
				start = k ;
				Map<Integer,ArrayList<Integer>> templocalIndegreeMap = new HashMap<>();
				for(k=start;k<(start+chunkSize);k++)
				{
					templocalIndegreeMap.put(totalListOfNodes.get(k),indegree.get(k));
				}
				localIndegreeMapArray[0]=templocalIndegreeMap;
				MPI.COMM_WORLD.Send(localIndegreeMapArray, 0, 1, MPI.OBJECT, process, 1);
			}
			for ( int i=0;i<chunkSize;i++)
			{
				localIndegreeMap.put(totalListOfNodes.get(i),indegree.get(i));
			}
			localIndegreeMapArray[0]=localIndegreeMap;
			
		}
		else
		{
			MPI.COMM_WORLD.Recv(localIndegreeMapArray, 0, 1, MPI.OBJECT, 0, 1);
			localIndegreeMap = localIndegreeMapArray[0];
			/** This portion of the code was used to for checking the nodes that each process received 
			 * 
			 
			if(rank == 1)
			{
				Iterator<Map.Entry<Integer, ArrayList<Integer>>> it = localIndegreeMap.entrySet().iterator();
				while(it.hasNext())
				{
					Map.Entry<Integer, ArrayList<Integer>> pair = it.next();
					System.out.println("I am "+rank+" and I have "+pair.getKey()+ "  " +pair.getValue());
				}
			}
			if(rank == 2)
			{
				Iterator<Map.Entry<Integer, ArrayList<Integer>>> it = localIndegreeMap.entrySet().iterator();
				while(it.hasNext())
				{
					Map.Entry<Integer, ArrayList<Integer>> pair = it.next();
					System.out.println("I am "+rank+" and I have "+pair.getKey()+ "  " +pair.getValue());
				}
			}
			if(rank == 3)
			{
				Iterator<Map.Entry<Integer, ArrayList<Integer>>> it = localIndegreeMap.entrySet().iterator();
				while(it.hasNext())
				{
					Map.Entry<Integer, ArrayList<Integer>> pair = it.next();
					System.out.println("I am "+rank+" and I have "+pair.getKey()+ "  " +pair.getValue());
				}
			}
			/** **************************************************************************** */
		}
		
	}
	public static void mpi_pagerank(Map<Integer,ArrayList<Integer>>[] lIndegreeMapArray,Map<Integer,
			Integer>[] outdegreeCountMapArray,int iterations,double df)
	{
		int count =0;
		
		while(count < iterations)
		{
			Map<Integer,ArrayList<Integer>> local = lIndegreeMapArray[0];
			Map<Integer,Integer> outdegreeCount=outdegreeCountMapArray[0];
			Map<Integer,Double>[] rankValuesArray = new Map[1];
			/** The below code was used for checking whether the outdegreeCount Map was received properly by all 
			 * the processes 
			
			Iterator<Map.Entry<Integer, Integer>> it1 = outdegreeCount.entrySet().iterator();
			MPI.COMM_WORLD.Barrier();
			System.out.println("Printing outdegree Counts");
			while(it1.hasNext())
			{
				Map.Entry<Integer, Integer> pair1 = it1.next();
				System.out.println("I am "+MPI.COMM_WORLD.Rank()+" and I have "+pair1.getKey()+ "  " +pair1.getValue());
			}
			MPI.COMM_WORLD.Barrier();
			*************************************************************************************************** */
			
			/** Main Rank Calculation */
			Iterator<Map.Entry<Integer, ArrayList<Integer>>> localIndegIter = local.entrySet().iterator();
			Map<Integer,Double> rankCopy = new HashMap<>();
			
			while(localIndegIter.hasNext())
			{
				Map.Entry<Integer, ArrayList<Integer>> localIndegIterPair = localIndegIter.next();
				ArrayList<Integer> localIndegIterPairValue = local.get(localIndegIterPair.getKey());
				Iterator<Integer> localIndegIterPairValueIter = localIndegIterPairValue.iterator();
				double value = 0.0;
				double newRank =0.0;
				while(localIndegIterPairValueIter.hasNext())
				{
					int currNode = localIndegIterPairValueIter.next();
					value += (rankValues.get(currNode))/(outdegreeCount.get(currNode));
				}
				/** Incorporating the damping factor */
				newRank = ((1-df)/outdegreeCount.size())+(df*value);
				rankCopy.put(localIndegIterPair.getKey(), newRank);
			}
			MPI.COMM_WORLD.Barrier();
			
			/** This portion of the code helps all the process to sync the rank values each of them updates*/
			if(MPI.COMM_WORLD.Rank()!=0)
    		{
				rankValuesArray[0]=rankCopy;
	        	MPI.COMM_WORLD.Send(rankValuesArray, 0, 1, MPI.OBJECT, 0 , 4);
    		}
			if(MPI.COMM_WORLD.Rank()==0)
    		{
        		
    			for(int proces=1;proces<MPI.COMM_WORLD.Size();proces++)
    			{
    				MPI.COMM_WORLD.Recv(rankValuesArray, 0, 1, MPI.OBJECT, proces, 4);
    				for(int nodeValue: rankValuesArray[0].keySet())
    				{
    					rankValues.put(nodeValue , rankValuesArray[0].get(nodeValue));
    				}
    			}
    			for(int eachKey: rankCopy.keySet())
            	{
        			rankValues.put(eachKey, rankCopy.get(eachKey));
            	}
    			
    		}
			
			/** Broadcasts the new ranks */
			rankValuesArray=new Map[1];
			rankValuesArray[0]=rankValues;
            MPI.COMM_WORLD.Bcast(rankValuesArray, 0, 1, MPI.OBJECT, 0);
            if(MPI.COMM_WORLD.Rank()!=0)
            {
            	rankValues=rankValuesArray[0];
            }
			/** Waits for all processes to reach till this point. Helps to understand the flow of the program while debugging */
            MPI.COMM_WORLD.Barrier();
			count++; // Counter for number of iterations
		}
		
		/* This part of the code was written for checking whether the rankValues table is consistent for all the processes
		 * Iterator<Map.Entry<Integer, Double>> it2 = rankValues.entrySet().iterator();
		
		while(it2.hasNext())
		{
			Map.Entry<Integer, Double> pair2 = it2.next();
			System.out.println("I am "+MPI.COMM_WORLD.Rank()+" and I have "+pair2.getKey()+ "  " +pair2.getValue());
		}
		/** ***********************************************************************************************************  */
		
	}
	public static void main(String[] args)throws Exception
	{
		String inputFile = args[3];
		String outputFileParallel = args[4];
		double df= Double.parseDouble(args[5]);
		int iterations = Integer.parseInt(args[6]);
		MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		//String inputFile = "/u/aritde/DistributedSystems/pagerank.input.1000.urls.14";
		int sizeBuf[] = new int[1];
		int localSize =0;
		int chunkSize = 1000/size;
		int remChunkSize = 1000%size;
		/* Number of nodes each process should expect. If the number of processes does not exactly divide the number of nodes,
		then one node is added to the 'remainder' number of nodes */
		if(rank == 0)
		{
			if(remChunkSize==0)
			{
				for (int i=1;i<size;i++)
				{
					sizeBuf[0]=chunkSize;
					MPI.COMM_WORLD.Send(sizeBuf, 0, 1, MPI.INT, i, 1);
				}
				localSize = chunkSize;
			}
			else
			{
				for (int i=1;i<size;i++)
				{
					if(i<remChunkSize)
						sizeBuf[0]=chunkSize+1;
					else
						sizeBuf[0]=chunkSize;
					MPI.COMM_WORLD.Send(sizeBuf, 0, 1, MPI.INT, i, 1);
				}
				localSize = chunkSize+1;
			}
		}
		else
		{
			MPI.COMM_WORLD.Recv(sizeBuf, 0, 1, MPI.INT, 0, 1);
			localSize = sizeBuf[0];
		}
		Map<Integer,ArrayList<Integer>>[] localIndegreeMapArray = new Map[1];
		Map<Integer,ArrayList<Integer>> localIndegreeMap = new HashMap<>();
		Map<Integer,Integer> outdegreeCount = new HashMap<>();
		/* THe read_mpi functions reads the inputfile . 
		 * It populates the outdegree , indegree and outdegree count map */
		read_mpi(inputFile,chunkSize,localIndegreeMapArray,outdegreeCount);
		
		//Waits for other processes to reach here. Facilitates better understanding during debugging
		MPI.COMM_WORLD.Barrier();
		
		//Calculates total number of URLS
		int[] totalUrlsInEach = new int[1];
		Map<Integer,ArrayList<Integer>> count = new HashMap<>();
		count = localIndegreeMapArray[0];
		totalUrlsInEach[0]=count.size();
		int[] sum = new int[1];
		MPI.COMM_WORLD.Allreduce(totalUrlsInEach, 0, sum, 0, 1, MPI.INT, MPI.SUM);
		
		Map<Integer,Double>[] rankValuesArray = new Map[1];
		// Populates initial rank table
		if(rank==0)
		{
			
			for(int pr = 0;pr<sum[0];pr++)
			{
				rankValues.put(pr, 1.0/sum[0]);
			}
			rankValuesArray[0] = rankValues;
			
		}
		//Broadcasts the rank table
		MPI.COMM_WORLD.Bcast(rankValuesArray, 0,1 , MPI.OBJECT, 0);
		
		Map<Integer,Integer>[] outdegreeCountMapArray = new Map[1];
		outdegreeCountMapArray[0]=outdegreeCount;
		// Broadcasts the outdegree count map  so that all nodes can the use it while calculating rank
		MPI.COMM_WORLD.Bcast(outdegreeCountMapArray, 0,1 , MPI.OBJECT, 0);
		if(rank!=0)
		{
			rankValues = rankValuesArray[0];
		}
		
		/* The mpi_pagerank function calculates the rank for n iterations */
		mpi_pagerank(localIndegreeMapArray,outdegreeCountMapArray/*,rankValuesArray*/,iterations,df);
		if(MPI.COMM_WORLD.Rank()==0)
		{
			// Sorts the treemap by values - indecreasing order
			rankValues = (TreeMap<Integer, Double>) sortByValues(rankValues);
			//Writes the output to the file
			mpi_printValues(outputFileParallel);
		}
		MPI.Finalize();
	}
}
