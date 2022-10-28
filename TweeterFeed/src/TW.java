import edu.stanford.nlp.ie.KBPTokensregexExtractor;
import mpi.MPI;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

// TODO
// standard deviation could be performing overflow, not sure about correct results

public class TW {
	static final int numOfCores = Runtime.getRuntime().availableProcessors();
	static final boolean DEBUG = false;
	enum ComputationType{
		SEQUENTIAL, PARALLEL, DISTRIBUTED
	}
	public static String DownloadedTweets = "DownloadedTweets.txt";
	public static String CurrentTheme = "DownloadedTheme.txt";
	public static String Theme = "Tema.txt";
	public static String ComputationTypeFile = "ComputationType.txt";
	public static int numberOfTweetsToBeProcessed = 10;
	public static boolean limitNumberOfTweets = true;
	// 280 characters is the limit of single tweet, so 5000 should be enough :D
	public static int mpiBufferLength = 5000;

	public static void testing(){
		String neki = "Raz, ?e ?aden pakiet nie istnia?. Dwa, ?e o wielkim projekcie pt. MISJA POKOJOWA " +
				"nie wiedzia? nikt, poza Kaczy?skim i Janezem Jans?, w?adc? S?owenii ???? " +
				"https://t.co/XNPUTEbPY4t d@Johanca_4me @SamoGlavan Ve kdo, ce bo Jansa lahko prisel," +
				" glede na to, da ima ob 18h sestanek v ZG?t ?Lavrov predlaga pogovore med rusko in " +
				"ukrajinsko delegacijo v BEOGRADU. Janša je pa spet izvisel. Kako nepomemben je, govori " +
				"zgoraj napisano dejstvo.t ?@jansa_dior @CjelskiPob To?no to, slovenske novice so nepomembne. " +
				"Globalne dosti bolj, saj si nažalost ne krojimo ve? sami usode...sploh pa zdaj..??????t " +
				"jRT @Demokracija1: .@JJansaSDS bo danes odpotoval v #Zagreb\n";
		byte[] bytes = null;
		System.out.println(neki);
		System.out.print("Size of string: ");
		System.out.println(neki.length());
		System.out.println("Serialization of string");
		bytes = serializeString(neki);
		System.out.print("Length of bytes array: ");
		System.out.println(bytes.length);
		System.out.println("Deserialization of string");
		String deserializedStr = deserializeString(bytes);
		System.out.println("Deserialized string: ");
		System.out.println(deserializedStr);
		Perf perf = evaluateTweet(deserializedStr);
		bytes = serializePerfObj(perf);
		perf = deserializePerfObj(bytes);
		System.out.println(perf.tweet);
		System.exit(0);
	}

	public static void main(String[] args) {
		/*
		// Testing because of some problems
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setHttpProxyHost("proxy");
		cb.setHttpProxyPort(8080);
		*/

		//tuki mam mpi
		MPI.Init(args); //nastavlen na 8 procesov
		int me = MPI.COMM_WORLD.Rank(); //ko nrdim mpi init, main funkcijo izvedejo usi procesi od mpi, locuje med njimi od 0-7, tko jih locm
		int mpiRank = MPI.COMM_WORLD.Size(); //stevilo procesov

		// some configuration, the variable that this method sets is static, so no need to set it again for threads
		StanfordNLP.nastavi(); //nastaviš spremenljivko pipeline u razredu 'tvito'

		/*
		if(true){
			if(me == 0)
				testing();
			MPI.Finalize();
			System.exit(0);
		}
		*/
			//tuki gre sam en proces ostali grejo u else
		if(me == 0) {

			//ComputationType.txt ct = getWantedTypeOfComputation();
			ComputationType ct = getComputationType(); //reads from file 'computationType' where i write if its sequential,..
			System.out.println("Using: " + ct.toString());

			// choose the topic of choice for downloading tweets
			//String tema = "trump";
			String tema = getWantedTheme(); //same for this, from the file
			System.out.println("Topic of interest: " + tema);


			// arraylist which will contain all tweets
			ArrayList<String> vsiTviti = null;

			// because of twitter api limiting downloading rate, tweets are saved in a file upon download
			File file = new File(DownloadedTweets); // open file where tweets are stored
			if (!file.exists() | themeChange(tema)) { // if tweets are not downloaded yet or change of theme it loads again
				vsiTviti = tvito.dobiTvite(tema);  // download tweets
				// store them to a file
				try {
					// Creates a FileOutputStream where objects from ObjectOutputStream are written
					FileOutputStream fileStream = new FileOutputStream(DownloadedTweets);
					// Creates the ObjectOutputStream
					ObjectOutputStream objStream = new ObjectOutputStream(fileStream);
					// write all downloaded tweets to file
					for (Object o : vsiTviti) {
						objStream.writeObject(o);
					}
					objStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				// write current theme in file to be able to tell change of theme
				writeThemeToFile(tema);
			} else {
				// if tweets are already downloaded, read them
				try {
					// Creates a file input stream linked with the specified file
					FileInputStream fileStream = new FileInputStream(DownloadedTweets);
					// Creates an object input stream using the file input stream
					ObjectInputStream objStream = new ObjectInputStream(fileStream);

					// make an array for tweets
					vsiTviti = new ArrayList<String>();

					// ObjectInputStream is just a wrapper for some other stream, such as FileInputStream.
					// Although ObjectInputStream.available () returns zero, the FileInputStream.available will return some value.
					while (fileStream.available() > 0) {
						try {
							// add all tweets to array
							vsiTviti.add((String) objStream.readObject());
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					}

				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			if(limitNumberOfTweets){
				// for testing purposes choosing less tweets, sublist for first 10 elements so its faster
				vsiTviti = new ArrayList<String>(vsiTviti.subList(0, numberOfTweetsToBeProcessed));
			}

			if (ct.equals(ComputationType.SEQUENTIAL)){
				stopAllWorkers(mpiRank);
				serialComputation(vsiTviti);
			}
			else if(ct.equals(ComputationType.PARALLEL)){
				stopAllWorkers(mpiRank);
				parallelComputation(vsiTviti);
			}
			else if(ct.equals(ComputationType.DISTRIBUTED)){
				//we got workers and 1 farmer
				// farmer is the process with 0 identification
				farmer(vsiTviti, mpiRank);
			}
		}
		//so everything that is not 0 goes to this worker method
		else{
			// the workers come here
			//if(me == 1)
				worker();
		}
		MPI.Finalize();

	}

	/***                  COMPUTATION METHODS                                     ***/

	public static void parallelComputation(ArrayList<String> vsiTviti){
		// parallel computation of problem
		// keep track of time
		long start = System.nanoTime();
		// create FixedThreadPool and use available processor cores,numOfCores is from Runtime.getRuntime().availableProcessors, how many 'threads'
		ExecutorService executor = Executors.newFixedThreadPool(numOfCores);
		// create array of future objects which will contain results for tweet evaluation
		Future[] futureArr = new Future[vsiTviti.size()];
		for(int i = 0; i < vsiTviti.size(); i++){
			// get tweet and add it to the threadpool for computation
			String tvit = vsiTviti.get(i);
			//executor.submit ,tells it to calculate this tvit
			futureArr[i] = (executor.submit(() -> evaluateTweet(tvit)));
			//a new thread will happen and evaluate twweet will happen and result will be saved on this table
			//so future object saved in array
		}

		// now that we divided tasks to threads we need to collect results
		ArrayList<Perf> procesiraniTweeti = new ArrayList<Perf>();
		for(int i = 0; i < futureArr.length; i++){
			try{
				// just loop through array of future objects and store results to arraylist procesiraniTweeti
				procesiraniTweeti.add((Perf)futureArr[i].get());
			} catch (ExecutionException | InterruptedException e){
				System.out.println(e.toString());
			}
		}

		// compute time elapsed for serial computing
		long duration = System.nanoTime() - start;

		// if this line is not present the execution doesn't stop, i can do this because i call .get() which means all has been called from procesiraniTweeti
		executor.shutdown();

		// compute factors of interest
		Result res = calculatePerformance(procesiraniTweeti);

		// add duration of serial computation to result
		res.durationOfOperation = duration;
		// add tweetsPerSecond to result
		res.tweetsPerSecond = (vsiTviti.size() / ((double)duration)) * res.nanosecondDivider;

		System.out.println(res);
	}

	public static void serialComputation(ArrayList<String> vsiTviti){
		// serial computation of problem
		// keep track of time
		long start = System.nanoTime();

		// make array procesiraniTweeti for results
		ArrayList<Perf> procesiraniTweeti = new ArrayList<Perf>();

		// go through all tweets
		for(String tvit : vsiTviti) {
			// process tweet and store it in arraylist procesiraniTweeti
			// storing results mainly because of post processing calculation
			procesiraniTweeti.add(evaluateTweet(tvit));
		}

		// compute time elapsed for serial computing
		long duration = System.nanoTime() - start;

		// compute factors of interest based on results from procesiraniTweeti
		Result res = calculatePerformance(procesiraniTweeti);

		// add duration of serial computation to result of whole operation
		res.durationOfOperation = duration;
		// add tweetsPerSecond to result we multiply by nano because we need seconds
		res.tweetsPerSecond = (vsiTviti.size() / ((double)duration)) * res.nanosecondDivider;

		System.out.println(res);
	}

	public static void farmer(ArrayList<String> vsiTviti, int mpiRank){
		// debate if one should use blocking or non blocking calls
		// decided to use blocking calls therefore two loops
		// erialization for mpi communication:
		// https://stackoverflow.com/questions/14200699/send-objects-with-mpj-express
		ArrayList<Perf> procesiraniTweeti = new ArrayList<Perf>();

		// process 0 is the farmer so it doesn't do evaluation of tweet
		// but just divides task among workers
		int index = 0;
		long start = System.nanoTime();
	//three loops so they dont wait for one another
		for(;index < mpiRank - 1; index++){
			String tweet = vsiTviti.get(index);
			// serialize string
			byte[] bytes = serializeString(tweet);

			// compute destination number
			int dest = index+ 1;
			// send that string
			//send bytes, start with 0 sending length, what type is in it-byte, dest=which worker do i send,tag is used so the worker knows when to finish
			MPI.COMM_WORLD.Send(bytes, 0 , bytes.length, MPI.BYTE, dest, 0);
		}

		// array for receiving results from workers
		byte[] bytes = new byte[mpiBufferLength];
		// array for sending
		byte[] bytes2 = new byte[mpiBufferLength];

		// loop through all tweets
		for(; index < vsiTviti.size(); index++){
			if(DEBUG)
				System.out.println("New loop iteration, index: " + index);
			// first receive evaluation from worker, send back what they calculated
			mpi.Status status = MPI.COMM_WORLD.Recv(bytes, 0, mpiBufferLength, MPI.BYTE, MPI.ANY_SOURCE, MPI.ANY_TAG);
			if(DEBUG)
				System.out.println(bytes.length);

			// convert to object
			Perf perf = deserializePerfObj(bytes);
			// add result to arraylist
			procesiraniTweeti.add(perf);


			// now send worker more work
			bytes2 = serializeString(vsiTviti.get(index));
			MPI.COMM_WORLD.Send(bytes2, 0, bytes2.length, MPI.BYTE,
					status.source, 0);
		}
		//when i send all tweets i wait for last 7 results.
		for(int i = 1; i < mpiRank; i++){
			// first receive evaluation from worker
			mpi.Status status = MPI.COMM_WORLD.Recv(bytes, 0, mpiBufferLength, MPI.BYTE, MPI.ANY_SOURCE, MPI.ANY_TAG);

			// convert to object
			Perf perf = deserializePerfObj(bytes);
			// add result to arraylist
			procesiraniTweeti.add(perf);
		}

		stopAllWorkers(mpiRank);

		long duration = System.nanoTime() - start;

		Result res = calculatePerformance(procesiraniTweeti);

		// add duration of serial computation to result
		res.durationOfOperation = duration;
		// add tweetsPerSecond to result
		res.tweetsPerSecond = (vsiTviti.size() / ((double)duration)) * res.nanosecondDivider;

		System.out.println(res);
	}

	public static void stopAllWorkers(int mpiRank){
		// send all workers that they are done
		for(int i = 1; i < mpiRank; i++){
			// sending absolutely nothing, with tag -1 the worker
			// exits loop and finishes so it is not available anymore
			byte[] bytes = new byte[0];
			//send everyone tag -1
			MPI.COMM_WORLD.Send(bytes, 0, 0, MPI.BYTE, i, -1);
		}
	}

	public static void worker(){
		// variables needed for worker
		// tvit for evaluation, status for worker to finish, buffer because of stupid mpi communication
		String tvit = "";
		mpi.Status status = new mpi.Status();
		status.tag = 0;
		byte[] bytes = new byte[mpiBufferLength];
		byte[] bytes2 = null;

			//here it puts it out
		while (status.tag != -1){
			// first receive bytes from farmer
			//waits till she gets data
			status = MPI.COMM_WORLD.Recv(bytes, 0, mpiBufferLength, MPI.BYTE, 0, MPI.ANY_TAG);
			// check for ending condition
			if(status.tag == -1)
				continue;

			tvit = deserializeString(bytes);

			// now we have tweet string, time to process it
			if(DEBUG)
				System.out.println("Faker: " + tvit);
			Perf perf = evaluateTweet(tvit);
			// again same process for sending, serialize object -> send
			bytes2 = serializePerfObj(perf);
			if(DEBUG)
				System.out.println("Dolzina objekta: " + bytes2.length);
			MPI.COMM_WORLD.Send(bytes2, 0, bytes2.length, MPI.BYTE, 0, 0);
		}
		if(DEBUG)
			System.out.println("Worker done");
	}


	/***                  AUXILIARY METHODS                                     ***/

	public static ComputationType getComputationType(){
		String line = "";
		try{
			File type = new File(ComputationTypeFile);
			Scanner reader = new Scanner(type);
			line = reader.nextLine();
			reader.close();
		}
		catch (FileNotFoundException e){
			e.printStackTrace();
		}
		ComputationType type = null;
		if(line.toLowerCase().equals("sequential")){
			return ComputationType.SEQUENTIAL;
		}
		else if(line.toLowerCase().equals("parallel")){
			return ComputationType.PARALLEL;
		}
		else if(line.toLowerCase().equals("distributed")){
			return ComputationType.DISTRIBUTED;
		}
		else{
			System.out.println("Wrong input in file ComputationType.txt, fix it, using SEQUENTIAL for this iteration.");
			return ComputationType.SEQUENTIAL;
		}
	}

	public static String getWantedTheme(){
		String line = "";
		try{
			File theme = new File(Theme);
			Scanner reader = new Scanner(theme);
			line = reader.nextLine();
			reader.close();
		}
		catch (FileNotFoundException e){
			e.printStackTrace();
		}
		return line;
	}

	public static Perf evaluateTweet(String tweet){
		//for every tweet it measures time it took
		long start = System.nanoTime();
		int result = StanfordNLP.najdiS(tweet);
		long timeElapsed = System.nanoTime() - start;
		System.out.println(tweet + " : " + result);
		//returns which tweet i processed, its time and 1-2-3
		return (new Perf(tweet, timeElapsed, result));
	}

	public static boolean themeChange(String theme){
		String line = "";
		try{
			File currentTheme = new File(CurrentTheme);
			Scanner reader = new Scanner(currentTheme);
			line = reader.nextLine();
			reader.close();
		}
		catch (FileNotFoundException e){
			System.out.println(e.toString());
			e.printStackTrace();
		}
		return ! theme.equals(line);
	}

	public static void writeThemeToFile(String theme){
		try{
			File file = new File(CurrentTheme);
			FileWriter writer = new FileWriter(file);
			writer.write(theme);
			writer.close();
		}
		catch (IOException e){
			e.printStackTrace();
		}

	}

	public static ComputationType getWantedTypeOfComputation(){
		Scanner sc = new Scanner(System.in);
		System.out.println("Choose type of computation(1-SEQUENTIAL, 2-PARALLEL, 3-DISTRIBUTED):");
		int a = 0;
		while(true){
			if(!sc.hasNextInt()){
				System.out.println("Incorrect input, use numbers!");
				sc.next();
				continue;
			}
			// read input
			a = sc.nextInt();
			// check if it is correct
			if(a > 0 && a < 4){
				// if correct, break out of loop
				break;
			}
			// otherwise repeat procedure
			System.out.println("Incorrect input, please write again!");
		}
		// convert int to ComputationType.txt enum
		return ComputationType.values()[a-1];
	}

	public static byte[] serializeString(String s){
		byte[] bytes = null;
		try{
			// serializing string, because otherwise doesn't work
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = null;
			out = new ObjectOutputStream(bos);
			out.writeObject(s);
			bytes = bos.toByteArray();
		}
		catch (IOException e){
			e.printStackTrace();
		}
		if(bytes == null){
			System.out.println("Serialization of string failed !!");
		}
		return bytes;
	}

	public static String deserializeString(byte[] bytes){
		String tvit = "";
		// procedure to convert bytes to string
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try{
			in = new ObjectInputStream(bis);
			Object obj = in.readObject();
			tvit = (String)obj;

		}
		catch (IOException | ClassNotFoundException e){
			e.printStackTrace();
		}
		if(tvit.equals("")){
			System.out.println("Deserialization of bytes to string failed !!");
		}
		return tvit;
	}

	public static byte[] serializePerfObj(Perf perf){
		byte[] bytes = null;
		try{
			// serializing perf object, because otherwise doesn't work
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = null;
			out = new ObjectOutputStream(bos);
			out.writeObject(perf);
			bytes = bos.toByteArray();
		}
		catch (IOException e){
			e.printStackTrace();
		}

		if(bytes == null){
			System.out.println("Problem with Perf serializing");
		}

		return bytes;
	}

	public static Perf deserializePerfObj(byte[] bytes){
		Perf perf = null;
		// procedure to convert bytes to string
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try{
			in = new ObjectInputStream(bis);
			Object obj = in.readObject();
			perf = (Perf)obj;

		}
		catch (IOException | ClassNotFoundException e){
			e.printStackTrace();
		}
		if(perf == null){
			System.out.println("Deserialization of bytes to Perf object failed !!");
		}
		return perf;
	}

	/***                  PERFORMANCE METHODS                                     ***/

	public static Result calculatePerformance(ArrayList<Perf> arr){
		// first sort the array(mainly because of median calculation)
		Collections.sort(arr);

		// create object to store results
		Result r = new Result();

		// MAXIMUM
		Perf p = arr.get(arr.size()-1);
		r.maximumObj = p; r.maximum = p.timeElapsed;

		// MINIMUM
		p = arr.get(0);
		r.minimumObj = p; r.minimum = p.timeElapsed;

		// MEDIAN
		p = arr.size() % 2 == 1 ? arr.get(arr.size()/2) : arr.get(arr.size()/2-1);
		r.medianObj = p; r.median = p.timeElapsed;

		// AVERAGE
		long sum = 0;
		for(Perf perf : arr){
			sum += perf.timeElapsed;
			// while calculating average computing also number of pos/neg/neu evaluation
			if(perf.result == 1){
				r.negativelyEvaluated++;
			}
			else if(perf.result == 2){
				r.neutrallyEvaluated++;
			}
			else if(perf.result == 3){
				r.positivelyEvaluated++;
			}
			else{
				System.out.println("Something is really really wrong");
			}
		}
		r.average = sum / arr.size();

		// STANDARD DEVIATION
		// can't sum it all at once because of overflow, so therefore dividing each factor of summation
		double partialSum = 0.0;
		for(Perf perf : arr){
			partialSum += (Math.pow(perf.timeElapsed - r.average, 2) / arr.size());
		}
		r.standardDeviation = (Math.sqrt(partialSum) / r.nanosecondDivider);
		return r;
	}
}

class Result{
	public long maximum, minimum, average, median;
	public double standardDeviation;
	public long durationOfOperation;
	public Perf maximumObj, minimumObj, medianObj;
	public final double nanosecondDivider = 1000000000;
	public int negativelyEvaluated = 0, neutrallyEvaluated = 0, positivelyEvaluated = 0;
	public double tweetsPerSecond = 0.0;

	public String toString(){
		return "MIN: " + this.fromNanosecToSec(this.minimum) + "s       -----> " + this.minimumObj.tweet
				+ "\nMAX: " + this.fromNanosecToSec(this.maximum) + "s       -----> " + this.maximumObj.tweet
				+ "\nMEDIAN: " + this.fromNanosecToSec(this.median) + "s    -----> " + this.medianObj.tweet
				+ "\nAVG: " + this.fromNanosecToSec(this.average) + "s"
				+ "\nSD: " + this.standardDeviation
				+ "\nTweets per second: " + this.tweetsPerSecond
				+ "\nNumber of negative tweets: " + this.negativelyEvaluated
				+ "\nNumber of neutral tweets:  " + this.neutrallyEvaluated
				+ "\nNumber of positive tweets: " + this.positivelyEvaluated;
	}

	public double fromNanosecToSec(long number){
		return ((double)number) / this.nanosecondDivider;
	}
}

// Comparable for sorting array, because of need for median and so on..
// Serializable for serialization of object, because of  mpi communication
class Perf implements Comparable, Serializable{
	public String tweet;
	public long timeElapsed;
	public int result;

	public Perf(){}

	public Perf(String tweet, long timeElapsed, int result){
		this.tweet = tweet;
		this.timeElapsed = timeElapsed;
		this.result = result;
	}

	@Override
	//method collection.sort needs a method CompareTo so that it sorts it
	public int compareTo(Object o) {
		// numbers are to big for integer, a little hack
		return Long.compare(this.timeElapsed, ((Perf)o).timeElapsed);
	}
}