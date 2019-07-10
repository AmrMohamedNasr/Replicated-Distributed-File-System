package rmi.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import configurations.ClientConfigurations;

import data.FileContent;
import data.TransactionMsg;
import exceptions.MessageNotFoundException;
import request.AbortRequest;
import request.BeginTransactionRequest;
import request.CommitRequest;
import request.ReadRequest;
import request.Request;
import request.WriteRequest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import data.RequestType;
import rmi.master.MasterServerClientInterface;
import rmi.replica.ReplicaServerClientInterface;
import utils.RMIUtils;

public class Client {
	// null for now till i get the info
	private MasterServerClientInterface masterServer = null;
	private ClientConfigurations cc;
	private HashMap<Integer, ReplicaServerClientInterface> transComOrAb;
	private HashMap<Integer, TransactionMsg> transMsgs;
	private Set<Integer> abortedTrans;
	private Set<Integer> comittedTrans;
	private HashMap<Integer, Integer> requestNums;
	private HashMap<Integer, String> transFiles;
	
	public static void main(String[] args) throws NotBoundException, 
		IOException, MessageNotFoundException {
		PrintStream st = new PrintStream(args[0] + "log.txt");
		System.setOut(st);
		System.setErr(st);
		Client c = new Client (args[0]);
		c.start();
		System.out.println("Client has done all his transactions");
		System.exit(0);
	}
	
	public Client (String fileName) throws RemoteException, NotBoundException {
		cc = new ClientConfigurations (fileName);
		masterServer = getMasterObject (cc.getMasterAdd(), cc.getMasterPort());
		transComOrAb = new HashMap<>();
		transMsgs = new HashMap<>();
		abortedTrans = new HashSet<Integer>();
		comittedTrans = new HashSet<Integer>();
		requestNums = new HashMap<>();
		transFiles = new HashMap<>();
	}
	
	private void cleanUp(int tid) {
		transComOrAb.remove(tid);
		transMsgs.remove(tid);
		requestNums.remove(tid);
		transFiles.remove(tid);
	}
	private void start () throws RemoteException,
		IOException, NotBoundException, MessageNotFoundException {

		List<Request> trans = cc.getRequest();
		for (int i = 0; i < trans.size(); i++) {
			Request t = trans.get(i);
			System.out.println("Transaction Number : " + t.getTransactionNum());
			if (abortedTrans.contains(t.getTransactionNum())) {
				System.out.println("\tTransaction was already aborted");
				continue;
			}
			if (comittedTrans.contains(t.getTransactionNum())) {
				System.out.println("\tTransaction was already committed");
				continue;
			}
			if (requestNums.containsKey(t.getTransactionNum())) {
				System.out.println("\tRequest Number : " + requestNums.get(t.getTransactionNum()));
			}
			if (!transComOrAb.containsKey(t.getTransactionNum()) && t.getType() != RequestType.BEGIN) {
				System.out.println("\tInvalid transaction number");
				continue;
			}
			switch (t.getType()) {
				case BEGIN:
					BeginTransactionRequest br = (BeginTransactionRequest) t;
					System.out.println("\tfile : " + br.getFileName());
					TransactionMsg msg = masterServer.request_transaction(br.getFileName());
					transMsgs.put(br.getTransactionNum(), msg);
					RMIUtils utils = new RMIUtils();
					transComOrAb.put(br.getTransactionNum(), utils.getReplicaServer(msg.getLoc()));
					requestNums.put(br.getTransactionNum(), 0);
					System.out.println("\tGet the Primary Replica : ( "
							+ msg.getLoc().toString() + " )");
					transFiles.put(br.getTransactionNum(), br.getFileName());
					break;
				case READ :
					ReadRequest r = (ReadRequest) t;
					String fn = transFiles.get(r.getTransactionNum());
					System.out.println("\tRequest Type : Read ... Reading file " + fn);
					try {
						FileContent file = read(r.getTransactionNum(), fn);
						if (file.isError()) {
							System.out.println("\tServer gives back error due to concurrency");
							abort(r.getTransactionNum());
						} else {
							System.out.println("\tFile Content : { ");
							System.out.println(file.toString() + " } ");
						}
					} catch (FileNotFoundException e) {
						System.out.println("\t" + fn + " doesn't exist in the system.");
						abort(r.getTransactionNum());
					}
					break;
				case WRITE :
					WriteRequest w = (WriteRequest) t;
					String fn2 = transFiles.get(w.getTransactionNum());
					List<String> dataLines = w.getData();
					ArrayList<FileContent> content = new ArrayList<>();
					for (int k = 0; k < dataLines.size(); k++) {
						content.add(new FileContent(fn2, dataLines.get(k) + "\n"));
					}
					System.out.println("\tRequest Type : Write ... Writing in file " + fn2);
					boolean succes = write( w.getTransactionNum(), content, fn2);
					if (!succes) {
						System.out.println("\tServer gives back error due to concurrency");
						abort(w.getTransactionNum());
					}
					break;
				case COMMIT :
					CommitRequest c = (CommitRequest) t;
					System.out.println("\tRequest Type : Commit ... Commiting file" + transFiles.get(c.getTransactionNum()));
					commit(c.getTransactionNum());
					break;
				case ABORT :
					AbortRequest a = (AbortRequest) t;
					System.out.println("\tRequest Type : Abort ... Aborting file" + transFiles.get(a.getTransactionNum()));
					abort(a.getTransactionNum());
					break;
				
			}
		}
	}
	
	public FileContent read(int id, String fileName) throws FileNotFoundException,
		RemoteException, IOException, NotBoundException {
		ReplicaServerClientInterface primaryReplica = transComOrAb.get(id);
		TransactionMsg msg = transMsgs.get(id);
		FileContent file = primaryReplica.read(msg, requestNums.get(id), fileName);
		requestNums.put(id, requestNums.get(id) + 1);
		return file;
	}
	
	public boolean write(int id,
			ArrayList<FileContent> data, String filename) throws FileNotFoundException,
			RemoteException, IOException, NotBoundException,
			MessageNotFoundException {
		TransactionMsg msg = transMsgs.get(id);
		ReplicaServerClientInterface primaryReplica = transComOrAb.get(id);
		System.out.println("In Client");
		System.out.println("Writing In : " + filename);
		boolean success = true;
		for (int i = 0; i < data.size() && success; i++) {
			int rnum = requestNums.get(id);
			System.out
					.println("\tSend Write Request to PrimaryReplica : transactionID : "
							+ msg.getTransactionId()
							+ ", msgSequenceNum : "
							+ rnum);
			success = success && primaryReplica.write(msg, rnum, data.get(i));
			requestNums.put(id, requestNums.get(id) + 1);
		}
		System.out.println("Finished .. Return ");
		return success;

	}
	
	private void commit(int id) throws RemoteException, IOException,
			NotBoundException, MessageNotFoundException {
		TransactionMsg msg = transMsgs.get(id);
		System.out
				.println("\tSend Commit Request to Primary Replica : transactionNum : "
						+ id );
		ReplicaServerClientInterface primaryReplica = transComOrAb.get(id);
		boolean success = primaryReplica.commit(msg);
		System.out.println("\tCommit Response : "
				+ success);
		cleanUp(id);
		if (success) {
			comittedTrans.add(id);
		} else {
			abortedTrans.add(id);
		}
	}
	
	private void abort (int id) throws RemoteException, IOException,
			NotBoundException, MessageNotFoundException {
		TransactionMsg msg = transMsgs.get(id);
		System.out
				.println("\tSend Abort Request to Primary Replica : transactionNum : "
						+ id);
		ReplicaServerClientInterface primaryReplica = transComOrAb.get(id);
		boolean success = primaryReplica.abort(msg);
		System.out.println("\tAbort Response : "
				+ success);
		cleanUp(id);
		abortedTrans.add(id);
	}
	
	private MasterServerClientInterface getMasterObject (String ipAddr, String port) throws RemoteException,
	NotBoundException {
		Registry registry = LocateRegistry.getRegistry(ipAddr, RMIUtils.RMI_PORT);
		return (MasterServerClientInterface) registry.lookup("Master");
	}


}
	