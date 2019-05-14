package rmi.replica;

import data.FileContent;
import data.ReplicaLoc;
import exceptions.MessageNotFoundException;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicaServer implements ReplicaInterface, ReplicaServerClientInterface,
                                      ReplicaServerInterface, ReplicaServerMasterInterface {
    // TODO: To be changed to configuration or arguments.
    public static final String REGISTRY_ADDRESS = "Test";
    public static final int REGISTRY_PORT = 2020;

    private static final String REPLICA_PATH_PREFIX = "Replica";

    private String path;
    private ReplicaLoc loc;
    private Registry registry;
    private Map<String, List<ReplicaServer>> sameFileReplicas;
    private Map<String, ReentrantReadWriteLock> locks;
    private Map<Long, String> transactionFiles;
    private Map<Long, Map<Long, String>> transactionWrites;

    public ReplicaServer(ReplicaLoc loc) {
        this.loc = loc;
        this.path = REPLICA_PATH_PREFIX + loc.getId();
        this.registry = getRegistry();
        this.sameFileReplicas = new ConcurrentHashMap<>();
        this.locks = new ConcurrentHashMap<>();
        this.transactionFiles = new HashMap<>();
        this.transactionWrites = new TreeMap<>();
        createDirectory();
    }

    private void createDirectory() {
        File file = new File(this.path);
        if (!file.exists()){
            file.mkdir();
        }
    }

    private Registry getRegistry() {
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry(REGISTRY_ADDRESS, REGISTRY_PORT);
        } catch (RemoteException e) {
            System.out.println("Unable to get Registry");
        }
        return registry;
    }

    @Override
    public void acquireLock(String fileName) throws RemoteException {
        locks.get(fileName).writeLock().lock();
    }

    @Override
    public boolean updateReplicas(long transactionId, String fileName, Map<Long, String> writes) throws RemoteException, IOException {
        File file = new File(path + "/" + fileName);
        if (!file.exists()) {
            file.createNewFile();
        }

        return false;
    }

    @Override
    public void releaseLock(String fileName) throws RemoteException {
        locks.get(fileName).writeLock().unlock();
    }

    @Override
    public void createFile(String fileName) throws RemoteException {
        synchronized (locks) {
            if (!locks.containsKey(fileName)) {
                locks.put(fileName, new ReentrantReadWriteLock());
            }
        }
    }

    @Override
    public void setAsPrimary(String fileName, List<ReplicaLoc> locations) throws RemoteException {
        List<ReplicaServer> replicaServers = new ArrayList<>();
        for (ReplicaLoc replicaLoc : locations) {
//            if (replicaLoc.getId() == this.loc.getId()) {
//                continue;
//            }
            replicaServers.add(getReplicaServer(replicaLoc));
        }
        sameFileReplicas.put(fileName, replicaServers);
    }

    private ReplicaServer getReplicaServer(ReplicaLoc replicaLoc) throws RemoteException {
        ReplicaServer replicaServer = null;
        try {
            // TODO: Modify Name to make it consistent.
            replicaServer = (ReplicaServer) registry.lookup("Replica" + replicaLoc.getId());
        } catch (NotBoundException e) {
            System.out.println("NotBoundException for Registry Variable");
        }
        return replicaServer;
    }

    @Override
    public boolean isAlive() throws RemoteException {
        return true;
    }

    @Override
    public Boolean write(long transactionId, long messageSequenceNumber, FileContent data) throws RemoteException, IOException {
        String fileName = data.getFilename();
        if (!transactionFiles.containsKey(transactionId)) {
            transactionFiles.put(transactionId, fileName);
            transactionWrites.put(transactionId, new TreeMap<Long, String>());
        }
        transactionWrites.get(transactionId).put(messageSequenceNumber, data.getData());
        return true;
    }

    @Override
    public FileContent read(long transactionId, String fileName) throws FileNotFoundException, IOException, RemoteException {
        File file = new File(path + "/" + fileName);
        if (transactionWrites.containsKey(transactionId)) {
            String initialContent = getFileContent(fileName);
            String unCommittedContent = getUnCommittedContent(transactionWrites.get(transactionId));
            return new FileContent(fileName, initialContent + unCommittedContent);
        }

        if (!file.exists()) {
            throw new FileNotFoundException();
        }

        String data = getFileContent(fileName);
        return new FileContent(fileName, data);
    }

    private String getUnCommittedContent(Map<Long, String> transactionWrites) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Long, String> entry : transactionWrites.entrySet()) {
            builder.append(entry.getValue());
        }
        return builder.toString();
    }

    private String getFileContent(String fileName) throws IOException {
        File file = new File(path + "/" + fileName);
        if (!file.exists()) {
            throw new FileNotFoundException();
        }
        locks.get(fileName).readLock().lock();
        String content = getContentFromFile(file);
        locks.get(fileName).readLock().unlock();
        return content;
    }

    private String getContentFromFile(File file) throws IOException {
        StringBuilder builder = new StringBuilder();
        FileReader fileStream = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileStream);
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            builder.append(line + System.lineSeparator());
        }
        bufferedReader.close();
        fileStream.close();
        return builder.toString();
    }

    @Override
    public boolean commit(long transactionId, long numOfMessages) throws MessageNotFoundException, IOException {
        Map<Long, String> writes = transactionWrites.get(transactionId);
        if (writes.entrySet().size() != numOfMessages) {
            throw new MessageNotFoundException();
        }
        String fileName = transactionFiles.get(transactionId);
        List<ReplicaServer> replicas = sameFileReplicas.get(fileName);
        for (ReplicaServer replica : replicas) {
            replica.acquireLock(fileName);
            replica.updateReplicas(transactionId, fileName, transactionWrites.get(transactionId));
            replica.releaseLock(fileName);
        }
        transactionWrites.remove(transactionId);
        transactionFiles.remove(transactionId);
        return true;
    }

    @Override
    public boolean abort(long transactionId) throws RemoteException {
        transactionWrites.remove(transactionId);
        transactionFiles.remove(transactionId);
        return true;
    }
}
