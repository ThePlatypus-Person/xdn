package edu.umass.cs.xdn.recorder;

import edu.umass.cs.xdn.utils.Shell;
import edu.umass.cs.xdn.utils.ShellOutput;
import io.netty.handler.codec.base64.Base64Encoder;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Set;
import java.util.stream.Collectors;

public class FuselogStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String FUSELOG_BIN_PATH = "/usr/local/bin/fuselog";
    private static final String FUSELOG_APPLY_BIN_PATH = "/usr/local/bin/fuselog-apply";

    private static final String defaultWorkingBasePath = "/tmp/xdn/state/fuselog/";

    private final String baseMountDirPath;
    private final String baseSocketDirPath;
    private final String baseDiffDirPath;

    // important locations:
    // /tmp/xdn/state/fuselog/<node-id>/                                    the base directory
    // /tmp/xdn/state/fuselog/<node-id>/mnt/                                the mount directory
    // /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/e<epoch>/        mount dir of specific service
    // /tmp/xdn/state/fuselog/<node-id>/sock/                               the socket directory
    // /tmp/xdn/state/fuselog/<node-id>/sock/<service-name>::e<epoch>.sock  socket to fs of specific service
    // /tmp/xdn/state/fuselog/<node-id>/diff/                               the stateDiff directory
    // /tmp/xdn/state/fuselog/<node-id>/diff/<service-name>::e<epoch>.diff  the stateDiff file

    // mapping service name and epoch into its fuselog filesystem socket
    public class SocketPair {
	private SocketChannel capture;
	private ServerSocketChannel applyChannel;

	public SocketPair() {
	    this.capture = null;
	    this.applyChannel = null;
	}

	public void setCaptureSocket(SocketChannel capture) {
	    this.capture = capture;
	}

	public void setApplyChannel(ServerSocketChannel apply) {
	    this.applyChannel = apply;
	}

	public SocketChannel getCaptureSocket() {
	    return capture;
	}

	public ServerSocketChannel getApplyChannel() {
	    return applyChannel;
	}
    }
    private final Map<String, Map<Integer, SocketPair>> serviceFsSocket;

    public FuselogStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingBasePath + nodeID + "/");
        System.out.println(">>> initializing fuse statediff recorder");

        // make sure that fuselog and fuselog-apply are exist
        File fuselog = new File(FUSELOG_BIN_PATH);
        if (!fuselog.exists()) {
            String errMessage = "fuselog binary does not exist at " + FUSELOG_BIN_PATH;
            System.out.println("ERROR: " + errMessage);
            throw new RuntimeException(errMessage);
        }
        File fuselogApplicator = new File(FUSELOG_APPLY_BIN_PATH);
        if (!fuselogApplicator.exists()) {
            String errMessage = "fuselog-apply binary does not exist at " + FUSELOG_APPLY_BIN_PATH;
            System.out.println("ERROR: " + errMessage);
            throw new RuntimeException(errMessage);
        }

        // create working mount dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/mnt/
        this.baseMountDirPath = this.baseDirectoryPath + "mnt/";
        try {
            Files.createDirectories(Paths.get(this.baseMountDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create socket dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/sock/
        this.baseSocketDirPath = defaultWorkingBasePath + nodeID + "/sock/";
        try {
            Files.createDirectories(Paths.get(this.baseSocketDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create diff dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/diff/
        this.baseDiffDirPath = defaultWorkingBasePath + nodeID + "/diff/";
        try {
            Files.createDirectories(Paths.get(this.baseDiffDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // initialize mapping between serviceName to the FS socket
        this.serviceFsSocket = new ConcurrentHashMap<>();
    }

    @Override
    public String getTargetDirectory(String serviceName, int placementEpoch) {
        // location: /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/e<epoch>/
        return String.format("%s%s/e%d/",
                baseMountDirPath, serviceName, placementEpoch);
    }

    @Override
    public boolean preInitialization(String serviceName, int placementEpoch) {
	if (this.serviceFsSocket.containsKey(serviceName) 
	    && this.serviceFsSocket.get(serviceName).containsKey((placementEpoch))) {
            System.out.printf(">>> Fuselog.preInitialization() - socket data for %s:%d already exists. Skipping...\n", 
		serviceName, placementEpoch);

	    return true;
	}

        String targetDirPath = this.getTargetDirectory(serviceName, placementEpoch);

        // create target mnt dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/mnt/service1/
        File targetDir = new File(targetDirPath);

        if (!targetDir.exists()) {
            System.out.printf("Fuselog.preInitialization() - Directory %s doesn't exist. Creating...\n", targetDirPath);

            String createDirCommand = String.format("mkdir -p %s", targetDirPath);
            int code = Shell.runCommand(createDirCommand);
            assert code == 0;
        } else {
            System.out.printf("Rsync.preInitialization() - Directory %s exist. Unmounting fuselog\n", targetDirPath);
            Shell.runCommand("fusermount -u " + targetDirPath);
        }

	// initialize apply stateDiff socket for fuselog-apply
        String applySocketFile = this.baseSocketDirPath + serviceName + "::" + placementEpoch + "::apply.sock";
	try {
            Files.deleteIfExists(Path.of(applySocketFile));
        } catch (IOException e) {
	    throw new RuntimeException(String.format(
		"Failed to delete old %s file: %s", applySocketFile, e));
        }

	UnixDomainSocketAddress applyAddress = UnixDomainSocketAddress.of(Path.of(applySocketFile));
	ServerSocketChannel applyServer;
        try {
	    applyServer = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
	    applyServer.bind(applyAddress);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

	this.serviceFsSocket.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
	SocketPair serviceSocket = new SocketPair();
	serviceSocket.setApplyChannel(applyServer);
        this.serviceFsSocket.get(serviceName).put(placementEpoch, serviceSocket);

        return true;
    }

    @Override
    public boolean postInitialization(String serviceName, int placementEpoch) {
        System.out.printf("FUSE.postInitialization(serviceName=%s, placementEpoch=%d)\n", serviceName, placementEpoch);

        String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
        String captureSocketFile = baseSocketDirPath + serviceName + "::" + placementEpoch + ".sock";

        // initialize file system in the mnt dir, with socket
        assert targetDir.length() > 1 : "invalid target mount directory";
        // remove the trailing '/' at the end of targetDir
        String targetDirPath = targetDir.substring(0, targetDir.length() - 1);
        String cmd = String.format("%s %s", FUSELOG_BIN_PATH, targetDirPath);

        Map<String, String> env = new HashMap<>();
        env.put("FUSELOG_SOCKET_FILE", captureSocketFile);
	env.put("ADAPTIVE_DEV_MODE", "false");
	env.put("FUSELOG_COMPRESSION", "true");
	env.put("FUSELOG_PRUNE", "true");
	env.put("ADAPTIVE_COMPRESSION", "false");
	env.put("WRITE_COALESCING", "true");
	env.put("RUST_LOG", "info");

        int exitCode = Shell.runCommand(cmd, true, env);
        assert exitCode == 0 : "failed to mount filesystem with exit code " + exitCode;

        // initialize capture stateDiff socket client for the filesystem
        SocketChannel captureChannel;
        UnixDomainSocketAddress captureAddress = UnixDomainSocketAddress.of(Path.of(captureSocketFile));
        try {
            captureChannel = SocketChannel.open(StandardProtocolFamily.UNIX);

            // Wait 0.5 seconds for fuselog to start
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            boolean isConnEstablished = captureChannel.connect(captureAddress);
            if (!isConnEstablished) {
                System.err.println("failed to connect to the filesystem");
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // update the socket metadata
	this.serviceFsSocket.get(serviceName).get(placementEpoch)
	    .setCaptureSocket(captureChannel);
	/*
	this.serviceFsSocket.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
	SocketPair serviceSocket = new SocketPair(captureChannel, applyServer);
        this.serviceFsSocket.get(serviceName).put(placementEpoch, serviceSocket);
	*/

        return true;
    }

    @Override
    public String captureStateDiff(String serviceName, int placementEpoch) {
        Map<Integer, SocketPair> epochToChannelMap = this.serviceFsSocket.get(serviceName);
        assert epochToChannelMap != null : "unknown fs socket client for " + serviceName;
        SocketChannel socketChannel = epochToChannelMap.get(placementEpoch).getCaptureSocket();
        assert socketChannel != null : "unknown fs socket client for " + serviceName;

        // send get command (g) to the filesystem
        try {
            System.out.println(">> sending fuselog command ...");
            socketChannel.write(ByteBuffer.wrap("g".getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // wait for response indicating the stateDiff size
	ByteBuffer sizeBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
	try {
	    while (sizeBuffer.hasRemaining()) {
		int bytesRead = socketChannel.read(sizeBuffer);
		if (bytesRead == -1) {
		    throw new EOFException("Socket closed before all data could be read");
		}
	    }
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}

	sizeBuffer.flip();
	long stateDiffSize = sizeBuffer.getLong();

	System.out.printf(">> stateDiff size = %d bytes (%.2f KB, %.2f MB)%n",
	    stateDiffSize,
	    stateDiffSize / 1024.0,
	    stateDiffSize / (1024.0 * 1024));

	if (stateDiffSize <= 0) {
	    System.err.println("No stateDiff to read.");
	    return null;
	}

	ByteBuffer stateDiffBuffer = ByteBuffer.allocate((int) stateDiffSize);
	try {
	    while (stateDiffBuffer.hasRemaining()) {
		int bytesRead = socketChannel.read(stateDiffBuffer);
		if (bytesRead == -1) {
		    throw new EOFException("Socket closed before all data could be read");
		}
	    }
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
	stateDiffBuffer.flip();

	byte[] resultBytes = new byte[stateDiffBuffer.remaining()];
	stateDiffBuffer.get(resultBytes);
	String stateDiff = Base64.getEncoder().encodeToString(resultBytes);
	//System.out.println(">> read stateDiff (base64): " + stateDiff);
	

	/*
        ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
        sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
        sizeBuffer.clear();
        System.out.println(">> reading fuselog response ...");
        int numRead;
        try {
            numRead = socketChannel.read(sizeBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(">> got " + numRead + "bytes: " + new String(sizeBuffer.array(),
                StandardCharsets.UTF_8));
        if (numRead < 8) {
            System.err.println("failed to read size of the stateDiff");
            return null;
        }

        long stateDiffSize = sizeBuffer.getLong(0);

        System.out.printf(
                ">> stateDiff size = %d bytes (%.2f KB, %.2f MB)%n",
                stateDiffSize,
                stateDiffSize / 1024.0,
                stateDiffSize / (1024.0 * 1024)
        );

        //System.out.println(">> stateDiff size=" + stateDiffSize);
        assert stateDiffSize >= 0 : String.format(" stateDiffSize %d is less than zero.", stateDiffSize);

        // read all the stateDiff
        System.out.println("FuseRecorder.captureStateDiff() - begin reading all stateDiff");
        ByteBuffer stateDiffBuffer = ByteBuffer.allocate((int) stateDiffSize);
        numRead = 0;
        try {
            while (numRead < stateDiffSize) {
                numRead += socketChannel.read(stateDiffBuffer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(">> complete reading stateDiff ...");
        String stateDiff = Base64.getEncoder().encodeToString(stateDiffBuffer.array());
        System.out.println(">> read stateDiff: " + stateDiff);
	*/

        // convert the stateDiff into String
        return stateDiff;
    }

    @Override
    public boolean applyStateDiff(String serviceName, int placementEpoch, String encodedState) {
        System.out.printf("FUSE.applyStateDiff(serviceName=%s, placementEpoch=%d)\n",
                serviceName, placementEpoch);

        String applySocketFile = this.baseSocketDirPath + serviceName + "::" + placementEpoch + "::apply.sock";
        String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
        Map<Integer, SocketPair> epochToChannelMap = this.serviceFsSocket.get(serviceName);
        assert epochToChannelMap != null : "unknown fs socket client for " + serviceName;

	ServerSocketChannel serverChannel = epochToChannelMap.get(placementEpoch).getApplyChannel();
        assert serverChannel != null : "unknown fs apply stateDiff server client for " + serviceName;

        // preparing the shell command to apply stateDiff
        String cmd = String.format("%s %s --applySocket=%s",
                FUSELOG_APPLY_BIN_PATH, targetDir, applySocketFile);

        int exitCode = Shell.runCommandThread(cmd, false, null);
	try (SocketChannel client = serverChannel.accept()) {
	    System.out.println("\t\t>> fuselog-apply client connected!");
	    byte[] base64Bytes = Base64.getDecoder().decode(encodedState);
	    ByteBuffer buffer = ByteBuffer.wrap(base64Bytes);

	    while (buffer.hasRemaining()) {
		client.write(buffer);
	    }

	    client.shutdownOutput();
	    System.out.println("\t\t>> stateDiff successfully sent to fuselog-apply. Closing connection.");
	} catch (IOException e) {
	    throw new RuntimeException("Fuselog error: " + e);
	}

        assert exitCode == 0 : "failed to apply stateDiff with exit code " + exitCode;

        return true;
    }

    @Override
    public boolean removeServiceRecorder(String serviceName, int placementEpoch) {
        String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
        int umountRetCode = Shell.runCommand("fusermount -u " + targetDir, false);
        int rmRetCode = Shell.runCommand("rm -rf " + targetDir, false);
        assert rmRetCode == 0;
        return true;
    }

    /**********************************************************************************************
     *                                  Backup Test methods                                     *
     *********************************************************************************************/

    @Override
    public String getDefaultBasePath() {
        return this.defaultWorkingBasePath;
    }

    @Override
    public void initContainerSync(String myNodeId, String serviceName, Map<String, InetAddress> ipAddresses, int placementEpoch, String sshKey) {
        System.out.printf("%s:FUSE.initContainerSync(serviceName=%s, placementEpoch=%d)\n", myNodeId, serviceName, placementEpoch);

        Set<String> backupNodes = ipAddresses.keySet().stream()
                .filter(node -> !node.equals(myNodeId.toString()))
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        System.out.printf("FuselogStateDiff backupNodes = %s\n", backupNodes);
        System.out.printf("FuselogStateDiff ipAddresses = %s\n", ipAddresses);

        String currentReplica = this.baseDirectoryPath;

        Map<String, String> backupReplicas = new HashMap<>();
        backupNodes.forEach(node -> backupReplicas.put(node, String.format("%s%s/", this.defaultWorkingBasePath, node)));

        String mntDir = String.format("mnt/%s/", serviceName);
        String username = Shell.runCommandWithOutput("whoami").stdout.trim();

        // Copy data to other replicas
        Boolean allSyncSuccess = false;
        int count = 0;

        ExecutorService executor = Executors.newFixedThreadPool(backupReplicas.size());
	String sshOption = sshKey != null && !sshKey.trim().isEmpty()
	    ? String.format("-e \"ssh -i %s\"", sshKey)
	    : "";

        while (!allSyncSuccess) {
            if (++count > 10) {
                throw new RuntimeException("Failed running rsync after 10 iterations");
            }

            allSyncSuccess = true;

            List<Future<Boolean>> futures = new ArrayList<>();

            for (String key : backupReplicas.keySet()) {
                final String replicaKey = key;

                futures.add(executor.submit(() -> {
                    int exitCode = Shell.runCommand(String.format("""
                                    rsync -avz --delete --human-readable \
				    %s \
                                    --include='mnt/' --include='%s' --include='%s***' \
                                    --exclude='*' \
                                    %s %s@%s:%s""",
			    sshOption,
                            mntDir, mntDir, currentReplica,
                            username, ipAddresses.get(replicaKey).getHostAddress(),
                            backupReplicas.get(key)
                    ), true);

                    if (exitCode != 0) {
                        System.out.println(String.format(
                                "Failed to sync %s to %s", currentReplica, backupReplicas.get(key)
                        ));
                        return false;
                    }

                    return true;
                }));
            }


            for (Future<Boolean> future : futures) {
                try {
                    if (!future.get()) {
                        allSyncSuccess = false;
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    allSyncSuccess = false;
                }
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            executor.shutdown();
        }

        Map<Integer, SocketPair> epochToChannelMap = this.serviceFsSocket.get(serviceName);
        assert epochToChannelMap != null : "unknown fs socket client for " + serviceName;
        SocketChannel socketChannel = epochToChannelMap.get(placementEpoch).getCaptureSocket();
        assert socketChannel != null : "unknown fs socket client for " + serviceName;

        // begin capturing statediff (c) in the filesystem
        try {
            System.out.println(">> clear stateDiffs...");
            socketChannel.write(ByteBuffer.wrap("c".getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return;
    }

    /**********************************************************************************************
     *                                  End Backup Test methods                                 *
     *********************************************************************************************/

}
