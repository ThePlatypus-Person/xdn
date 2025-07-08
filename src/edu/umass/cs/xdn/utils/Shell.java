package edu.umass.cs.xdn.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import edu.umass.cs.xdn.utils.ShellOutput;


public class Shell {

    public static int runCommand(String command, boolean isSilent,
                                 Map<String, String> environmentVariables) {
        try {
            // prepare to start the command
            ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));
            if (isSilent) {
                pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
                pb.redirectError(ProcessBuilder.Redirect.DISCARD);
            }

            if (environmentVariables != null) {
                Map<String, String> processEnv = pb.environment();
                processEnv.putAll(environmentVariables);
            }

            if (!isSilent) {
                System.out.println("command: " + command);
                if (environmentVariables != null) {
                    System.out.println(environmentVariables.toString());
                }
            }

            // run the command as a new OS process
            Process process = pb.start();

            // print out the output in stderr, if needed
            if (!isSilent) {
                InputStream inputStream = process.getInputStream();
                String output = new String(inputStream.readAllBytes(), StandardCharsets.ISO_8859_1);
                if (!output.isEmpty())
                    System.out.println("output:\n" + output);

                InputStream errStream = process.getErrorStream();
                String err = new String(errStream.readAllBytes(), StandardCharsets.ISO_8859_1);
                if (!err.isEmpty())
                    System.out.println("error:\n" + err);
            }

            int exitCode = process.waitFor();

            if (!isSilent) {
                System.out.println("exit code: " + exitCode);
            }

            return exitCode;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static int runCommand(String command, boolean isSilent) {
        return runCommand(command, isSilent, null);
    }

    public static int runCommand(String command) {
        return runCommand(command, true);
    }


    public static ShellOutput runCommandWithOutput(String command, boolean isSilent,
                                 Map<String, String> environmentVariables) {
        try {
            ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));

            if (environmentVariables != null) {
                Map<String, String> processEnv = pb.environment();
                processEnv.putAll(environmentVariables);
            }

            if (!isSilent) {
                System.out.println("command: " + command);
                if (environmentVariables != null) {
                    System.out.println(environmentVariables.toString());
                }
            }

            Process process = pb.start();

            String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
            String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.ISO_8859_1);

            int exitCode = process.waitFor();

            return new ShellOutput(stdout, stderr, exitCode);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static ShellOutput runCommandWithOutput(String command, boolean isSilent)  {
        return runCommandWithOutput(command, isSilent, null);
    }

    public static ShellOutput runCommandWithOutput(String command)  {
        return runCommandWithOutput(command, true);
    }


    // process output is not printed out due to thread blocking
    public static int runCommandThread(String command, boolean isSilent,
                                 Map<String, String> environmentVariables) {
	Thread processThread = new Thread(() -> {
	    try {
		ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));

		pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
		pb.redirectError(ProcessBuilder.Redirect.DISCARD);

		if (environmentVariables != null) {
		    Map<String, String> processEnv = pb.environment();
		    processEnv.putAll(environmentVariables);
		}

		if (!isSilent) {
		    System.out.println("command: " + command);
		    if (environmentVariables != null) {
			System.out.println(environmentVariables.toString());
		    }
		}

		Process process = pb.start();
		int exitCode = process.waitFor();

		if (!isSilent) {
		    System.out.println("exit code: " + exitCode);
		}
	    } catch (IOException | InterruptedException e) {
		throw new RuntimeException(e);
	    }
	});

        try {
	    processThread.start();
        } catch (IllegalThreadStateException e) {
            throw new RuntimeException(e);
        }

	return 0;
    }
}
