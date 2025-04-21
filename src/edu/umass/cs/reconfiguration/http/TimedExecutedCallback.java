package edu.umass.cs.reconfiguration.http;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.LinkedHashMap;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

public class TimedExecutedCallback implements ExecutedCallback {
    private final ExecutedCallback inner;
    private final Map<String, Long> timeMarkers = new LinkedHashMap<>();
    private final long startTime;
    private final XdnHttpRequest request;
    private final String startTimestamp;

    public TimedExecutedCallback(XdnHttpRequest request, ExecutedCallback inner) {
        this.request = request;
        this.inner = inner;
        this.startTime = System.nanoTime();
        this.startTimestamp = LocalDateTime.now().format(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        );
    }

    public void mark(String label) {
        timeMarkers.put(label, System.nanoTime());
    }

    @Override
    public void executed(Request executedRequest, boolean handled) {
        inner.executed(executedRequest, handled); // call original ExecutedCallback

        /*
        HttpRequest reqData = request.getHttpRequest();
        String reqInfo = String.format("[%s] %s - %s%s", 
            startTimestamp,
            reqData.method().toString(), 
            request.getServiceName(),
            reqData.uri()
        );
        System.out.println(reqInfo);
        */

        // Print out time spent on each function.
        /*
        long prev = startTime;
        for (Map.Entry<String, Long> entry : timeMarkers.entrySet()) {
            System.out.printf("%50s %6.3fs\n", 
                entry.getKey(), 
                (entry.getValue() - prev) / 1000_000_000.0
            );
            prev = entry.getValue();
        }
        */

        /*
        long deltaTotal = System.nanoTime() - startTime;
        String duration = String.format("Total Duration: %.3fs", 
            deltaTotal / 1000_000_000.0
        );
        System.out.println(duration);
        */
    }
}
