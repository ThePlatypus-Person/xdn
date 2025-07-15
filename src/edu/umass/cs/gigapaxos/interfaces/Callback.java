package edu.umass.cs.gigapaxos.interfaces;

/**
 * @param <R>
 * @param <V>
 * @author arun
 */
public interface Callback<R, V> {
    /**
     * @param response
     * @return Value returned by processing {@code response}.
     */
    public V processResponse(R response);
}
