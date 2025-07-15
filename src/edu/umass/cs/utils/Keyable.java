package edu.umass.cs.utils;

/**
 * @param <KeyType>
 * @author V. Arun
 */
public interface Keyable<KeyType> {
    /**
     * @return The key.
     */
    public KeyType getKey();
}
