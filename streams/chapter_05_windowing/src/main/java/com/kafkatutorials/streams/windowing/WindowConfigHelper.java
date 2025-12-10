package com.kafkatutorials.streams.windowing;

import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

public class WindowConfigHelper {
    
    public static TimeWindows getTumblingWindow(Duration size) {
        return TimeWindows.ofSizeWithNoGrace(size);
    }
    
    public static TimeWindows getHoppingWindow(Duration size, Duration advance) {
        return TimeWindows.ofSizeWithNoGrace(size).advanceBy(advance);
    }
    
    public static SessionWindows getSessionWindow(Duration inactivityGap) {
        return SessionWindows.ofInactivityGapWithNoGrace(inactivityGap);
    }
}
