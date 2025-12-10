package com.kafkatutorials.serialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Simple POJO for JSON serialization examples.
 * Represents a user event.
 */
public class UserEvent {
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("username")
    private String username;
    
    @JsonProperty("email")
    private String email;
    
    @JsonProperty("timestamp")
    private long timestamp;
    
    @JsonProperty("phone_number")
    private String phoneNumber;
    
    // Default constructor for Jackson
    public UserEvent() {
    }
    
    public UserEvent(String userId, String username, String email, long timestamp) {
        this.userId = userId;
        this.username = username;
        this.email = email;
        this.timestamp = timestamp;
    }
    
    public UserEvent(String userId, String username, String email, long timestamp, String phoneNumber) {
        this(userId, username, email, timestamp);
        this.phoneNumber = phoneNumber;
    }
    
    // Getters and setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phoneNumber) { this.phoneNumber = phoneNumber; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserEvent userEvent = (UserEvent) o;
        return timestamp == userEvent.timestamp &&
               Objects.equals(userId, userEvent.userId) &&
               Objects.equals(username, userEvent.username) &&
               Objects.equals(email, userEvent.email) &&
               Objects.equals(phoneNumber, userEvent.phoneNumber);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userId, username, email, timestamp, phoneNumber);
    }
    
    @Override
    public String toString() {
        return "UserEvent{" +
               "userId='" + userId + '\'' +
               ", username='" + username + '\'' +
               ", email='" + email + '\'' +
               ", timestamp=" + timestamp +
               ", phoneNumber='" + phoneNumber + '\'' +
               '}';
    }
}
