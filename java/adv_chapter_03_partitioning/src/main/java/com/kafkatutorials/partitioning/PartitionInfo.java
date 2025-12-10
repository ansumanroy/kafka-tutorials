package com.kafkatutorials.partitioning;

/**
 * Information about a partition including message count and keys.
 */
public class PartitionInfo {
    private final int partition;
    private long messageCount;
    private long startOffset;
    private long endOffset;
    
    public PartitionInfo(int partition) {
        this.partition = partition;
        this.messageCount = 0;
        this.startOffset = -1;
        this.endOffset = -1;
    }
    
    public int getPartition() {
        return partition;
    }
    
    public long getMessageCount() {
        return messageCount;
    }
    
    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }
    
    public long getStartOffset() {
        return startOffset;
    }
    
    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }
    
    public long getEndOffset() {
        return endOffset;
    }
    
    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }
    
    public double getPercentage(long totalMessages) {
        if (totalMessages == 0) return 0.0;
        return (messageCount * 100.0) / totalMessages;
    }
    
    public String getBar(int maxLength) {
        int barLength = (int) Math.round((messageCount * maxLength) / (double) messageCount);
        return "█".repeat(Math.max(0, barLength));
    }
}
