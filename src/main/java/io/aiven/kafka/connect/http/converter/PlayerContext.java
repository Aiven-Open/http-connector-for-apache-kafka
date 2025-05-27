package io.aiven.kafka.connect.http.converter;


public class PlayerContext {
    private String contextId;
    private String playerId;

    public PlayerContext(String contextId, String playerId) {
        this.contextId = contextId;
        this.playerId = playerId;
    }

  public String getContextId() {
        return contextId;
    }

    public String getPlayerId() {
        return playerId;
    }

    public void setContextId(String contextId) {
        this.contextId = contextId;
    }

    public void setPlayerId(String playerId) {
        this.playerId = playerId;
    }



}