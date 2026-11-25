package org.group17;

import java.util.List;

public class LeaderElection {

    private ServerInfo currentLeader;
    private List<ServerInfo> participants;

    public void startElection() {}

    public void receiveElectionMessage(ElectionMessage message) {}

    public void announceLeader(ServerInfo leader) {}

    public ServerInfo getCurrentLeader() {
        return currentLeader;
    }

}
