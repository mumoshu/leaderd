package leaderd

import (
	"log"
	"time"
)

func (l Instance) Run() {
	var leader = "unknown-leader"

	var currentLeader *CurrentLeader
	var err error

	var lastLeaderUpdate int64

	for {
		if leader == l.Name {
			err = l.UpdateLastUpdate()
			if err != nil {
				log.Print("Unable to update leader status.")
				// If we haven't been able to update our status as leader in +timeout+
				// seconds, stop assuming we are the leader.
				if lastLeaderUpdate < time.Now().Unix()-l.Timeout {
					log.Printf("%d seconds since we last updated our leader status, assuming we lost leader role.", l.Timeout)
					leader = "unknown-leader"
				}
			} else {
				// Keep track of when we last updated our status as leader.
				lastLeaderUpdate = time.Now().Unix()
			}
		} else {
			currentLeader, err = l.GetCurrentLeader()

			if err != nil {
				log.Printf("Failed to query current leader: %s.", err.Error())

				time.Sleep(time.Duration(l.Interval) * time.Second)
				continue
			} else {
				if currentLeader.Name != leader {
					log.Printf("Leader has changed from %s to %s.", leader, currentLeader.Name)
				}

				leader = currentLeader.Name
			}

			// If the current leader has expired, try to steal leader.
			if currentLeader.Name != l.Name && currentLeader.LastUpdate <= time.Now().Unix()-int64(l.Timeout) {
				log.Printf("Attempting to steal leader from expired leader %s.", currentLeader.Name)
				err = l.AttemptToStealLeader()
				if err == nil {
					log.Print("Success! This node is now the leader.")
					leader = l.Name
				} else {
					log.Printf("Error while stealing leadership role: %s", err)
				}
			}
		}

		time.Sleep(time.Duration(l.Interval) * time.Second)
	}
}
