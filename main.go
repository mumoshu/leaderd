package leaderd

import (
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"os"
	"time"
	"github.com/askreet/leaderd/pkg/leaderd"
)

var region string

var l leaderd.Instance

var interval int

var leader = "unknown-leader"

func main() {
	if err := parseArguments(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	l.Dynamo = dynamodb.New(session.New(&aws.Config{MaxRetries: aws.Int(0)}))

	var currentLeader *leaderd.CurrentLeader
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

				time.Sleep(time.Duration(interval) * time.Second)
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

		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func parseArguments() error {
	flag.StringVar(&l.Table, "table", "", "dynamodb table to use")
	flag.StringVar(&l.Name, "name", "", "name for this node")
	flag.IntVar(&interval, "interval", 10, "how often (seconds) to check if leader can be replaced, or to update leader timestamp if we are leader")
	flag.Int64Var(&l.Timeout, "timeout", 60, "number of seconds before attempting to steal leader")

	flag.Parse()

	if l.Table == "" {
		return errors.New("required argument table not provided")
	}

	if l.Name == "" {
		return errors.New("required argument name not provided")
	}

	return nil
}
