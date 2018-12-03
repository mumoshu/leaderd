package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"os"
	"github.com/askreet/leaderd/pkg/leaderd"
)

func main() {
	var l leaderd.Instance

	flag.StringVar(&l.Table, "table", "", "dynamodb table to use")
	flag.StringVar(&l.Name, "name", "", "name for this node")
	flag.IntVar(&l.Interval, "interval", 10, "how often (seconds) to check if leader can be replaced, or to update leader timestamp if we are leader")
	flag.Int64Var(&l.Timeout, "timeout", 60, "number of seconds before attempting to steal leader")

	flag.Parse()

	if err := l.Validate(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	l.Dynamo = dynamodb.New(session.New(&aws.Config{MaxRetries: aws.Int(0)}))

	l.Run()
}
