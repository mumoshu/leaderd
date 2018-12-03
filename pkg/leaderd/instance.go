package leaderd

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"strconv"
	"time"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"log"
)

type Instance struct {
	Dynamo *dynamodb.DynamoDB
	Name   string
	Table  string

	Timeout int64
	Interval int
}

func (l Instance) GetCurrentLeader() (*CurrentLeader, error) {
	result, err := l.Dynamo.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(l.Table),
		Key: map[string]*dynamodb.AttributeValue{
			"LockName": &dynamodb.AttributeValue{S: aws.String("Leader")},
		},
	})
	if err != nil {
		return nil, err
	}

	var lastUpdate int64
	if val, ok := result.Item["LastUpdate"]; ok {
		lastUpdate, err = strconv.ParseInt(*val.N, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		// Leader has not been properly set.
		return &CurrentLeader{Set: false}, nil
	}

	var leaderName string
	if val, ok := result.Item["LeaderName"]; ok {
		leaderName = *val.S
	} else {
		return &CurrentLeader{Set: false}, nil
	}

	currentLeader := &CurrentLeader{
		Set:        true,
		Name:       leaderName,
		LastUpdate: lastUpdate,
	}

	return currentLeader, nil
}


// If we are the current leader, keep LastUpdate up-to-date,
// so that no one steals our title.
func (l Instance) UpdateLastUpdate() error {
	now := strconv.FormatInt(time.Now().Unix(), 10)

	_, err := l.Dynamo.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(l.Table),
		Item: map[string]*dynamodb.AttributeValue{
			"LockName":   &dynamodb.AttributeValue{S: aws.String("Leader")},
			"LeaderName": &dynamodb.AttributeValue{S: aws.String(l.Name)},
			"LastUpdate": &dynamodb.AttributeValue{N: aws.String(now)},
		},
		ConditionExpression: aws.String("LeaderName = :name"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name": &dynamodb.AttributeValue{S: aws.String(l.Name)},
		},
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			log.Printf("Code=%s, Message=%s", awsErr.Code(), awsErr.Message())
		}
		// TODO: If the condition expression fails, we've lost our leadership.
		// We'll have to convert this error and test for that failure.
		log.Printf("updateLastUpdate(): %#v", err)
		log.Print(err.Error())
		return err
	}

	return nil
}


func (l Instance) AttemptToStealLeader() error {
	expiry := strconv.FormatInt(time.Now().Unix()-int64(l.Timeout), 10)
	now := strconv.FormatInt(time.Now().Unix(), 10)

	_, err := l.Dynamo.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(l.Table),
		Item: map[string]*dynamodb.AttributeValue{
			"LockName":   &dynamodb.AttributeValue{S: aws.String("Leader")},
			"LeaderName": &dynamodb.AttributeValue{S: aws.String(l.Name)},
			"LastUpdate": &dynamodb.AttributeValue{N: aws.String(now)},
		},
		// Only take leadership if no leader is assigned, or if the current leader
		// hasn't checked in in the last +timeout+ seconds.
		ConditionExpression: aws.String("attribute_not_exists(LeaderName) OR LastUpdate <= :expiry"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":expiry": &dynamodb.AttributeValue{N: aws.String(expiry)},
		},
		ReturnValues: aws.String("ALL_OLD"),
	})

	return err
}
