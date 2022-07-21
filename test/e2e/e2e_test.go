//go:build integration
// +build integration

package e2e_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.buf.build/odpf/gw/odpf/proton/odpf/assets"

	"github.com/odpf/meteor/cmd"
	"github.com/odpf/meteor/config"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	_ "github.com/odpf/meteor/plugins/extractors"
	_ "github.com/odpf/meteor/plugins/processors"
	_ "github.com/odpf/meteor/plugins/sinks"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var (
	db     *sql.DB
	conn   *kafka.Conn
	broker kafka.Broker
)

const (
	testDB     = "test_db"
	user       = "test_user"
	pass       = "admin"
	mysqlHost  = "localhost:3306"
	brokerHost = "localhost:9093"
	testTopic  = "topic-a"
	partition  = 0
)

func TestMain(m *testing.M) {
	// generate purge function
	mysqlPurgeContainer, err := mysqlDockerSetup()
	if err != nil {
		return
	}
	kafkaPurgeContainer, err := kafkaDockerSetup()
	if err != nil {
		return
	}

	// setup and populate data for testing
	if err := setupMySQL(); err != nil {
		log.Fatal(err)
	}
	if err := setupKafka(); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	if err = conn.Close(); err != nil {
		return
	}
	if err = db.Close(); err != nil {
		return
	}

	// purge containers
	if err := mysqlPurgeContainer(); err != nil {
		log.Fatal(err)
	}
	if err := kafkaPurgeContainer(); err != nil {
		log.Fatal(err)
	}

	os.Exit(code)
}

// TestMySqlToKafka tests the recipe from source to sink completely
func TestMySqlToKafka(t *testing.T) {
	err := setupKafka()
	if err != nil {
		t.Fatal(err)
	}

	var sinkData []*v1beta2.Asset
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		err = listenToTopic(ctx, testTopic, &sinkData)
		if err != nil {
			t.Error(err)
		}
	}()

	// run mysql_kafka.yml file
	cfg, err := config.Load()
	if err != nil {
		t.Error(err)
	}
	command := cmd.New(utils.Logger, nil, cfg)
	command.SetArgs([]string{"run", "mysql_kafka.yml"})
	if err := command.Execute(); err != nil {
		if strings.HasPrefix(err.Error(), "unknown command ") {
			if !strings.HasSuffix(err.Error(), "\n") {
				t.Fatal(err)
			}
			t.Fatal(err)
		} else {
			t.Fatal(err)
		}
	}

	time.Sleep(2 * time.Second)        // this is to wait consumer to finish adding data to sinkData
	cancel()                           // cancel will cancel context, hinting the consumer to end
	time.Sleep(100 * time.Millisecond) // this is to give time for the consumer to closing all its connections

	expected := getExpectedTables()
	assert.Equal(t, len(getExpectedTables()), len(sinkData))
	for tableNum := 0; tableNum < len(getExpectedTables()); tableNum++ {
		assert.Equal(t, expected[tableNum].Resource.Urn, sinkData[tableNum].Resource.Urn)
		assert.Equal(t, expected[tableNum].Resource.Name, sinkData[tableNum].Resource.Name)
		assert.Equal(t, len(expected[tableNum].Schema.Columns), len(sinkData[tableNum].Schema.Columns))
	}
}

// listenToTopic listens to a topic and stores the data in sinkData
func listenToTopic(ctx context.Context, topic string, data *[]*v1beta2.Asset) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerHost},
		Topic:   topic,
	})
	defer func(reader *kafka.Reader) {
		if err := reader.Close(); err != nil {
			return
		}
	}(reader)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			break

		}
		var convertMsg assets.Table
		if err := proto.Unmarshal(msg.Value, &convertMsg); err != nil {
			return errors.Wrap(err, "failed to parse kafka message")
		}
		*data = append(*data, &convertMsg)
	}

	return nil
}

// setupKafka initializes kafka broker with topic and partition
func setupKafka() error {
	conn, err := kafka.DialLeader(context.TODO(), "tcp", net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)), testTopic, partition)
	if err != nil {
		return errors.Wrap(err, "failed to setup kafka connection")
	}
	defer func(conn *kafka.Conn) {
		if err := conn.Close(); err != nil {
			return
		}
	}(conn)

	if err := conn.DeleteTopics(testTopic); err != nil {
		return errors.Wrap(err, "failed to delete topic")
	}
	if err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             testTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		return errors.Wrap(err, "failed to create topic")
	}

	return nil
}

// setupMySQL initializes mysql database
func setupMySQL() (err error) {
	// create database, user and grant access
	if err = execute(db, []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB),
		fmt.Sprintf("CREATE DATABASE %s", testDB),
		fmt.Sprintf("USE %s;", testDB),
		fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';`, user, pass),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';`, user),
	}); err != nil {
		return errors.Wrap(err, "failed to create database")
	}

	// create and populate tables
	if err = execute(db, []string{
		"CREATE TABLE applicant (applicant_id int, last_name varchar(255), first_name varchar(255));",
		"INSERT INTO applicant VALUES (1, 'test1', 'test11');",
		"CREATE TABLE jobs (job_id int, job varchar(255), department varchar(255));",
		"INSERT INTO jobs VALUES (2, 'test2', 'test22');",
	}); err != nil {
		return errors.Wrap(err, "failed to populate database")
	}

	return
}

// execute executes a list of sql statements
func execute(db *sql.DB, queries []string) (err error) {
	for _, query := range queries {
		_, err = db.Exec(query)
		if err != nil {
			return
		}
	}

	return
}

// kafkaDockerSetup sets up a kafka docker container
func kafkaDockerSetup() (purge func() error, err error) {
	// kafka setup test
	kafkaOpts := dockertest.RunOptions{
		Repository: "moeenz/docker-kafka-kraft",
		Tag:        "latest",
		Env: []string{
			"KRAFT_CONTAINER_HOST_NAME=kafka",
		},
		ExposedPorts: []string{"9093"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9093": {
				{HostIP: "localhost", HostPort: "9093"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	kafkaRetryFn := func(resource *dockertest.Resource) (err error) {
		// create client
		if conn, err = kafka.Dial("tcp", brokerHost); err != nil {
			return errors.Wrap(err, "failed to kafka create client")
		}
		if broker, err = conn.Controller(); err != nil {
			return errors.Wrap(err, "failed to generate broker request")
		}
		return
	}
	purgeContainer, err := utils.CreateContainer(kafkaOpts, kafkaRetryFn)
	if err != nil {
		log.Fatal(err)
	}

	return purgeContainer, nil
}

// mysqlDockerSetup sets up a mysql docker container
func mysqlDockerSetup() (purge func() error, err error) {
	// mysql setup test
	mysqlOpts := dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "latest",
		Env: []string{
			"MYSQL_ROOT_PASSWORD=" + pass,
		},
		ExposedPorts: []string{"3306"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3306": {
				{HostIP: "0.0.0.0", HostPort: "3306"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	mysqlRetryFn := func(resource *dockertest.Resource) (err error) {
		db, err = sql.Open("mysql", fmt.Sprintf("root:%s@tcp(%s)/", pass, mysqlHost))
		if err != nil {
			return errors.Wrap(err, "failed to create mysql client")
		}
		return db.Ping()
	}
	purgeContainer, err := utils.CreateContainer(mysqlOpts, mysqlRetryFn)
	if err != nil {
		log.Fatal(err)
	}

	return purgeContainer, nil
}

// getExpectedTables returns the expected tables
func getExpectedTables() []*v1beta2.Asset {
	return []*v1beta2.Asset{
		{
			Resource: &commonv1beta1.Resource{
				Urn:  testDB + ".applicant",
				Name: "applicant",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:       "applicant_id",
						DataType:   "int",
						IsNullable: true,
						Length:     0,
					},
					{
						Name:       "first_name",
						DataType:   "varchar",
						IsNullable: true,
						Length:     255,
					},
					{
						Name:       "last_name",
						DataType:   "varchar",
						IsNullable: true,
						Length:     255,
					},
				},
			},
		},
		{
			Resource: &commonv1beta1.Resource{
				Urn:  testDB + ".jobs",
				Name: "jobs",
			},
			Schema: &facetsv1beta1.Columns{
				Columns: []*facetsv1beta1.Column{
					{
						Name:       "department",
						DataType:   "varchar",
						IsNullable: true,
						Length:     255,
					},
					{
						Name:       "job",
						DataType:   "varchar",
						IsNullable: true,
						Length:     255,
					},
					{
						Name:       "job_id",
						DataType:   "int",
						IsNullable: true,
						Length:     0,
					},
				},
			},
		},
	}
}
