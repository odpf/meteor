//go:build plugins
// +build plugins

package kafka_test

import (
	"context"
	"errors"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"google.golang.org/protobuf/types/known/anypb"
	"log"
	"net"

	"github.com/odpf/meteor/test/utils"

	"os"
	"strconv"
	"testing"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/kafka"
	"github.com/odpf/meteor/test/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	kafkaLib "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

var (
	brokerHost = "localhost:9093"
)

func TestMain(m *testing.M) {
	var conn *kafkaLib.Conn
	var broker kafkaLib.Broker

	// setup test
	opts := dockertest.RunOptions{
		Repository: "moeenz/docker-kafka-kraft",
		Tag:        "229159a4a45b",
		Env: []string{
			"KRAFT_CONTAINER_HOST_NAME=1",
		},
		ExposedPorts: []string{"9093"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9093": {
				{HostIP: "localhost", HostPort: "9093"},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		// create client
		conn, err = kafkaLib.Dial("tcp", brokerHost)
		if err != nil {
			return
		}

		// healthcheck
		brokerList, err := conn.Brokers()
		if err != nil {
			return
		}
		if len(brokerList) == 0 {
			err = errors.New("not ready")
			return
		}

		broker, err = conn.Controller()
		if err != nil {
			conn.Close()
			return
		}

		return
	}
	purgeContainer, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// setup and populate kafka for testing
	if err := setup(broker); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	conn.Close()

	// purge container
	if err := purgeContainer(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := newExtractor().Init(context.TODO(), map[string]interface{}{
			"wrong-config": "wrong-value",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should emit list of topic metadata", func(t *testing.T) {
		ctx := context.TODO()
		extr := newExtractor()
		err := extr.Init(ctx, map[string]interface{}{
			"broker": brokerHost,
			"label":  "my-kafka-cluster",
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		data, err := anypb.New(&v1beta2.Topic{
			Profile: &v1beta2.TopicProfile{
				NumberOfPartitions: 1,
			},
		})

		// assert results with expected data
		expected := []models.Record{
			models.NewRecord(&v1beta2.Asset{
				Urn:     "kafka::my-kafka-cluster/meteor-test-topic-1",
				Name:    "meteor-test-topic-1",
				Service: "kafka",
				Type:    "topic",
				Data:    data,
			}),
			models.NewRecord(&v1beta2.Asset{
				Urn:     "kafka::my-kafka-cluster/meteor-test-topic-2",
				Name:    "meteor-test-topic-2",
				Service: "kafka",
				Type:    "topic",
				Data:    data,
			}),
			models.NewRecord(&v1beta2.Asset{
				Urn:     "kafka::my-kafka-cluster/meteor-test-topic-3",
				Name:    "meteor-test-topic-3",
				Service: "kafka",
				Type:    "topic",
				Data:    data,
			}),
		}
		// We need this function because the extractor cannot guarantee order
		// so comparing expected slice and result slice will not be consistent
		assertResults(t, expected, emitter.Get())
	})
}

func setup(broker kafkaLib.Broker) (err error) {
	// create broker connection to create topics
	var conn *kafkaLib.Conn
	conn, err = kafkaLib.Dial("tcp", net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return
	}
	defer conn.Close()

	// create topics
	topicConfigs := []kafkaLib.TopicConfig{
		{Topic: "meteor-test-topic-1", NumPartitions: 1, ReplicationFactor: 1},
		{Topic: "meteor-test-topic-2", NumPartitions: 1, ReplicationFactor: 1},
		{Topic: "meteor-test-topic-3", NumPartitions: 1, ReplicationFactor: 1},
	}
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		return
	}

	return
}

func newExtractor() *kafka.Extractor {
	return kafka.New(utils.Logger)
}

// This function compares two slices without concerning about the order
func assertResults(t *testing.T, expected []models.Record, result []models.Record) {
	assert.Len(t, result, len(expected))

	expectedMap := make(map[string]*v1beta2.Asset)
	for _, record := range expected {
		asset := record.Data()
		expectedMap[asset.Urn] = asset
	}

	for _, record := range result {
		topic := record.Data()
		assert.Contains(t, expectedMap, topic.Urn)
		assert.Equal(t, expectedMap[topic.Urn], topic)

		// delete entry to make sure there is no duplicate
		delete(expectedMap, topic.Urn)
	}
}
