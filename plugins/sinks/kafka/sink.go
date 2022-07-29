package kafka

import (
	"context"
	_ "embed"
	"reflect"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

//go:embed README.md
var summary string

type Config struct {
	Brokers string `mapstructure:"brokers" validate:"required"`
	Topic   string `mapstructure:"topic" validate:"required"`
	KeyPath string `mapstructure:"key_path"`
}

var info = plugins.Info{
	Description: "Sink metadata to Apache Kafka topic",
	Summary:     summary,
	Tags:        []string{"kafka", "topic", "sink"},
	SampleConfig: `
	# Kafka broker addresses
	brokers: "localhost:9092"
	# The Kafka topic to write to
	topic: sample-topic-name
	# The path to the key field in the payload
	key_path: xxx`,
}

type ProtoReflector interface {
	ProtoReflect() protoreflect.Message
}

type Sink struct {
	plugins.BasePlugin
	writer *kafka.Writer
	config Config
	logger log.Logger
}

func New(logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	s.writer = createWriter(s.config)

	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		if err := s.push(ctx, record.Data()); err != nil {
			return err
		}
	}

	return
}

func (s *Sink) Close() (err error) {
	return s.writer.Close()
}

func (s *Sink) push(ctx context.Context, payload interface{}) error {
	kafkaValue, err := s.buildValue(payload)
	if err != nil {
		return err
	}

	kafkaKey, err := s.buildKey(payload, s.config.KeyPath)
	if err != nil {
		return err
	}

	err = s.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   kafkaKey,
			Value: kafkaValue,
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to write messages")
	}

	return nil
}

func (s *Sink) buildValue(value interface{}) ([]byte, error) {
	protoBytes, err := proto.Marshal(value.(proto.Message))
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize payload as a protobuf message")
	}
	return protoBytes, nil
}

// we can optimize this by caching descriptor and key path
func (s *Sink) buildKey(payload interface{}, keyPath string) ([]byte, error) {
	if keyPath == "" {
		return nil, nil
	}

	// extract key field name and value
	fieldName, err := s.getTopLevelKeyFromPath(keyPath)
	if err != nil {
		return nil, err
	}
	keyString, keyJSONName, err := s.extractKeyFromPayload(fieldName, payload)
	if err != nil {
		return nil, err
	}

	// get descriptor
	reflector, ok := payload.(ProtoReflector)
	if !ok {
		return nil, errors.New("not a valid proto payload")
	}
	messageDescriptor := reflector.ProtoReflect().Descriptor()
	fieldDescriptor := messageDescriptor.Fields().ByJSONName(keyJSONName)
	if fieldDescriptor == nil {
		return nil, errors.New("failed to build kafka key")
	}

	// populate message
	dynamicMsgKey := dynamicpb.NewMessage(messageDescriptor)
	dynamicMsgKey.Set(fieldDescriptor, protoreflect.ValueOfString(keyString))
	return proto.Marshal(dynamicMsgKey)
}

func (s *Sink) extractKeyFromPayload(fieldName string, value interface{}) (string, string, error) {
	valueOf := reflect.ValueOf(value)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return "", "", errors.New("invalid data")
	}

	structField, ok := valueOf.Type().FieldByName(fieldName)
	if !ok {
		return "", "", errors.New("invalid path, unknown field")
	}
	jsonName := strings.Split(structField.Tag.Get("json"), ",")[0]

	fieldVal := valueOf.FieldByName(fieldName)
	if !fieldVal.IsValid() || fieldVal.IsZero() {
		return "", "", errors.New("invalid path, unknown field")
	}
	if fieldVal.Type().Kind() != reflect.String {
		return "", "", errors.Errorf("unsupported key type, should be string found: %s", fieldVal.Type().String())
	}

	return fieldVal.String(), jsonName, nil
}

func (s *Sink) getTopLevelKeyFromPath(keyPath string) (string, error) {
	keyPaths := strings.Split(keyPath, ".")
	if len(keyPaths) < 2 {
		return "", errors.New("invalid path, require at least one field name e.g.: .Urn")
	}
	if len(keyPaths) > 2 {
		return "", errors.New("invalid path, doesn't support nested field names yet")
	}
	return keyPaths[1], nil
}

func createWriter(config Config) *kafka.Writer {
	brokers := strings.Split(config.Brokers, ",")
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    config.Topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func init() {
	if err := registry.Sinks.Register("kafka", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
