package kafka

import (
	"context"
	"reflect"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/golang/protobuf/proto"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/pkg/errors"
	kafka "github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers string `mapstructure:"brokers" validate:"required"`
	Topic   string `mapstructure:"topic" validate:"required"`
	KeyPath string `mapstructure:"key_path"`
}

type ProtoReflector interface {
	ProtoReflect() protoreflect.Message
}

type Sink struct {
	writer *kafka.Writer
}

func New() plugins.Syncer {
	return new(Sink)
}

func (s *Sink) Sink(ctx context.Context, configMap map[string]interface{}, in <-chan interface{}) error {
	var config Config
	if err := utils.BuildConfig(configMap, &config); err != nil {
		return err
	}

	s.writer = createWriter(config)
	for val := range in {
		if err := s.push(ctx, config, val); err != nil {
			return err
		}
	}

	if err := s.writer.Close(); err != nil {
		return errors.Wrap(err, "failed to close writer")
	}

	return nil
}

func (s *Sink) push(ctx context.Context, config Config, payload interface{}) error {
	// struct needs to be cast to pointer to implement proto methods
	payload = castModelToPointer(payload)

	kafkaValue, err := s.buildValue(payload)
	if err != nil {
		return err
	}

	kafkaKey, err := s.buildKey(payload, config.KeyPath)
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
	keyString, keyJsonName, err := s.extractKeyFromPayload(fieldName, payload)
	if err != nil {
		return nil, err
	}

	// get descriptor
	reflector, ok := payload.(ProtoReflector)
	if !ok {
		return nil, errors.New("not a valid proto payload")
	}
	messageDescriptor := reflector.ProtoReflect().Descriptor()
	fieldDescriptor := messageDescriptor.Fields().ByJSONName(keyJsonName)
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

func castModelToPointer(value interface{}) interface{} {
	vp := reflect.New(reflect.TypeOf(value))
	vp.Elem().Set(reflect.ValueOf(value))

	return vp.Interface()
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
		return &Sink{}
	}); err != nil {
		panic(err)
	}
}
