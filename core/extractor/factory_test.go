package extractor_test

import (
	"context"
	"testing"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/registry"
	"github.com/stretchr/testify/assert"
)

type mockExtractor struct {
}

func (p *mockExtractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	return nil
}

func newMockExtractor() core.Extractor {
	return new(mockExtractor)
}

func TestFactoryGet(t *testing.T) {
	t.Run("should return not found error if extractor does not exist", func(t *testing.T) {
		name := "wrong-name"

		factory := registry.NewFactory()
		factory.Register("mock", newMockExtractor)

		_, err := factory.Get(name)
		assert.Equal(t, registry.NotFoundError{name}, err)
	})

	t.Run("should return a new instance of extractor with given name", func(t *testing.T) {
		name := "mock"

		factory := registry.NewFactory()
		factory.Register(name, newMockExtractor)

		extr, err := factory.Get(name)
		if err != nil {
			t.Error(err.Error())
		}

		assert.Equal(t, new(mockExtractor), extr)  // Same type
		assert.True(t, new(mockExtractor) != extr) // Different instance
	})
}

func TestFactoryRegister(t *testing.T) {
	t.Run("should add extractor factory with given key", func(t *testing.T) {
		factory := registry.NewFactory()
		factory.Register("mock1", newMockExtractor)
		factory.Register("mock2", newMockExtractor)

		mock1, err := factory.Get("mock1")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockExtractor), mock1)  // Same type
		assert.True(t, new(mockExtractor) != mock1) // Different instance

		mock2, err := factory.Get("mock2")
		if err != nil {
			t.Error(err.Error())
		}
		assert.Equal(t, new(mockExtractor), mock2)  // Same type
		assert.True(t, new(mockExtractor) != mock2) // Different instance
	})
}
