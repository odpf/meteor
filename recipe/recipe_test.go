package recipe_test

import (
	"testing"

	"github.com/odpf/meteor/recipe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecipeGetLine(t *testing.T) {
	reader := recipe.NewReader()
	r, err := reader.Read("./testdata/recipe-read-line.yaml")
	require.NoError(t, err)
	require.Len(t, r, 1)
	rcp := r[0]

	t.Run("should return source line and column", func(t *testing.T) {
		assert.Equal(t, 3, rcp.Source.Node.Type.Line)
		assert.Equal(t, 9, rcp.Source.Node.Type.Column)
	})

	t.Run("should return processors line and column", func(t *testing.T) {
		assert.Equal(t, 9, rcp.Processors[0].Node.Name.Line)
		assert.Equal(t, 11, rcp.Processors[0].Node.Name.Column)

		assert.Equal(t, 14, rcp.Processors[1].Node.Name.Line)
		assert.Equal(t, 11, rcp.Processors[1].Node.Name.Column)
	})

	t.Run("should return sinks line and column", func(t *testing.T) {
		assert.Equal(t, 20, rcp.Sinks[0].Node.Name.Line)
		assert.Equal(t, 11, rcp.Sinks[0].Node.Name.Column)

		assert.Equal(t, 25, rcp.Sinks[1].Node.Name.Line)
		assert.Equal(t, 11, rcp.Sinks[1].Node.Name.Column)
	})
}
