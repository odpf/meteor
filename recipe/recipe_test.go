package recipe_test

import (
	"sort"
	"testing"

	"github.com/odpf/meteor/recipe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecipeGetLine tests recipe by line number
func TestRecipeGetLine(t *testing.T) {
	reader := recipe.NewReader()
	r, err := reader.Read("./testdata/recipe-read-line.yaml")
	require.NoError(t, err)
	require.Len(t, r, 1)
	rcp := r[0]

	t.Run("should return source line and column", func(t *testing.T) {
		assert.Equal(t, 3, rcp.Source.Node.Name.Line)
		assert.Equal(t, 9, rcp.Source.Node.Name.Column)
	})

	t.Run("should return config source lines", func(t *testing.T) {
		expectedLineNum := []int{5, 6, 7}
		var lineNum []int
		srcConfig := rcp.Source.Node.Config
		for _, j := range srcConfig {
			lineNum = append(lineNum, j.Line)
		}
		sort.Ints(lineNum)
		assert.Equal(t, expectedLineNum, lineNum)
	})

	t.Run("should return config source line for a specific config key", func(t *testing.T) {
		expectedLineNum := 6
		srcConfigKey := rcp.Source.Node.Config["srcKey2"]
		assert.Equal(t, expectedLineNum, srcConfigKey.Line)
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

// TestRecipeGetLineBySrcTypeTag tests recipe source with tag `type` by line number
func TestRecipeGetLineBySrcTypeTag(t *testing.T) {
	reader := recipe.NewReader()
	r, err := reader.Read("./testdata/src- typeTag-recipe-read-line.yaml")
	require.NoError(t, err)
	require.Len(t, r, 1)
	rcp := r[0]

	t.Run("should return source line and column", func(t *testing.T) {
		assert.Equal(t, 3, rcp.Source.Node.Type.Line)
		assert.Equal(t, 9, rcp.Source.Node.Type.Column)
	})

	t.Run("should return config source lines", func(t *testing.T) {
		expectedLineNum := []int{5, 6, 7}
		var lineNum []int
		srcConfig := rcp.Source.Node.Config
		for _, j := range srcConfig {
			lineNum = append(lineNum, j.Line)
		}
		sort.Ints(lineNum)
		assert.Equal(t, expectedLineNum, lineNum)
	})

	t.Run("should return config source line for a specific config key", func(t *testing.T) {
		expectedLineNum := 6
		srcConfigKey := rcp.Source.Node.Config["srcKey2"]
		assert.Equal(t, expectedLineNum, srcConfigKey.Line)
	})
}