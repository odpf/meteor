package recipe_test

import (
	"errors"
	"os"
	"testing"

	"github.com/odpf/meteor/recipe"
	"github.com/stretchr/testify/assert"
)

var (
	emptyConfigPath = ""
	username        = "admin"
	password        = "1234"
)

func TestReaderRead(t *testing.T) {
	t.Run("should return error if file is not found", func(t *testing.T) {
		reader := recipe.NewReader(testLog, emptyConfigPath)

		_, err := reader.Read("./wrong-path.yaml")
		assert.NotNil(t, err)
	})

	t.Run("should return error if recipe is not parsed correctly", func(t *testing.T) {
		reader := recipe.NewReader(testLog, emptyConfigPath)

		_, err := reader.Read("./testdata/wrong-format.txt")
		assert.NotNil(t, err)
	})

	t.Run("should return recipe from a path given in parameter", func(t *testing.T) {
		t.Run("where recipe has a name", func(t *testing.T) {
			reader := recipe.NewReader(testLog, emptyConfigPath)

			recipes, err := reader.Read("./testdata/testdir/test-recipe.yaml")
			if err != nil {
				t.Fatal(err)
			}
			expectedRecipes := []recipe.Recipe{
				{
					Name: "test-recipe",
					Source: recipe.PluginRecipe{
						Name:  "test-source",
						Scope: "my-scope",
						Config: map[string]interface{}{
							"foo": "bar",
						},
					},
					Sinks: []recipe.PluginRecipe{
						{
							Name:   "test-sink",
							Config: map[string]interface{}{},
						},
					},
				}}

			assert.Len(t, recipes, len(expectedRecipes))
			for i, r := range recipes {
				compareRecipes(t, expectedRecipes[i], r)
			}
		})

		t.Run("where recipe does not have a name", func(t *testing.T) {
			reader := recipe.NewReader(testLog, emptyConfigPath)

			recipes, err := reader.Read("./testdata/testdir/test-recipe-no-name.yaml")
			if err != nil {
				t.Fatal(err)
			}
			expectedRecipes := []recipe.Recipe{
				{
					Name:    "test-recipe-no-name",
					Version: "v1beta1",
					Source: recipe.PluginRecipe{
						Name:  "test-source",
						Scope: "my-scope",
						Config: map[string]interface{}{
							"foo": "bar",
						},
					},
					Sinks: []recipe.PluginRecipe{
						{
							Name:   "test-sink",
							Config: map[string]interface{}{},
						},
					},
				}}

			assert.Len(t, recipes, len(expectedRecipes))
			for i, r := range recipes {
				compareRecipes(t, expectedRecipes[i], r)
			}
		})
	})

	t.Run("should parse variable in recipe with value from env vars prefixed with METEOR_", func(t *testing.T) {
		os.Setenv("METEOR_SOURCE_USERNAME", username)
		os.Setenv("METEOR_SOURCE_PASSWORD", password)
		defer func() {
			os.Unsetenv("METEOR_SOURCE_USERNAME")
			os.Unsetenv("METEOR_SOURCE_PASSWORD")
		}()

		reader := recipe.NewReader(testLog, emptyConfigPath)
		recipes, err := reader.Read("./testdata/testdir/test-recipe-variables.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expectedRecipes := []recipe.Recipe{
			{
				Name: "test-recipe",
				Source: recipe.PluginRecipe{
					Name:  "test-source",
					Scope: "my-scope",
					Config: map[string]interface{}{
						"username": username,
						"password": password,
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
				Processors: []recipe.PluginRecipe{
					{
						Name:   "test-processor",
						Config: map[string]interface{}{},
					},
				},
			}}

		assert.Len(t, recipes, len(expectedRecipes))
		for i, r := range recipes {
			compareRecipes(t, expectedRecipes[i], r)
		}
	})

	t.Run("should return error if directory is not found", func(t *testing.T) {
		reader := recipe.NewReader(testLog, emptyConfigPath)
		_, err := reader.Read("./testdata/wrong-dir")
		assert.NotNil(t, err)
	})

	t.Run("should return error if path is not a directory", func(t *testing.T) {
		reader := recipe.NewReader(testLog, emptyConfigPath)
		_, err := reader.Read("./testdata/wrong-format.txt")
		assert.NotNil(t, err)
	})

	t.Run("should return recipes on success", func(t *testing.T) {
		os.Setenv("METEOR_SOURCE_USERNAME", username)
		os.Setenv("METEOR_SOURCE_PASSWORD", password)
		defer func() {
			os.Unsetenv("METEOR_SOURCE_USERNAME")
			os.Unsetenv("METEOR_SOURCE_PASSWORD")
		}()

		reader := recipe.NewReader(testLog, emptyConfigPath)
		results, err := reader.Read("./testdata/testdir")
		if err != nil {
			t.Fatal(err)
		}
		expected := []recipe.Recipe{
			{
				Name: "test-recipe-no-name",
				Source: recipe.PluginRecipe{
					Name:  "test-source",
					Scope: "my-scope",
					Config: map[string]interface{}{
						"foo": "bar",
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
			},
			{
				Name: "test-recipe",
				Source: recipe.PluginRecipe{
					Name:  "test-source",
					Scope: "my-scope",
					Config: map[string]interface{}{
						"username": username,
						"password": password,
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
				Processors: []recipe.PluginRecipe{
					{
						Name:   "test-processor",
						Config: map[string]interface{}{},
					},
				},
			},
			{
				Name: "test-recipe",
				Source: recipe.PluginRecipe{
					Name:  "test-source",
					Scope: "my-scope",
					Config: map[string]interface{}{
						"foo": "bar",
					},
				},
				Sinks: []recipe.PluginRecipe{
					{
						Name:   "test-sink",
						Config: map[string]interface{}{},
					},
				},
			},
		}

		assert.Len(t, results, len(expected))
		for i, r := range results {
			compareRecipes(t, expected[i], r)
		}
	})

	// Testing populateData() with various environment configs!!
	t.Run("should read config file in current directory", func(t *testing.T) {
		reader := recipe.NewReader(testLog, "sample_config.yaml")
		results, err := reader.Read("./testdata/testdir/test-recipe-variables.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expected := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name:  "test-source",
				Scope: "my-scope",
				Config: map[string]interface{}{
					"username": username,
					"password": password,
				},
			},
			Sinks: []recipe.PluginRecipe{
				{
					Name:   "test-sink",
					Config: map[string]interface{}{},
				},
			},
			Processors: []recipe.PluginRecipe{
				{
					Name:   "test-processor",
					Config: map[string]interface{}{},
				},
			},
		}
		compareRecipes(t, expected, results[0])
	})

	t.Run("should read config file in other directory", func(t *testing.T) {
		reader := recipe.NewReader(testLog, "testdata/config2.yaml")
		results, err := reader.Read("./testdata/testdir/test-recipe-variables.yaml")
		if err != nil {
			t.Fatal(err)
		}
		expected := recipe.Recipe{
			Name: "test-recipe",
			Source: recipe.PluginRecipe{
				Name:  "test-source",
				Scope: "my-scope",
				Config: map[string]interface{}{
					"username": username,
					"password": password,
				},
			},
			Sinks: []recipe.PluginRecipe{
				{
					Name:   "test-sink",
					Config: map[string]interface{}{},
				},
			},
			Processors: []recipe.PluginRecipe{
				{
					Name:   "test-processor",
					Config: map[string]interface{}{},
				},
			},
		}
		compareRecipes(t, expected, results[0])
	})

	t.Run("should return error if version is missing/incorrect", func(t *testing.T) {
		reader := recipe.NewReader(testLog, "testdata/config2.yaml")
		_, err := reader.Read("./testdata/missing-version.yaml")
		errors.Is(err, recipe.ErrInvalidRecipeVersion)

		_, err = reader.Read("./testdata/incorrect-version.yaml")
		errors.Is(err, recipe.ErrInvalidRecipeVersion)

		_, err = reader.Read("./testdata/dir_2") // error is logged in case of a directory
		assert.Nil(t, err)
	})
}

func compareRecipes(t *testing.T, expected, actual recipe.Recipe) {
	assert.Equal(t, expected.Name, actual.Name)
	assert.Equal(t, len(expected.Sinks), len(actual.Sinks))
	assert.Equal(t, len(expected.Processors), len(actual.Processors))

	assert.Equal(t, expected.Source.Name, actual.Source.Name)
	assert.Equal(t, expected.Source.Scope, actual.Source.Scope)
	assert.Equal(t, expected.Source.Config, actual.Source.Config)
	for i := range actual.Sinks {
		assert.Equal(t, expected.Sinks[i].Name, actual.Sinks[i].Name)
		assert.Equal(t, expected.Sinks[i].Config, actual.Sinks[i].Config)
	}
	for i := range actual.Processors {
		assert.Equal(t, expected.Processors[i].Name, actual.Processors[i].Name)
		assert.Equal(t, expected.Processors[i].Config, actual.Processors[i].Config)
	}
}
