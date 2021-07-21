package cmd

import (
	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/logger"
)

// run recipes in a given directory path
func rundir(dirPath string) {
	c, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}
	log := logger.New(c.LogLevel)

	reader := recipe.NewReader()
	recipeList, err := reader.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}
	runner, cleanFn := initRunner(c, log)
	defer cleanFn()
	faileds, err := runner.RunMultiple(recipeList)
	if err != nil {
		log.Error(err)
	} else {
		log.WithField("failed_recipes", faileds).Info("Done!")
	}
}
