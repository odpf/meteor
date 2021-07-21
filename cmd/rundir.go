package cmd

import (
	goLog "log"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/core/recipe"
	"github.com/odpf/meteor/logger"
)

// run recipes in a given directory path
func rundir(dirPath string) {
	c, err := config.LoadConfig()
	if err != nil {
		goLog.Fatal(err)
	}
	log := logger.New(c.LogLevel)

	reader := recipe.NewReader()
	recipeList, err := reader.ReadDir(dirPath)
	if err != nil {
		log.Fatal(err)
	}
	runner, cleanFn := initRunner(c, log)
	defer cleanFn()

	failedRecipes, err := runner.RunMultiple(recipeList)
	if err != nil {
		log.Error(err)
	} else {
		log.WithField("failed_recipes", failedRecipes).Info("Done!")
	}
}
