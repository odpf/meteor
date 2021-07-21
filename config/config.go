package config

import (
	"fmt"

	"github.com/jeremywohl/flatten"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Config struct {
	LogLevel      string `mapstructure:"LOG_LEVEL" default:"info"`
	StatsdEnabled bool   `mapstructure:"STATSD_ENABLED" default:"false"`
	StatsdHost    string `mapstructure:"STATSD_HOST" default:"localhost:8125"`
	StatsdPrefix  string `mapstructure:"STATSD_PREFIX" default:"meteor"`
}

// LoadConfig returns application configuration
func LoadConfig() (c Config, err error) {
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("../")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			fmt.Println("config file was not found. Env vars and defaults will be used")
		} else {
			return c, err
		}
	}

	bindEnvVars()
	defaults.SetDefaults(&c)
	err = viper.Unmarshal(&c)
	if err != nil {
		return c, fmt.Errorf("unable to unmarshal config to struct: %v\n", err)
	}

	return
}

func bindEnvVars() {
	err, configKeys := getFlattenedStructKeys(Config{})
	if err != nil {
		panic(err)
	}

	// Bind each conf fields to environment vars
	for key := range configKeys {
		err := viper.BindEnv(configKeys[key])
		if err != nil {
			panic(err)
		}
	}
}

func getFlattenedStructKeys(config Config) (error, []string) {
	var structMap map[string]interface{}
	err := mapstructure.Decode(config, &structMap)
	if err != nil {
		return err, nil
	}

	flat, err := flatten.Flatten(structMap, "", flatten.DotStyle)
	if err != nil {
		return err, nil
	}

	keys := make([]string, 0, len(flat))
	for k := range flat {
		keys = append(keys, k)
	}

	return nil, keys
}
