package database

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
)

type Config struct {
	Host      string `json:"host"`
	Port      uint16 `json:"port"`
	User      string `json:"user"`
	Pass      string `json:"pass"`
	TrustCert bool   `json:"trust_cert"`
}

func errorf(format string, v ...interface{}) error {
	errString := fmt.Sprintf(format, v...)
    return fmt.Errorf("SequelGo: Unable to get database config: %s", errString)
}

func GetConfig() (*Config, error) {
	userHomeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, errorf(err.Error())
	}

	var configFilePath string
	if runtime.GOOS == "windows" {
		configFilePath = userHomeDir + "\\.sequelgo\\settings.json"
	} else {
		configFilePath = userHomeDir + "/.sequelgo/settings.json"
	}
	fileBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, errorf("Could not open config file %s", configFilePath)
	}

	var config Config
	err = json.Unmarshal(fileBytes, &config)
	if err != nil {
		return nil, errorf(err.Error())
	}

	return &config, err
}
