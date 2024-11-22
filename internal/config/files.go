package config

import (
	"os"
	"path/filepath"
)

// Variables defining paths to the certs we generated 
var (
	CAFile					= configFile("ca.pem")
	ServerCertFile			= configFile("server.pem")
	ServerKeyFile			= configFile("server-key.pem")
	RootClientCertFile 		= configFile("client.pem")
	RootClientKeyFile		= configFile("client-key.pem")
	NobodyClientCertFile 	= configFile("nobody-client.pem")
	NobodyClientKeyFile		= configFile("nobody-client-key.pem")
	ACLModelFile			= configFile("model.conf")
	ACLPolicyFile			= configFile("policy.csv")
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(homeDir, ".Proglog", filename)
}