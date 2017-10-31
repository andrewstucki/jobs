// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/andrewstucki/jobs"
	"github.com/andrewstucki/jobs/raft"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tidwall/redcon"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "jobs",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := serve(ServerConfig{
			Host: "127.0.0.1",
			Port: 8085,
			RaftStoreConfig: raft.RaftStoreConfig{
				EnableSingle:     true,
				StorageDirectory: "job_db",
				Bind:             "127.0.0.1",
				BindPort:         8086,
				ID:               "job_db",
			},
		}); err != nil {
			log.Fatal(err)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.server.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".server" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".server")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

type ServerConfig struct {
	raft.RaftStoreConfig
	Host string
	Port int
}

type JobPayload struct {
	Data     string `json:"data"`
	Priority uint32 `json:"priority"`
	ID       string `json:"id"`
}

func serve(config ServerConfig) error {
	fmt.Println("Initializing store")
	store, err := raft.NewRaftStore(config.RaftStoreConfig)
	if err != nil {
		return err
	}
	defer store.Close()

	fmt.Println("Initializing client")
	client, err := raft.NewRaftClient(store)
	if err != nil {
		return err
	}
	defer client.Close()

	store.WaitForLeader()

	fmt.Println("Initializing queue")
	queue, err := jobs.NewSortedPriorityQueue(client)
	if err != nil {
		return err
	}

	fmt.Println("Initializing redis")
	return redcon.ListenAndServe(fmt.Sprintf("%s:%d", config.Host, config.Port),
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "push":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				var job JobPayload
				err := json.Unmarshal(cmd.Args[2], &job)
				if err != nil {
					fmt.Printf("error unmarshal, %s\n", cmd.Args[2])
					conn.WriteError(err.Error())
					return
				}

				jid, err := queue.Push(string(cmd.Args[1]), jobs.Job{
					Data:     []byte(job.Data),
					Priority: job.Priority,
				})
				if err != nil {
					conn.WriteError(err.Error())
					return
				}
				conn.WriteBulk([]byte(jid))
			case "pop":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				var data []byte
				err := queue.Pop(string(cmd.Args[1]), func(job *jobs.Job) error {
					var err error
					payload := JobPayload{
						ID:       job.ID,
						Data:     string(job.Data),
						Priority: job.Priority,
					}
					data, err = json.Marshal(payload)
					return err
				})
				if err != nil {
					conn.WriteError(err.Error())
					return
				}
				conn.WriteBulk(data)
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
}
