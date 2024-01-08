/*
* CursusDB
* Integration/E2E Test for CursusDB
* Originally authored by Alex Gaetano Padula
* Copyright (C) CursusDB
*
* Goes through every functionality on CursusDB and could take a few minutes to complete!
** IF YOU HAVE ANYTHING SETUP ON YOUR NODE DIRECTORY PLEASE MOVE THEM AS THIS INTEGRATION TEST WILL DELETE ALL CONFIGS AND GENERATED FILES **
* ******************************************************************
* These tests go through different methods within each system.  Furthermore the tests from this point on will be added to within this file if need be for general functions.
* More complex testing shall be done from this file onward on a wider scope either with specialized tooling or manually.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	cursusdbgo "github.com/cursusdb/cursusdb-go"
	"gopkg.in/yaml.v3"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// copy copies from source to destination
func copy(source, destination string) error {
	var err error = filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		var relPath string = strings.Replace(path, source, "", 1)
		if relPath == "" {
			return nil
		}
		if info.IsDir() {
			return os.Mkdir(filepath.Join(destination, relPath), 0755)
		} else {

			var data, err1 = ioutil.ReadFile(filepath.Join(source, relPath))
			if err1 != nil {
				return err1
			}
			return ioutil.WriteFile(filepath.Join(destination, relPath), data, 0777)
		}
	})
	if errors.Is(err, os.ErrExist) {
		err = nil
	}
	return err
}

// main test starts here
func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile) // log file etc..

	// Clear up node/ and cluster/ directories

	// Remove .cdat
	err := os.Remove("../node/.cdat")
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) { // avoid ../cluster/.cursusconfig: no such file or directory
			log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
		}
	}

	// Remove .curodeconfig
	err = os.Remove("../node/.curodeconfig")
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
		}
	}

	// Remove .qqueue
	err = os.Remove("../node/.qqueue")
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
		}
	}

	// Remove .cursusconfig
	err = os.Remove("../cluster/.cursusconfig")
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
		}
	}

	// build cluster and node

	// build cluster
	cmd := exec.Command("go", "build") // or whatever the program is
	cmd.Dir = "../cluster"             // or whatever directory it's in
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	} else {
		fmt.Printf("%s", out)
	}

	// build node
	cmd = exec.Command("go", "build")
	cmd.Dir = "../node"
	out, err = cmd.Output()
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	} else {
		fmt.Printf("%s", out)
	}

	// Remove any current test directories
	err = os.RemoveAll("./cluster")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	err = os.RemoveAll("./node1")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	err = os.RemoveAll("./node2")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	err = os.RemoveAll("./node1replica")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	err = os.RemoveAll("./node2replica")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}
	// remove prior if any

	// make test cluster directory
	err = os.MkdirAll("cluster", os.ModePerm)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// make node 1 directory
	err = os.MkdirAll("node1", os.ModePerm)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// make node 2 directory
	err = os.MkdirAll("node2", os.ModePerm)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// make node 1 replica directory
	err = os.MkdirAll("node1replica", os.ModePerm)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// make node 2 replica directory
	err = os.MkdirAll("node2replica", os.ModePerm)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// copy built cluster to test cluster directory
	err = copy("../cluster", "./cluster")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// copy built node to node1 directory
	err = copy("../node", "./node1")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// copy built node to node2 directory
	err = copy("../node", "./node2")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// copy built node to node1 replica directory
	err = copy("../node", "./node1replica")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// copy built node to node2 replica directory
	err = copy("../node", "./node2replica")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	// Run cluster and node setup setting shared key as testkey and initial user as test and password as password
	cmd = exec.Command("/bin/sh", "cluster-nodes-setup.sh") // or whatever the program is

	var outBuff bytes.Buffer
	cmd.Stdout = &outBuff

	err = cmd.Run()
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	cmd.Wait()

	fmt.Println(outBuff.String())

	log.Println("✅ PASS ON CLUSTER AND NODE SETUPS")

	log.Println("⚙️ Configuring cluster and nodes(s) configuration...")

	// I know we could have split the structs into a package and imported them, I may do that later.

	// Replica is a test cluster node that current node data will be replicated/synced to
	type Replica struct {
		Host string `yaml:"host"` // Host of replica i.e an ip or fqdn
		Port int    `yaml:"port"` // Port of replica
	}

	// Observer is a test CursusDB Observer which listens for realtime node events.
	type Observer struct {
		Host string `yaml:"host"` // Host of Observer i.e an ip or fqdn
		Port int    `yaml:"port"` // Port of Observer
	}

	// NodeConfig is test node config
	type NodeConfig struct {
		Replicas                    []Replica  `yaml:"replicas"`                                 // Replicas are replica of this current node
		TLSCert                     string     `yaml:"tls-cert"`                                 // TLS cert path
		TLSKey                      string     `yaml:"tls-key"`                                  // TLS cert key
		Host                        string     `yaml:"host"`                                     // Node host i.e 0.0.0.0 usually
		TLS                         bool       `default:"false" yaml:"tls"`                      // Use TLS?
		Port                        int        `yaml:"port"`                                     // Node port
		Key                         string     `yaml:"key"`                                      // Key for a cluster to communicate with the node and also used to resting data.
		MaxMemory                   uint64     `yaml:"max-memory"`                               // Default 10240MB = 10 GB (1024 * 10)
		LogMaxLines                 int        `yaml:"log-max-lines"`                            // At what point to clear logs.  Each log line start's with a [UTC TIME] LOG DATA
		Logging                     bool       `default:"false" yaml:"logging"`                  // Log to file ?
		ReplicationSyncTime         int        `yaml:"replication-sync-time"`                    // in minutes default is every 10 minutes
		ReplicationSyncTimeout      int        `yaml:"replication-sync-timeout"`                 // As your node grows in size you may want to increase.  Default is 10 minutes.
		TLSReplication              bool       `default:"false" yaml:"tls-replication"`          // If your cluster node replicas are running TLS then configure this to true
		AutomaticBackups            bool       `default:"false" yaml:"automatic-backups"`        // If for some reason a .cdat gets corrupt you can choose to have the system save a state of your .cdat file every set n amount of time.  (default is every 8 hours(480 minutes) to make a backup of your nodes data under BackupsDirectory(which the system will create inside your binary executable location) files are named like so .cdat_YYMMDDHHMMSS in your set timezone
		AutomaticBackupTime         int        `yaml:"automatic-backup-time"`                    // Automatic node backup time.  Default is 8 (hours)
		AutomaticBackupCleanup      bool       `default:"false" yaml:"automatic-backup-cleanup"` // If set true node will clean up backups that are older than AutomaticBackupCleanupTime days old
		AutomaticBackupCleanupHours int        `yaml:"automatic-backup-cleanup-hours"`           // Clean up old .cdat backups that are n amount hours old only used if AutomaticBackups is set true default is 12 hours
		Timezone                    string     `default:"Local" yaml:"timezone"`                 // i.e America/Chicago default is local system time
		Observers                   []Observer `yaml:"observers"`                                // Observer servers listening for realtime node events (insert,update,delete).  Curode if configured will relay successful inserts, updates, and deletes to all Observer(s)
		TLSObservers                bool       `yaml:"tls-observers"`                            // Set whether your Observers are listening on tls or not
		BackupsDirectory            string     `yaml:"backups-directory"`                        // Backups directory by default is in the execution directory /backups/ Whatever is provided the system will create the director(ies) if they doesn't exist.
	}

	// NodeReplica is a test replica of master node.  Used in-case active node is not available
	type NodeReplica struct {
		Host string `yaml:"host"` // Cluster node replica host i.e 0.0.0.0 or cluster0.example.com
		Port int    `yaml:"port"` // Default cluster node port of 7682 but can be configured
	}

	// Node is a test cluster node
	type Node struct {
		Host     string        `yaml:"host"` // Cluster node host i.e 0.0.0.0 or cluster0.example.com
		Port     int           `yaml:"port"` // Cluster node port default for a cluster node is 7682
		Replicas []NodeReplica // Cluster node replicas of configured.  If node becomes unavailable where to go to instead.
	}

	// ClusterConfig is the test cluster config
	type ClusterConfig struct {
		Nodes            []Node   `yaml:"nodes"`                         // Node host/ips
		Host             string   `yaml:"host"`                          // Cluster host
		TLSNode          bool     `default:"false" yaml:"tls-node"`      // Connects to nodes with tls.  Nodes MUST be using tls in-order to set this to true.
		TLSCert          string   `yaml:"tls-cert"`                      // Location to TLS cert
		TLSKey           string   `yaml:"tls-key"`                       // Location to TLS key
		TLS              bool     `default:"false" yaml:"tls"`           // TLS on or off ?
		Port             int      `yaml:"port"`                          // Cluster port
		Key              string   `yaml:"key"`                           // Shared key - this key is used to encrypt data on all nodes and to authenticate with a node.
		Users            []string `yaml:"users"`                         // Array of encoded users
		NodeReaderSize   int      `yaml:"node-reader-size"`              // How large of a response buffer can the cluster handle
		LogMaxLines      int      `yaml:"log-max-lines"`                 // At what point to clear logs.  Each log line start's with a [UTC TIME] LOG DATA
		JoinResponses    bool     `default:"true" yaml:"join-responses"` // Joins all nodes results limiting at n
		Logging          bool     `default:"false" yaml:"logging"`       // Log to file ?
		LogQuery         bool     `default:"false" yaml:"log-query"`     // Log incoming queries
		Timezone         string   `default:"Local" yaml:"timezone"`      // i.e America/Chicago default is local system time.  On the cluster we use the Timezone for logging purposes.
		NodeReadDeadline int      `yaml:"node-read-deadline"`            // Amount of seconds to wait for a node or node replica.  Default is 2 seconds
	}

	// Ok now we marshal the current yamls and modify for our tests..

	var clusterConfig ClusterConfig

	// read test .cursusconfig
	cursusConfigYaml, err := os.ReadFile("./cluster/.cursusconfig")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if err := yaml.Unmarshal(cursusConfigYaml, &clusterConfig); err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	clusterConfig.JoinResponses = true

	clusterConfig.Nodes = append(clusterConfig.Nodes, Node{
		Host: "0.0.0.0",
		Port: 7682,
		Replicas: append([]NodeReplica{}, NodeReplica{
			Host: "0.0.0.0",
			Port: 7683,
		}),
	},
		Node{
			Host: "0.0.0.0",
			Port: 7684,
			Replicas: append([]NodeReplica{}, NodeReplica{
				Host: "0.0.0.0",
				Port: 7685,
			}),
		})

	clusterConfigOverwrite, err := os.OpenFile("./cluster/.cursusconfig", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	defer clusterConfigOverwrite.Close()

	yamlClusterConfig, err := yaml.Marshal(&clusterConfig)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	clusterConfigOverwrite.Write(yamlClusterConfig)

	var node1Config NodeConfig

	// read test .curodeconfig for node1
	curodeNode1Yaml, err := os.ReadFile("./node1/.curodeconfig")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if err := yaml.Unmarshal(curodeNode1Yaml, &node1Config); err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node1Config.Observers = append([]Observer{}, Observer{ // Setup for test observer
		Host: "0.0.0.0",
		Port: 7680,
	})

	node1Config.Port = 7682
	node1Config.Logging = true
	node1Config.AutomaticBackups = true
	node1Config.AutomaticBackupTime = 1
	node1Config.ReplicationSyncTime = 1
	node1Config.AutomaticBackupCleanupHours = 1

	node1Config.Replicas = append([]Replica{}, Replica{
		Host: "0.0.0.0",
		Port: 7683,
	})

	node1ConfigOverwrite, err := os.OpenFile("./node1/.curodeconfig", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	defer node1ConfigOverwrite.Close()

	// Marshal config to yaml
	yamlNode1Config, err := yaml.Marshal(&node1Config)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node1ConfigOverwrite.Write(yamlNode1Config)

	var node2Config NodeConfig

	// read test .curodeconfig for node2
	curodeNode2Yaml, err := os.ReadFile("./node2/.curodeconfig")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if err := yaml.Unmarshal(curodeNode2Yaml, &node2Config); err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node2Config.Observers = append([]Observer{}, Observer{ // Setup for test observer
		Host: "0.0.0.0",
		Port: 7680,
	})

	node2Config.Port = 7684
	node2Config.Logging = true
	node2Config.AutomaticBackups = true
	node2Config.ReplicationSyncTime = 1
	node2Config.AutomaticBackupTime = 1
	node2Config.AutomaticBackupCleanupHours = 1

	node2Config.Replicas = append([]Replica{}, Replica{
		Host: "0.0.0.0",
		Port: 7685,
	})

	node2ConfigOverwrite, err := os.OpenFile("./node2/.curodeconfig", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	defer node2ConfigOverwrite.Close()

	yamlNode2Config, err := yaml.Marshal(&node2Config)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node2ConfigOverwrite.Write(yamlNode2Config)

	var node1RepConfig NodeConfig

	// read test .curodeconfig for node1 replica
	curodeNode1RepYaml, err := os.ReadFile("./node1replica/.curodeconfig")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if err := yaml.Unmarshal(curodeNode1RepYaml, &node1RepConfig); err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node1RepConfig.Port = 7683
	node1RepConfig.Logging = true
	node1RepConfig.AutomaticBackups = true
	node1RepConfig.AutomaticBackupTime = 1
	node1RepConfig.AutomaticBackupCleanupHours = 1

	node1RepConfigOverwrite, err := os.OpenFile("./node1replica/.curodeconfig", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	defer node1RepConfigOverwrite.Close()

	// Marshal config to yaml
	yamlNode1RepConfig, err := yaml.Marshal(&node1RepConfig)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node1RepConfigOverwrite.Write(yamlNode1RepConfig)

	var node2RepConfig NodeConfig

	// read test .curodeconfig for node2 replica
	curodeNode2RepYaml, err := os.ReadFile("./node2replica/.curodeconfig")
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if err := yaml.Unmarshal(curodeNode2RepYaml, &node2RepConfig); err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node2RepConfig.Port = 7685
	node2RepConfig.Logging = true
	node2RepConfig.AutomaticBackupTime = 1
	node2RepConfig.AutomaticBackups = true
	node2RepConfig.AutomaticBackupCleanupHours = 1

	node2RepConfigOverwrite, err := os.OpenFile("./node2replica/.curodeconfig", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	defer node2RepConfigOverwrite.Close()

	// Marshal config to yaml
	yamlNode2RepConfig, err := yaml.Marshal(&node2RepConfig)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	node2RepConfigOverwrite.Write(yamlNode2RepConfig)

	// Now the cluster and node(s) are ready to be started and tested.

	cmds := exec.Command("/bin/sh", "restart-systems.sh")
	cmds.Stdout = os.Stdout
	cmds.Stderr = os.Stderr
	err = cmds.Run()
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	time.Sleep(time.Second * 2)

	var client *cursusdbgo.Client

	client = &cursusdbgo.Client{
		TLS:                false,
		ClusterHost:        "0.0.0.0",
		ClusterPort:        7681,
		Username:           "test",
		Password:           "password",
		ClusterReadTimeout: time.Now().Add(time.Second * 10),
	}

	err = client.Connect()
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	defer client.Close()

	res, err := client.Query(`ping;`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.EqualFold(res, "pong") {
		log.Println("✅ PASS CLUSTER PING")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL CLUSTER PING %s", err.Error()))
	}

	res, err = client.Query(fmt.Sprintf(`insert into users({"name": "Alex", "last": "Lee", "age": 28, "createdOn": 1704682782});`))
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "statusCode\":2000") {
		log.Println("✅ PASS CLUSTER INSERT INTO NODE TEST 1")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL CLUSTER INSERT INTO NODE TEST 1 %s", err.Error()))
	}

	res, err = client.Query(fmt.Sprintf(`insert into users({"name": "John", "last": "Josh", "age": 28, "tags!": ["tag1", "tag2"], "createdOn": 1704682791});`))
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "statusCode\":2000") {
		log.Println("✅ PASS CLUSTER INSERT INTO NODE TEST 2")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL CLUSTER INSERT INTO NODE TEST 2"))
	}

	// Checking uniqueness
	res, err = client.Query(fmt.Sprintf(`insert into users({"name": "Maddison", "last": "Molly", "age": 21, "tags!": ["tag1", "tag2"], "createdOn": %d});`, time.Now().Unix()))
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "statusCode\":4004") {
		log.Println("✅ PASS CLUSTER INSERT DUPE INTO NODE TEST 2")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL CLUSTER INSERT DUPE INTO NODE TEST 2"))
	}

	res, err = client.Query(`select * from users;`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "\"name\":\"John\"") && strings.Contains(res, "\"name\":\"Alex\"") {
		log.Println("✅ PASS SELECT ALL FROM COLL")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL SELECT ALL FROM COLL"))
	}

	res, err = client.Query(`select count from users;`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "{\"count\":2}") {
		log.Println("✅ PASS SELECT COUNT FROM COLL")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL SELECT COUNT FROM COLL"))
	}

	res, err = client.Query(`select * from users order by createdOn desc;`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	var orderedCollDesc []map[string]interface{}
	json.Unmarshal([]byte(res), &orderedCollDesc)

	for i, u := range orderedCollDesc {
		// first user should be John
		if i == 0 {
			if u["name"] == "John" {
				log.Println("✅ PASS SELECT COLL ORDER BY DESC")
				goto passOrderedCollDesc
			}
		}
	}

	log.Fatal(fmt.Sprintf("❌ FAIL SELECT COLL ORDER BY DESC"))

passOrderedCollDesc:

	res, err = client.Query(`select * from users order by createdOn asc;`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	var orderedCollAsc []map[string]interface{}
	json.Unmarshal([]byte(res), &orderedCollAsc)

	for i, u := range orderedCollAsc {
		// first user should be Alex
		if i == 0 {
			if u["name"] == "Alex" {
				log.Println("✅ PASS SELECT COLL ORDER BY ASC")
				goto passOrderedCollAsc
			}
		}
	}

	log.Fatal(fmt.Sprintf("❌ FAIL SELECT COLL ORDER BY ASC"))

passOrderedCollAsc:

	res, err = client.Query(`select 2 from users order by createdOn desc;`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	var orderedCollWLimitDesc []map[string]interface{}
	json.Unmarshal([]byte(res), &orderedCollWLimitDesc)

	for i, u := range orderedCollWLimitDesc {
		// first user should be John
		if i == 0 {
			if u["name"] == "John" {
				log.Println("✅ PASS SELECT LIMIT COLL ORDER BY DESC")
				goto passOrderedCollWLimitDesc
			}
		}
	}

	log.Fatal(fmt.Sprintf("❌ FAIL SELECT LIMIT COLL ORDER BY DESC"))

passOrderedCollWLimitDesc:
	res, err = client.Query(`select 2 from users order by createdOn asc;`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	var orderedCollWLimitAsc []map[string]interface{}
	json.Unmarshal([]byte(res), &orderedCollWLimitAsc)

	for i, u := range orderedCollWLimitAsc {
		// first user should be Alex
		if i == 0 {
			if u["name"] == "Alex" {
				log.Println("✅ PASS SELECT LIMIT COLL ORDER BY ASC")
				goto passOrderedCollWLimitAsc
			}
		}
	}

	log.Fatal(fmt.Sprintf("❌ FAIL SELECT LIMIT COLL ORDER BY ASC"))

passOrderedCollWLimitAsc:

	res, err = client.Query(`select * from users where name = 'John';`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "\"name\":\"John\"") {
		log.Println("✅ PASS SELECT ALL FROM COLL WITH CONDITION")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL SELECT ALL FROM COLL WITH CONDITION"))
	}

	res, err = client.Query(`select 1 from users where name = 'John';`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "\"name\":\"John\"") {
		log.Println("✅ PASS SELECT LIMIT FROM COLL WITH CONDITION")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL SELECT LIMIT FROM COLL WITH CONDITION"))
	}

	res, err = client.Query(`select * from users where name = 'John' && tags = 'tag1';`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "\"name\":\"John\"") {
		log.Println("✅ PASS SELECT ALL FROM COLL WITH CONDITIONS")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL SELECT ALL FROM COLL WITH CONDITIONS"))
	}

	res, err = client.Query(`select 1 from users where name = 'John' && tags = 'tag1';`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "\"name\":\"John\"") {
		log.Println("✅ PASS SELECT LIMIT FROM COLL WITH CONDITIONS")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL SELECT LIMIT FROM COLL WITH CONDITIONS"))
	}

	res, err = client.Query(`update * in users where name = 'John' set name = 'Johnny';`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "\"name\":\"Johnny\"") {
		log.Println("✅ PASS UPDATE ALL FROM COLL WITH CONDITIONS AND ONE SET")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL UPDATE ALL FROM COLL WITH CONDITIONS AND ONE SET"))
	}

	res, err = client.Query(`update * in users where name = 'Johnny' && tags = 'tag1' set name = 'John' set last = 'Lee';`)
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	if strings.Contains(res, "\"name\":\"John\"") && strings.Contains(res, "\"last\":\"Lee\"") {
		log.Println("✅ PASS UPDATE ALL FROM COLL WITH CONDITIONS AND MULTI SET")
	} else {
		log.Fatal(fmt.Sprintf("❌ FAIL UPDATE ALL FROM COLL WITH CONDITIONS AND MULTI SET"))
	}

	// Deletes

	// Reinsert few more records

	// Wait 2 minutes to check on backups and sync to replicas

	// What we will do now is shutdown main node 1 and test is data is persisted to replica

	// Ok now we will recover node 1 by deleting main .cdat and making sure it recovers

	// Once recovered do one more select

	// Check logging to file

	// Fin
	os.RemoveAll("cluster")
	os.RemoveAll("node1")
	os.RemoveAll("node2")
	os.RemoveAll("node1replica")
	os.RemoveAll("node2replica")

	cmds = exec.Command("/bin/sh", "kill.sh")
	cmds.Stdout = os.Stdout
	cmds.Stderr = os.Stderr
	err = cmds.Run()
	if err != nil {
		log.Fatal(fmt.Sprintf("❌ FAIL %s", err.Error()))
	}

	log.Println("✅ FIN")
}
