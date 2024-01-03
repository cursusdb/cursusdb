/*
* CursusDB
* Cluster
* ******************************************************************
* Originally authored by Alex Gaetano Padula
* Copyright (C) CursusDB
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
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"math/rand"
	"net"
	"net/textproto"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

// Cursus is the main CursusDB cluster struct
type Cursus struct {
	TCPAddr         *net.TCPAddr       // TCPAddr represents the address of the clusters TCP end point
	TCPListener     *net.TCPListener   // TCPListener is the cluster TCP network listener.
	Wg              *sync.WaitGroup    // Cluster WaitGroup waits for all goroutines to finish up
	NodeConnections []*NodeConnection  // Configured and forever connected node connections until shutdown.
	SignalChannel   chan os.Signal     // Catch operating system signal
	Config          Config             // Cluster config
	TLSConfig       *tls.Config        // Cluster TLS config if TLS is true
	ContextCancel   context.CancelFunc // For gracefully shutting down
	ConfigMu        *sync.RWMutex      // Cluster config mutex
	Context         context.Context    // Main looped go routine context.  This is for listeners, event loops and so forth
	LogMu           *sync.Mutex        // Log file mutex (only if logging enabled)
	LogFile         *os.File           // Opened log file (only if logging enabled)
}

// NodeConnection is the cluster connected to a node as a client.
type NodeConnection struct {
	Conn       *net.TCPConn    // Net connection
	SecureConn *tls.Conn       // Secure connection with TLS
	Text       *textproto.Conn // For writing and reading
	Mu         *sync.Mutex     // Multiple connections shouldn't hit the same node without the node being locked
	Replica    bool            // is node replica?
	Ok         bool            // Is node ok?
	Node       Node            // The underlaying Node for connection
}

// Connection is the main TCP connection struct for cluster
type Connection struct {
	Text *textproto.Conn        // Text is used for reading and writing
	Conn net.Conn               // net.Conn is a generic stream-oriented network connection.
	User map[string]interface{} // Authenticated user
}

// Config is the CursusDB cluster config struct
type Config struct {
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

// Node is a cluster node
type Node struct {
	Host     string        `yaml:"host"` // Cluster node host i.e 0.0.0.0 or cluster0.example.com
	Port     int           `yaml:"port"` // Cluster node port default for a cluster node is 7682
	Replicas []NodeReplica // Cluster node replicas of configured.  If node becomes unavailable where to go to instead.
}

// NodeReplica is a replica of original node.  Used in-case active node is not available
type NodeReplica struct {
	Host string `yaml:"host"` // Cluster node replica host i.e 0.0.0.0 or cluster0.example.com
	Port int    `yaml:"port"` // Default cluster node port of 7682 but can be configured
}

// Global variables
var (
	cursus *Cursus // Global cluster pointer
)

// main cluster starts here
func main() {
	cursus = &Cursus{}                                                              // Set cluster variable
	cursus.Wg = &sync.WaitGroup{}                                                   // create waitgroup
	cursus.SignalChannel = make(chan os.Signal, 1)                                  // make signal channel
	cursus.Context, cursus.ContextCancel = context.WithCancel(context.Background()) // Create context for shutdown
	cursus.ConfigMu = &sync.RWMutex{}                                               // Cluster config mutex

	// We check if a .cursusconfig file exists
	if _, err := os.Stat("./.cursusconfig"); errors.Is(err, os.ErrNotExist) {
		// .cursusconfig does not exist..

		cursus.Config.Port = 7681              // Default CursusDB cluster port
		cursus.Config.NodeReaderSize = 2097152 // Default node reader size of 2097152 bytes (2MB).. Pretty large json response
		cursus.Config.Host = "0.0.0.0"         // Default host of 0.0.0.0
		cursus.Config.LogMaxLines = 1000       // Default of 1000 lines then truncate/clear
		cursus.Config.Timezone = "Local"       // Default is system local time
		cursus.Config.NodeReadDeadline = 2     // Default of 2 seconds waiting for a node to respond

		// Get initial database user credentials
		fmt.Println("Before starting your CursusDB cluster you must first create an initial database user and shared cluster and node key.  This initial database user will have read and write permissions.  To add more users use curush (The CursusDB Shell) or native client.  The shared key is checked against what you setup on your nodes and used for data encryption.  All your nodes should share the same key you setup on your clusters.")
		fmt.Print("username> ")
		username, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			cursus.Printl(fmt.Sprintf("main(): %s", err.Error()), "ERROR")
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Relay entry with asterisks
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(username)))) // Relay input with *
		fmt.Println("")
		fmt.Print("password> ")
		password, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			cursus.Printl(fmt.Sprintf("main(): %s", err.Error()), "ERROR")
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Relay entry with asterisks
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(password)))) // Relay input with *
		fmt.Println("")
		fmt.Print("key> ")
		key, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			cursus.Printl(fmt.Sprintf("main(): %s", err.Error()), "ERROR")
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Relay entry with asterisks
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(key)))) // Relay input with *
		fmt.Println("")

		// Hash provided shared key
		hashedKey := sha256.Sum256(key)
		cursus.Config.Key = base64.StdEncoding.EncodeToString(append([]byte{}, hashedKey[:]...)) // Encode hashed key

		cursus.NewUser(string(username), string(password), "RW") // Create new user with RW permissions

		fmt.Println("")

		clusterConfigFile, err := os.OpenFile("./.cursusconfig", os.O_CREATE|os.O_RDWR, 0777) // Create .cursusconfig yaml file
		if err != nil {
			cursus.Printl(fmt.Sprintf("main(): %s", err.Error()), "ERROR")
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Marshal config to yaml
		yamlData, err := yaml.Marshal(&cursus.Config)
		if err != nil {
			cursus.Printl(fmt.Sprintf("main(): %s", err.Error()), "ERROR")
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		clusterConfigFile.Write(yamlData) // Write to yaml config

		clusterConfigFile.Close() // close up cluster config
	} else { // .cursusconfig exists

		// Read .cursus config
		clusterConfigFile, err := os.ReadFile("./.cursusconfig")
		if err != nil {
			cursus.Printl(fmt.Sprintf("main(): %s", err.Error()), "ERROR")
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Unmarshal config into cluster.config
		err = yaml.Unmarshal(clusterConfigFile, &cursus.Config)
		if err != nil {
			cursus.Printl(fmt.Sprintf("main(): %s", err.Error()), "ERROR")
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		if cursus.Config.Logging {
			cursus.LogMu = &sync.Mutex{} // Cluster log mutex
			cursus.LogFile, err = os.OpenFile("cursus.log", os.O_CREATE|os.O_RDWR, 0777)
			if err != nil {
				fmt.Println("main():", "Could not open log file - ", err.Error())
				os.Exit(1)
			}
		}

	}

	// If cluster configured cluster nodes == 0, inform user to add at least one node
	if len(cursus.Config.Nodes) == 0 {
		fmt.Println("You must setup nodes your CursusDB cluster to read from in your .cursusconfig file.")
		os.Exit(0)
	}

	signal.Notify(cursus.SignalChannel, syscall.SIGINT, syscall.SIGTERM) // Setup cluster signal channel

	// If port provided as flag use it instead of whats on config file
	flag.IntVar(&cursus.Config.Port, "port", cursus.Config.Port, "port for cluster")
	flag.Parse()

	cursus.ConnectToNodes() // Connect to configured nodes and node replicas for fast communication

	cursus.Wg.Add(1)
	go cursus.SignalListener() // Listen to system signals

	cursus.Wg.Add(1)
	go cursus.StartTCP_TLS() // Start listening tcp/tls with setup configuration

	cursus.Wg.Add(1)
	go cursus.LostReconnect() // Always attempt to reconnect to lost nodes if unavailable

	cursus.Wg.Wait() // Wait for all go routines to finish up

	os.Exit(0) // exit
}

// SaveConfig save cluster config such as created users and so forth on shutdown (Don't make changes to .cursusconfig when running as on shutdown changes will get overwritten)
func (cursus *Cursus) SaveConfig() {
	config, err := os.OpenFile(".cursusconfig", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		cursus.Printl("SaveConfig(): Could not update config file"+err.Error(), "ERROR")
		return
	}

	defer config.Close()

	// Marshal config to yaml
	yamlConfig, err := yaml.Marshal(&cursus.Config)
	if err != nil {
		cursus.Printl(fmt.Sprintf("SaveConfig(): Could not marshal config file %s", err.Error()), "ERROR")
		os.Exit(1)
	}

	config.Write(yamlConfig)
}

// ValidatePermission validates cluster permissions aka R or RW
func (cursus *Cursus) ValidatePermission(perm string) bool {
	switch perm {
	case "R":
		return true
	case "RW":
		return true
	default:
		return false
	}
}

// NewUser creates new database user
func (cursus *Cursus) NewUser(username, password, permission string) (string, map[string]interface{}, error) {
	user := make(map[string]interface{}) // Create map with username, password, and permission
	user["username"] = username
	user["password"] = password
	var encodeUsername string

	encodeUsername = base64.StdEncoding.EncodeToString([]byte(username))

	for _, u := range cursus.Config.Users {
		if strings.Split(u, ":")[0] == encodeUsername {
			return "", user, errors.New(fmt.Sprintf("%d Database user already exists.", 103))
		}
	}

	permission = strings.TrimSpace(strings.TrimSuffix(permission, ";")) // trim any space

	// validate permission
	if cursus.ValidatePermission(permission) {
		user["permission"] = permission
		b, err := json.Marshal(user)
		if err != nil {
			return "", user, errors.New(fmt.Sprintf("%d Could not marshal user for creation %s", 205, err.Error()))
		}

		h := sha256.New()
		h.Write(b)
		hashedUser := h.Sum(nil)

		cursus.ConfigMu.Lock()
		cursus.Config.Users = append(cursus.Config.Users, fmt.Sprintf("%s:%s", encodeUsername, base64.StdEncoding.EncodeToString(hashedUser))) // base64-encoded-username:struct-encoded-hashed
		cursus.ConfigMu.Unlock()

		return base64.StdEncoding.EncodeToString(hashedUser), user, nil
	} else {
		return "", user, errors.New(fmt.Sprintf("%d Invalid permission.", 101))
	}
}

// CountLog counts amount of lines within log file
func (cursus *Cursus) CountLog(r io.Reader) int {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count

		case err != nil:
			cursus.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s - %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
			return 99999999
		}
	}
}

// Printl prints a line to the cursus.log file also will clear at LogMaxLines.
// Appropriate levels: ERROR, INFO, FATAL, WARN
func (cursus *Cursus) Printl(data string, level string) {
	if cursus.Config.Logging {
		if cursus.CountLog(cursus.LogFile)+1 >= cursus.Config.LogMaxLines {
			cursus.LogMu.Lock()
			defer cursus.LogMu.Unlock()
			cursus.LogFile.Close()
			err := os.Truncate(cursus.LogFile.Name(), 0)
			if err != nil {
				cursus.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] Printl(): %s %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
				return
			}

			tz, err := time.LoadLocation(cursus.Config.Timezone)
			if err != nil {
				cursus.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s - %s\r\n", "ERROR", time.Now().UTC(), "Count not use configured timezone", err.Error())))
				return
			}

			cursus.LogFile, err = os.OpenFile("cursus.log", os.O_CREATE|os.O_RDWR, 0777)
			if err != nil {
				return
			}
			cursus.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), fmt.Sprintf("Log truncated at %d", cursus.Config.LogMaxLines))))
			cursus.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), data)))
		} else {
			tz, err := time.LoadLocation(cursus.Config.Timezone)
			if err != nil {
				cursus.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] Printl(): %s %s\r\n", "ERROR", time.Now().UTC(), "Count not use configured timezone", err.Error())))
				return
			}

			cursus.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().In(tz).Format(time.RFC822), data)))
		}
	} else {
		log.Println(fmt.Sprintf("[%s] %s", level, data))
	}

}

// ConnectToNodes connects to configured nodes
func (cursus *Cursus) ConnectToNodes() {
	// Is the cluster connecting to nodes via TLS?
	if cursus.Config.TLSNode {

		// Iterate over configured nodes and connect
		for _, n := range cursus.Config.Nodes {

			// Resolve TCP addr
			tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", n.Host, n.Port))
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// We will keep the node connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Configure TLS
			config := tls.Config{ServerName: n.Host}

			// Create TLS client connection
			secureConn := tls.Client(conn, &config)

			// Authenticate with node passing shared key wrapped in base64
			conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := conn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to slice
				cursus.NodeConnections = append(cursus.NodeConnections, &NodeConnection{
					Conn:       conn,
					SecureConn: secureConn,
					Text:       textproto.NewConn(secureConn),
					Node:       n,
					Mu:         &sync.Mutex{},
					Ok:         true,
				})

				for _, rep := range n.Replicas {

					// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
					tcpAddrReplica, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", rep.Host, rep.Port))
					if err != nil {
						fmt.Println("ConnectToNodes(): ", err.Error())
						cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")
						os.Exit(1)
					}

					// Dial tcp address up
					connReplica, err := net.DialTCP("tcp", nil, tcpAddrReplica)
					if err != nil {
						cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")

						os.Exit(1)
					}

					// We will keep the node connection alive until shutdown
					connReplica.SetKeepAlive(true) // forever

					// Configure TLS
					configReplica := tls.Config{ServerName: rep.Host}

					// Create TLS client connection
					secureConnReplica := tls.Client(conn, &configReplica)

					// Authenticate with node passing shared key wrapped in base64
					connReplica.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

					// Authentication response buffer
					authBufReplica := make([]byte, 1024)

					// Read response back from node
					rReplica, _ := connReplica.Read(authBufReplica[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:rReplica]), "0") {
						cursus.NodeConnections = append(cursus.NodeConnections, &NodeConnection{
							Conn:       conn,
							SecureConn: secureConnReplica,
							Text:       textproto.NewConn(secureConnReplica),
							Replica:    true,
							Node: Node{
								Host: rep.Host,
								Port: rep.Port,
							}, // Referring to main node
							Ok: true,
							Mu: &sync.Mutex{},
						})
					}

					cursus.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, connReplica.RemoteAddr().String()), "INFO")
				}

				// Report back successful connection
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, conn.RemoteAddr().String()), "INFO")
			} else {
				// Report back invalid key.
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", "Invalid key."), "ERROR")
				fmt.Println("ConnectToNodes(): ", "Invalid key.")
				os.Exit(1)
			}
		}
	} else {
		for _, n := range cursus.Config.Nodes {

			// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
			tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", n.Host, n.Port))
			if err != nil {
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")
				os.Exit(1)
			}

			// We will keep the node connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Authenticate with node passing shared key wrapped in base64
			conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := conn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to slice
				cursus.NodeConnections = append(cursus.NodeConnections, &NodeConnection{
					Conn: conn,
					Mu:   &sync.Mutex{},
					Text: textproto.NewConn(conn),
					Ok:   true,
					Node: n,
				})

				for _, rep := range n.Replicas {

					// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
					tcpAddrReplica, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", rep.Host, rep.Port))
					if err != nil {
						fmt.Println("ConnectToNodes(): ", err.Error())
						cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")
						os.Exit(1)
					}

					// Dial tcp address up
					connReplica, err := net.DialTCP("tcp", nil, tcpAddrReplica)
					if err != nil {
						cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", err.Error()), "ERROR")

						os.Exit(1)
					}

					// We will keep the node connection alive until shutdown
					connReplica.SetKeepAlive(true) // forever

					// Authenticate with node passing shared key wrapped in base64
					connReplica.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

					// Authentication response buffer
					authBufReplica := make([]byte, 1024)

					// Read response back from node
					rReplica, _ := connReplica.Read(authBufReplica[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:rReplica]), "0") {
						cursus.NodeConnections = append(cursus.NodeConnections, &NodeConnection{
							Conn:    connReplica,
							Text:    textproto.NewConn(connReplica),
							Replica: true,
							Node: Node{
								Host: rep.Host,
								Port: rep.Port,
							},
							Ok: true,
							Mu: &sync.Mutex{},
						})
					}

					cursus.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, connReplica.RemoteAddr().String()), "INFO")
				}

				// Report back successful connection
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %d Node connection established to %s", 225, conn.RemoteAddr().String()), "INFO")
			} else {
				// Report back invalid key
				cursus.Printl(fmt.Sprintf("ConnectToNodes(): %s", "Invalid key."), "ERROR")
				fmt.Println("ConnectToNodes(): ", "Invalid key.")
				os.Exit(1)
			}

		}

	}
}

// SignalListener listens for system signals
func (cursus *Cursus) SignalListener() {
	defer cursus.Wg.Done()
	for {
		select {
		case sig := <-cursus.SignalChannel: // Start graceful shutdown of cluster
			cursus.Printl(fmt.Sprintf("SignalListener(): %d Received signal %s starting database cluster shutdown.", -1, sig), "INFO")
			cursus.TCPListener.Close() // Close main tcp listener
			cursus.ContextCancel()     // Send context shutdown to stop all long running go routines

			// Close all node connections
			for _, nc := range cursus.NodeConnections {
				if cursus.Config.TLSNode {
					nc.SecureConn.Close()
				} else {
					nc.Conn.Close()
				}

				nc.Text.Close()

			}

			cursus.SaveConfig() // Save config
			return
		default: // continue on waiting for signals time.Nanosecond * 1000000 is VERY efficient
			time.Sleep(time.Nanosecond * 1000000)
		}
	}
}

// StartTCP_TLS starts listening on tcp/tls on configured host and port
func (cursus *Cursus) StartTCP_TLS() {
	var err error          // Local error variable for StartTCP_TLS
	defer cursus.Wg.Done() // Return go routine back to wait group

	// Resolved TCPAddr struct based on configured host and port combination
	cursus.TCPAddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", cursus.Config.Host, cursus.Config.Port))
	if err != nil {
		cursus.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
		cursus.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
		return
	}

	// Start listening for TCP connections on the given address
	cursus.TCPListener, err = net.ListenTCP("tcp", cursus.TCPAddr)
	if err != nil {
		cursus.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
		cursus.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
		return
	}

	for {
		conn, err := cursus.TCPListener.Accept() // Accept a new TCP connection
		if err != nil {
			cursus.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
			return
		}

		// If TLS is set to true within cluster config let's make the accepted connection secure
		if cursus.Config.TLS {
			cert, err := tls.LoadX509KeyPair(cursus.Config.TLSCert, cursus.Config.TLSKey)
			if err != nil {
				cursus.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
				cursus.SignalChannel <- os.Interrupt // Send interrupt signal to channel to stop cluster
				return
			}

			cursus.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}

			tlsUpgrade := tls.Server(conn, cursus.TLSConfig)
			err = tlsUpgrade.Handshake()
			if err != nil {
				conn.Close()
				continue
			} // Upgrade client connection

			conn = net.Conn(tlsUpgrade)
		}

		conn.SetReadDeadline(time.Now().Add(time.Millisecond * 150)) // We will only wait 150ms for authentication then close up their connection and go onto next
		//Expect Authentication: username\0password\n b64 encoded
		auth, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				conn.Close()
				continue // Take next connection not waiting
			}
			return
		}

		// Split at Authentication:
		authSpl := strings.Split(strings.TrimSpace(auth), "Authentication:")
		if len(authSpl) != 2 { // length not equal 2?  not good return error
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 1, "Missing authentication header.")))
			conn.Close()
			continue
		}

		// Get auth value and decode.
		authValues, err := base64.StdEncoding.DecodeString(strings.TrimSpace(authSpl[1]))
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}

		// Split AT \0 and get username and password
		authValuesSpl := strings.Split(string(authValues), "\\0")
		if len(authValuesSpl) != 2 {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}

		// Authenticate user
		_, u, err := cursus.AuthenticateUser(authValuesSpl[0], authValuesSpl[1])
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 4, err.Error()))) // no user match
			conn.Close()
			continue
		}

		// Write back to client that authentication was a success
		conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 0, "Authentication successful.")))

		cursus.Wg.Add(1)
		go cursus.HandleClientConnection(conn, u) // Handle client connection
		// .. continue on and take next client connection
	}
}

// AuthenticateUser checks if a user exists and returns the user
func (cursus *Cursus) AuthenticateUser(username string, password string) (string, map[string]interface{}, error) {
	// Here we are hashing the provided username and password and seeing if a user exists with either permission

	userR := make(map[string]interface{}) // Create map with username, password, and permission
	userR["username"] = username
	userR["password"] = password
	userR["permission"] = "R"

	bR, err := json.Marshal(userR)
	if err != nil {
		panic(err)
	}
	hR := sha256.New()
	hR.Write(bR)
	hashedR := hR.Sum(nil)

	userRW := make(map[string]interface{}) // Create map with username, password, and permission
	userRW["username"] = username
	userRW["password"] = password
	userRW["permission"] = "RW"

	bRW, err := json.Marshal(userRW)
	if err != nil {
		return "", userRW, errors.New(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
	}
	hRW := sha256.New()
	hRW.Write(bRW)
	hashedRW := hRW.Sum(nil)

	for _, u := range cursus.Config.Users {
		if u == fmt.Sprintf("%s:%s", base64.StdEncoding.EncodeToString([]byte(username)), base64.StdEncoding.EncodeToString(hashedR)) {
			return u, userR, nil
		} else if u == fmt.Sprintf("%s:%s", base64.StdEncoding.EncodeToString([]byte(username)), base64.StdEncoding.EncodeToString(hashedRW)) {
			return u, userRW, nil
		}

	}

	return "", nil, errors.New(fmt.Sprintf("%d User does not exist.", 102))
}

// InsertIntoNode selects one node within cluster nodes and inserts json document.
func (cursus *Cursus) InsertIntoNode(connection *Connection, insert string, collection string, id string) {
	var node *NodeConnection // Node connection which will be chosen randomly
	var nodeRetries int      // Amount of retries based on main node count

	nonReplicaCount := (func() int {
		i := 0
		for _, nc := range cursus.NodeConnections {
			if !nc.Replica {
				i += 1
			}
		}
		return i
	})() // Get non node replica count

	if nonReplicaCount >= 10 {
		nodeRetries = nonReplicaCount * 2 // Amount of times to retry another node if the chosen node is at peak allocation or unavailable
	} else {
		nodeRetries = 10 // Retry main node 10 times to insert
	}

	// Setting up document
	doc := make(map[string]interface{})

	// Unmarshal insert json into CursusDB document
	err := json.Unmarshal([]byte(insert), &doc)
	if err != nil {
		connection.Text.PrintfLine("%d Unmarsharable JSON insert.", 4000)
		return
	}

	doc["$id"] = id // We have already verified the id to not exist

	requestMap := make(map[string]interface{}) // request structure for node to understand

	requestMap["document"] = doc
	requestMap["action"] = "insert"

	requestMap["collection"] = collection

	jsonString, err := json.Marshal(requestMap)
	if err != nil {
		connection.Text.PrintfLine("Cannot insert. %s", err.Error())
		return
	}

	goto query

query:
	rand.Seed(time.Now().UnixNano())

	node = cursus.NodeConnections[(0 + rand.Intn((len(cursus.NodeConnections)-1)-0+1))] // Select a random node that is not a replica

	if !node.Replica { // Make sure node connection is node a replica for inserts
		goto ok
	} else {
		goto query
	}

ok:

	if !cursus.Config.TLSNode {
		node.Conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cursus.Config.NodeReadDeadline)))
		node.Conn.SetNoDelay(true)
	} else {
		node.SecureConn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cursus.Config.NodeReadDeadline)))
	}

	goto insert

insert:
	node.Mu.Lock()
	node.Text.PrintfLine("%s", string(jsonString)) // Send the query over

	response, err := node.Text.ReadLine()
	if err != nil {
		node.Mu.Unlock()
		node.Ok = false
		if nodeRetries > -1 {
			nodeRetries -= 1
			currentNode := node
			goto findNode

		findNode:

			if nodeRetries == -1 {
				connection.Text.PrintfLine("%d No node was available for insert.", 104)
				return
			}

			node = cursus.NodeConnections[(0 + rand.Intn((len(cursus.NodeConnections)-1)-0+1))] // Pick another node, not the current one we have selected prior

			if len(cursus.Config.Nodes) > 1 {
				if fmt.Sprintf("%s:%d", node.Node.Host, node.Node.Port) == fmt.Sprintf("%s:%d", currentNode.Node.Host, currentNode.Node.Port) { // To not retry same node
					nodeRetries -= 1
					goto findNode
				} else {
					goto insert
				}
			} else {
				goto query
			}
		} else {
			connection.Text.PrintfLine("%d No node was available for insert.", 104)
			return
		}

	}

	node.Mu.Unlock()

	if strings.Contains(response, "\"statusCode\":\"100\"") {
		// Node was at peak allocation.
		cursus.Printl(fmt.Sprintf("InsertIntoNode(): %s was at peak allocation.  Consider providing more memory to node.", node.Conn.RemoteAddr().String()), "WARN")
		// Picking another node and trying again
		if nodeRetries > -1 {
			nodeRetries -= 1
			goto query
		} else {
			node.Ok = false
			connection.Text.PrintfLine("%d No node was available for insert.", 104)
			return
		}
	}

	connection.Text.PrintfLine(response)

}

// QueryNodes queries all nodes in parallel and gets responses
func (cursus *Cursus) QueryNodes(connection *Connection, body map[string]interface{}) error {

	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	wgPara := &sync.WaitGroup{}
	muPara := &sync.RWMutex{}
	for _, n := range cursus.NodeConnections {
		if !n.Replica {
			wgPara.Add(1)
			go cursus.QueryNode(n, jsonString, wgPara, muPara, &responses, body["action"].(string))
		}
	}

	wgPara.Wait()

	if cursus.Config.JoinResponses {
		if body["action"].(string) == "select" {
			var docs []interface{}

			count := 0 // if count
			isCount := false

			for _, res := range responses {

				if strings.Contains(res, "\"statusCode\": 105") {
					cursus.Printl("QueryNodes(): "+res, "INFO")
					continue
				}

				var x []interface{}
				err := json.Unmarshal([]byte(res), &x)
				if err != nil {
					return errors.New(fmt.Sprintf("%d Unmarsharable JSON.", 4018))
				}

				if len(x) > 0 {
					c, ok := x[0].(map[string]interface{})["count"]
					if ok {
						if !isCount {
							isCount = true
						}

						count += int(c.(float64))
					}
				}

				if !isCount {
					docs = append(docs, x...)
				}
			}

			if !isCount {

				if body["limit"] != -1 {
					var docsToRemoveFromResponse []int
					for i, _ := range docs {
						if i >= body["limit"].(int) {
							docsToRemoveFromResponse = append(docsToRemoveFromResponse, i)
						}
					}

					for _, i := range docsToRemoveFromResponse {
						copy(docs[i:], docs[i+1:])
						docs[len(docs)-1] = ""
						docs = docs[:len(docs)-1]
					}

					docsJson, err := json.Marshal(docs)
					if err != nil {
						return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
					}

					connection.Text.PrintfLine(string(docsJson))
				} else {

					docsJson, err := json.Marshal(docs)
					if err != nil {
						return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
					}

					connection.Text.PrintfLine(string(docsJson))
				}
			} else {
				countResponse := make(map[string]interface{})
				countResponse["count"] = count

				countJson, err := json.Marshal(countResponse)
				if err != nil {
					return errors.New(fmt.Sprintf("%d Could not marshal JSON.", 4012))
				}

				connection.Text.PrintfLine(string(countJson))
			}
		} else {
			var response string
			for key, res := range responses {
				response += fmt.Sprintf(`{"%s": %s},`, key, res)
			}

			connection.Text.PrintfLine(fmt.Sprintf("[%s]", strings.TrimSuffix(response, ",")))
		}

	} else {
		var response string
		for key, res := range responses {
			response += fmt.Sprintf(`{"%s": %s},`, key, res)
		}

		connection.Text.PrintfLine(fmt.Sprintf("[%s]", strings.TrimSuffix(response, ",")))

	}

	return nil
}

// QueryNodesRet queries all nodes and combines responses
func (cursus *Cursus) QueryNodesRet(body map[string]interface{}) map[string]string {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	wgPara := &sync.WaitGroup{}
	muPara := &sync.RWMutex{}
	for _, n := range cursus.NodeConnections {
		if !n.Replica {
			wgPara.Add(1)
			go cursus.QueryNode(n, jsonString, wgPara, muPara, &responses, body["action"].(string))
		}
	}

	wgPara.Wait()

	return responses
}

// QueryNode queries a specific node
func (cursus *Cursus) QueryNode(n *NodeConnection, body []byte, wg *sync.WaitGroup, mu *sync.RWMutex, responses *map[string]string, action string) {
	defer wg.Done() // defer returning go routine to waitgroup
	n.Mu.Lock()

	defer n.Mu.Unlock()

	mn := n // non replica main node

	retriesReplica := len(n.Node.Replicas) // Retry on configured node read replicas

	retriesMainNode := 3 // Retry main node 3 times

	var attemptedReplicas []string

	goto query

query:
	n.Text.Reader.R = bufio.NewReaderSize(n.Conn, cursus.Config.NodeReaderSize)

	n.Text.PrintfLine("%s", string(body))

	n.Conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cursus.Config.NodeReadDeadline))) // Timeout if node doesn't respond within NodeReadDeadline seconds(we will try 3 more times before trying a replica)
	line, err := n.Text.ReadLine()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			n.Ok = false
			if len(n.Node.Replicas) == 0 {
				retriesMainNode -= retriesMainNode

				if retriesMainNode > -1 {
					goto query
				}

			}

		} else if errors.Is(err, io.EOF) {
			n.Ok = false
			if len(n.Node.Replicas) == 0 {
				retriesMainNode -= retriesMainNode

				if retriesMainNode > 0 {
					goto query
				}
				goto unavailable
			}
		} else {
			n.Ok = false
			if len(n.Node.Replicas) == 0 {
				retriesMainNode -= retriesMainNode

				if retriesMainNode > 0 {
					goto query
				}
			}
		}

		mn.Ok = false
		goto unavailable
	}

	mu.Lock()
	(*responses)[n.Conn.RemoteAddr().String()] = line
	mu.Unlock()
	goto fin

unavailable:
	n.Ok = false

	// Will retry on nodes replicas, if n-rep1 not available, go next n-rep2, so forth until no replicas are available.

	if strings.Contains(action, "select") || strings.Contains(action, "count") {
		for _, r := range mn.Node.Replicas {
			for _, nc := range cursus.NodeConnections {
				if fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port) == fmt.Sprintf("%s:%d", r.Host, r.Port) && nc.Replica == true {

					if slices.Contains(attemptedReplicas, fmt.Sprintf("%s:%d", r.Host, r.Port)) {
						continue
					}

					n = nc
					retriesReplica -= 1

					if retriesReplica > -1 {
						attemptedReplicas = append(attemptedReplicas, fmt.Sprintf("%s:%d", r.Host, r.Port))
						goto query
					} else {
						break
					}
				}
			}
		}
	}

	if len(mn.Node.Replicas) > 0 {
		cursus.Printl(fmt.Sprintf("QueryNode(): %d Node %s and replicas %s are unavailable.", 105, mn.Conn.RemoteAddr().String(), strings.Join((func(s []string) []string {
			if len(s) < 1 {
				return s
			}

			sort.Strings(s)
			prev := 1
			for curr := 1; curr < len(s); curr++ {
				if s[curr-1] != s[curr] {
					s[prev] = s[curr]
					prev++
				}
			}

			return s[:prev]
		})(attemptedReplicas), ",")), "WARNING")
	} else {
		cursus.Printl(fmt.Sprintf(`QueryNode(): %d Node %s is unavailable.`, 105, n.Conn.RemoteAddr().String()), "WARNING")
	}

	return
fin:
	return
}

// HandleClientConnection handles tcp/tls client connection
func (cursus *Cursus) HandleClientConnection(conn net.Conn, user map[string]interface{}) {
	defer cursus.Wg.Done() // defer report return to waitgroup
	defer conn.Close()     // defer client connection close
	text := textproto.NewConn(conn)
	defer text.Close() // defer close connection writer and reader

	query := "" // clients current query

	for {
		conn.SetReadDeadline(time.Now().Add(time.Nanosecond * 1000000)) // essentially keep listening until the client closes connection or cluster shuts down
		read, err := text.ReadLine()                                    // read line from client
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if cursus.Context.Err() != nil { // if signaled to shutdown
					break
				}
				continue // continue on listening to client
			} else {
				break
			}
		}

		// Does line end with a semicolon?
		if strings.HasSuffix(strings.TrimSpace(string(read)), ";") {
			query += strings.TrimSpace(string(read))
		} else {
			query += strings.TrimSpace(string(read)) + " "
		}

		if strings.HasPrefix(query, "ping") { // ping
			text.PrintfLine("pong")
			query = ""
			continue
		} else if strings.HasSuffix(query, ";") { // Does line end with a semicolon?

			if cursus.Config.LogQuery { // Only log if configuration set as queries can contain harmful personal information
				cursus.Printl(fmt.Sprintf("HandleClientConnection(): %s query(%s)", conn.RemoteAddr().String(), query), "INFO")
			}

			// Check user permission and check if their allowed to use the specific action
			switch user["permission"] {
			case "R":
				if strings.HasPrefix(query, "update") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "insert") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "new user") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "users") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "delete user") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "delete key") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed
				} else if strings.HasPrefix(query, "delete") {
					text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
					goto continueOn // User not allowed, ret
				} else if strings.HasPrefix(query, "select") {
					goto allowed // Goto allowed
				} else if strings.HasPrefix(query, "collections") {
					goto allowed // Goto allowed
				}
			case "RW":
				goto allowed // Goto allowed
			}

		continueOn: // User isn't allowed to use action but continue listening for something else
			query = ""
			continue

		allowed: // User is allowed

			switch {
			// query starts with collections
			case strings.HasPrefix(query, "collections"):
				// get all cluster collections

				// Start node request
				body := make(map[string]interface{})
				body["action"] = "collections"

				// query nodes
				err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				query = ""
				continue

			// Processing read query as CDQL
			case strings.HasPrefix(query, "insert "):
				// start insert
				// insert into users({"firstName": "John", "lastName": "Doe"});

				retries := 5 // how many times to retry if node is not available for uniqueness isn`t met for $id
				// query is not valid
				// must have a full prefix of 'insert into '
				if !strings.HasPrefix(query, "insert into ") {
					text.PrintfLine(fmt.Sprintf("%d Invalid insert query missing 'insert into'.", 4009))
					query = "" // Clear query variable and listen for another
					continue
				}

				// Regex for insert i.e coll({}) in-between parenthesis
				var insertJsonRegex = regexp.MustCompile(`\((.*?)\)`)

				insertJson := insertJsonRegex.FindStringSubmatch(query) // Get insert JSON

				collection := strings.ReplaceAll(strings.Split(query, "({\"")[0], "insert into ", "") // Get collection

				if len(insertJson) != 2 {
					text.PrintfLine(fmt.Sprintf("%d Invalid insert query is missing parentheses.", 4010))
					query = ""
					continue
				}

				// Check if insert is json array i.e insert i.e `insert into example([{"x": 4, "y": 44}, {"x": 2, "y": 22}]);`
				// This is not allowed.  If you want to batch insert create many connections to the cluster and insert in parallel.
				if strings.HasPrefix(strings.TrimSpace(insertJson[1]), "[{") && strings.HasSuffix(strings.TrimSpace(insertJson[1]), "}]") {

					text.PrintfLine(fmt.Sprintf("%d Batch insertion not supported.", 4024))
					query = ""
					continue
				}

				// Check reserved words based on https://go.dev/ref/spec and CursusDB system reserved words
				// What has been commented out has been tested inserting and reading like so
				// insert into test({"case": "test"});
				// select * from test where case = 'test';
				switch {
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"count":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"$id":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"$indx":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"in":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"like":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"not like":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"!like":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"where":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"from":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"*":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"break":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"case":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"chan":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"const":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"continue":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"default":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"defer":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"else":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"fallthrough":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"for":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"func":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"go":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"goto":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"if":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"import":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"interface":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"map":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"package":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"range":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"return":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"select":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"struct":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"switch":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"type":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"var":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"false":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"true":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"uint8":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"uint16":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"uint32":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"uint64":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"int8":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"int16":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"int32":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"int64":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"float32":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"float64":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"complex64":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"complex128":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"byte":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"rune":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"uint":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"int":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"uintptr":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"string":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
					query = ""
					continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"make":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				//case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"new":`):
				//	text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved word.", 505))
				//	query = ""
				//	continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"==":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved symbol.", 506))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"&&":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved symbol.", 506))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"||":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved symbol.", 506))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `">":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved symbol.", 506))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"<":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved symbol.", 506))
					query = ""
					continue
				case strings.Contains(strings.ReplaceAll(insertJson[1], "!\":", "\":"), `"=":`):
					text.PrintfLine(fmt.Sprintf("%d Key cannot use reserved symbol.", 506))
					query = ""
					continue
				default:
					goto keyOk // insert key is ok
				}

			keyOk:

				// Checking if there are any !s to process
				var indexed = regexp.MustCompile(`"([^"]+!)"`) // "email!":
				// "key!" means check all nodes if this key and value exists
				// if an array "key!": [arr]
				// Cursus will check all values within the basic array.

				indexedRes := indexed.FindAllStringSubmatch(query, -1)
				// loop over unique key value pairs checking nodes
				// Returns error 4004 to client if a document exists
				for _, indx := range indexedRes {

					// Read json key VALUE(s)!
					kValue := regexp.MustCompile(fmt.Sprintf(`"%s"\s*:\s*(true|false|null|[A-Za-z]|\[.*?\]|[0-9]*[.]?[0-9]+|".*?"|'.*?')`, indx[1]))

					// Create node request
					body := make(map[string]interface{})
					body["action"] = "select"       // We will select 1 from all nodes with provided key value
					body["limit"] = 1               // limit of 1 of course
					body["collection"] = collection // collection is provided collection
					body["conditions"] = []string{""}
					body["skip"] = 0
					body["sort-pos"] = ""
					body["sort-key"] = ""
					var interface1 []interface{} // In-order to have an interface slice in go you must set them up prior to using them.
					var interface2 []interface{} // ^
					var interface3 []interface{} // ^

					body["keys"] = interface1   // We send nodes an array of keys to query
					body["oprs"] = interface2   // We send nodes an array of oprs to use for query
					body["values"] = interface3 // Values for query
					// There must be equal keys, oprs, and values.

					body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
					body["oprs"] = append(body["oprs"].([]interface{}), "==")                                                // == obviously

					body["lock"] = true // lock on read.  There can be many clusters reading at one time.  This helps setup uniqueness across all nodes if indexes are required

					if len(kValue.FindStringSubmatch(query)) > 0 {
						if strings.HasPrefix(kValue.FindStringSubmatch(query)[1], "[") && strings.HasSuffix(kValue.FindStringSubmatch(query)[1], "]") {
							var arr []interface{}
							err := json.Unmarshal([]byte(kValue.FindStringSubmatch(query)[1]), &arr)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unmarsharable JSON insert.", 4000))
								query = ""
								continue
							}

							for j, a := range arr {

								body["values"] = append(body["values"].([]interface{}), a)
								if j == 0 {
									body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
									body["oprs"] = append(body["oprs"].([]interface{}), "==")
								}
							}

							res := cursus.QueryNodesRet(body)
							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists.")

									r, _ := json.Marshal(result)
									text.PrintfLine(string(r))
									query = ""
									goto cont
								}
							}

						} else {

							body["values"] = append(body["values"].([]interface{}), kValue.FindStringSubmatch(query)[1])

							if strings.EqualFold(body["values"].([]interface{})[0].(string), "null") {
								body["values"].([]interface{})[0] = nil
							} else if cursus.IsString(body["values"].([]interface{})[0].(string)) {

								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "'")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "'")
							} else if cursus.IsInt(body["values"].([]interface{})[0].(string)) {
								i, err := strconv.Atoi(body["values"].([]interface{})[0].(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = i

							} else if cursus.IsFloat(body["values"].([]interface{})[0].(string)) {

								f, err := strconv.ParseFloat(body["values"].([]interface{})[0].(string), 64)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = f
							} else if cursus.IsBool(body["values"].([]interface{})[0].(string)) {

								b, err := strconv.ParseBool(body["values"].([]interface{})[0].(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = b
							}

							res := cursus.QueryNodesRet(body)

							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists")

									r, _ := json.Marshal(result)
									text.PrintfLine(string(r))
									query = ""
									goto cont
								}
							}
						}
					}
				}

				goto ok

			cont:
				query = ""
				continue

			ok:
				body := make(map[string]interface{})

				var interface1 []interface{}
				var interface2 []interface{}
				var interface3 []interface{}
				body["action"] = "select"
				body["limit"] = 1
				body["skip"] = 0
				body["collection"] = collection
				body["conditions"] = []string{""}
				body["sort-pos"] = ""
				body["sort-key"] = ""

				body["keys"] = interface1
				body["keys"] = append(body["keys"].([]interface{}), "$id")
				body["oprs"] = interface2
				body["oprs"] = append(body["oprs"].([]interface{}), "==")

				body["lock"] = true // lock on read.  There can be many clusters reading at one time.  This helps setup uniqueness across all nodes
				body["values"] = interface3
				body["values"] = append(body["values"].([]interface{}), uuid.New().String())

				res := cursus.QueryNodesRet(body)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						if retries != 0 {
							retries -= 1
							goto retry // $id already exist
						} else {
							text.PrintfLine(fmt.Sprintf("%d No unique $id could be found for insert.", 4023)) // Wouldn't happen ever but if it does the system as you can see would try at least 5 times
							goto cont
						}
					}
				}

				goto insert
			retry:
				body["values"].([]interface{})[0] = uuid.New().String() // Generate new uuid

				res = cursus.QueryNodesRet(body)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						if retries != 0 {
							retries -= 1
							goto retry // $id already exist
						} else {
							goto cont
						}
					}
				}

				goto insert

			insert:
				cursus.InsertIntoNode(&Connection{Conn: conn, Text: text, User: nil}, strings.ReplaceAll(insertJson[1], "!\":", "\":"), collection, body["values"].([]interface{})[0].(string))

				query = ""
				continue
				// end insert
			case strings.HasPrefix(query, "select "):
				// start select
				// select LIMIT from COLLECTION SET KEY = V SET KEY = V;
				// update LIMIT from COLLECTION where KEY = V && KEY = V order by KEY desc;

				if !strings.Contains(query, "from ") {
					text.PrintfLine(fmt.Sprintf("%d From is required.", 4006))
					query = ""
					continue
				}

				if strings.Contains(query, "not like") {
					query = strings.ReplaceAll(query, "not like", "!like")
				}

				sortPos := ""
				sortKey := ""

				if strings.Contains(query, "order by ") {
					sortKey = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(query[strings.Index(query, "order by "):], "order by "), "asc;"), "desc;"))
					if strings.HasSuffix(query, "asc;") {
						sortPos = "asc"
					} else {
						sortPos = "desc"
					}

					query = query[:strings.Index(query, "order by ")]
				}

				qsreg := regexp.MustCompile("'.+'|\".+\"|\\S+")

				querySplit := qsreg.FindAllString(strings.Replace(strings.Replace(query, "from", "", 1), "where", "", 1), -1)

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]

					if len(querySplit) == 2 {
						text.PrintfLine(fmt.Sprintf("%d Missing limit value.", 4016))
						query = ""
						continue
					}

					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["skip"] = 0
					body["conditions"] = []string{""}
					body["lock"] = false // lock on read.  There can be many clusters reading at one time.
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					if body["limit"].(string) != "count" {

						if body["limit"].(string) == "*" {
							body["limit"] = -1
						} else if strings.Contains(body["limit"].(string), ",") {
							if len(strings.Split(body["limit"].(string), ",")) == 2 {
								body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
									query = ""
									continue
								}

								if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
									body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
									if err != nil {
										text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 502, err.Error()))
										query = ""
										continue
									}
								} else {
									body["limit"] = -1
								}
							} else {
								text.PrintfLine("%d Invalid limiting value.", 504)
								query = ""
								continue
							}
						} else {
							body["limit"], err = strconv.Atoi(body["limit"].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
								query = ""
								continue
							}
						}
					} else {
						body["limit"] = -2
						body["count"] = true
					}

					if strings.Contains(query, "where") {
						if len(body["values"].([]interface{})) == 0 || body["values"] == nil {
							text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 506))
							query = ""
							continue
						}
					}

					err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				} else {
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["skip"] = 0
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					for k, s := range andOrSplit {
						querySplitNested := qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.Replace(strings.TrimSuffix(s, ";"), "from", "", 1), "where", "", 1)), -1)
						if len(querySplitNested) < 3 {
							text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
							query = ""
							goto extCont
						}

						body["keys"] = append(body["keys"].([]interface{}), querySplitNested[len(querySplitNested)-3])
						body["oprs"] = append(body["oprs"].([]interface{}), querySplitNested[len(querySplitNested)-2])

						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "="):
							body["oprs"].([]interface{})[k] = "=="
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "like"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!like"):
						default:
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
							query = ""
							goto extCont
						}

						body["values"] = append(body["values"].([]interface{}), strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";"))

						lindx := strings.Index(query, fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
						valLen := len(fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

						if len(query[lindx+valLen:]) > 3 {
							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(query[lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cursus.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cursus.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						} else if cursus.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cursus.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						}

					}

					if len(body["values"].([]interface{})) == 0 {
						text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 506))
						query = ""
						continue
					}

					if body["limit"].(string) != "count" {
						if body["limit"].(string) == "*" {
							body["limit"] = -1
						} else if strings.Contains(body["limit"].(string), ",") {
							if len(strings.Split(body["limit"].(string), ",")) == 2 {
								var err error
								body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
									query = ""
									continue
								}

								if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
									body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
									if err != nil {
										text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
										query = ""
										continue
									}
								} else {
									body["limit"] = -1
								}
							} else {
								text.PrintfLine("%d Invalid limiting value.", 504)
								query = ""
								continue
							}
						} else {
							var err error
							body["limit"], err = strconv.Atoi(body["limit"].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
								query = ""
								continue
							}
						}
					} else {
						body["limit"] = -2
						body["count"] = true

					}

					err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				extCont:
					continue

				}
				// end select
			case strings.HasPrefix(query, "update "):
				// start update
				// update LIMIT in COLLECTION SET KEY = V SET KEY = V;
				// update LIMIT in COLLECTION where KEY = V && KEY = V order by KEY desc SET KEY = V;

				if !strings.Contains(query, "in ") {
					text.PrintfLine(fmt.Sprintf("%d In is required.", 4020))
					query = ""
					continue
				}

				query = strings.ReplaceAll(query, "not like", "!like")

				sortPos := ""
				sortKey := ""

				if strings.Contains(query, "order by ") {
					sortKey = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(query[strings.Index(query, "order by "):], "order by "), "asc;"), "desc;"))
					if strings.HasSuffix(query, "asc;") {
						sortPos = "asc"
					} else {
						sortPos = "desc"
					}

					query = query[:strings.Index(query, "order by ")]
				}

				qsreg := regexp.MustCompile("'.+'|\".+\"|\\S+")
				querySplit := qsreg.FindAllString(strings.Replace(strings.Replace(strings.Replace(query, "in", "", 1), "from", "", 1), "where", "", 1), -1)

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]

					if len(querySplit) == 2 {
						text.PrintfLine(fmt.Sprintf("%d Missing limit value.", 4016))
						query = ""
						continue
					}

					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}
					var interface4 []interface{}
					var interface5 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["update-keys"] = interface4
					body["new-values"] = interface5
					body["conditions"] = []string{""}
					body["lock"] = false
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey
					body["skip"] = 0

					// Get new key values
					if len(strings.Split(query, "set ")) == 1 {
						text.PrintfLine(fmt.Sprintf("%d Update sets are missing.", 4019))
						query = ""
						continue
					}

					for _, s := range strings.Split(query, "set ")[1:] {
						newValues := strings.Split(strings.ReplaceAll(s, "set ", ""), ",")

						for _, nvSet := range newValues {
							spl := strings.Split(nvSet, " = ")
							body["update-keys"] = append(body["update-keys"].([]interface{}), strings.TrimSpace(spl[0]))
							var val interface{}
							if len(spl) != 2 {
								text.PrintfLine(fmt.Sprintf("%d Set is missing =.", 4008))
								query = ""
								goto extCont5
							}

							val = strings.TrimSuffix(strings.TrimSpace(spl[1]), ";")
							if strings.EqualFold(val.(string), "null") {
								val = nil
							} else if cursus.IsString(val.(string)) {

								val = strings.TrimSuffix(val.(string), "\"")
								val = strings.TrimPrefix(val.(string), "\"")
								val = strings.TrimSuffix(val.(string), "'")
								val = strings.TrimPrefix(val.(string), "'")
							} else if cursus.IsInt(val.(string)) {
								i, err := strconv.Atoi(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
									query = ""
									goto extCont5
								}

								val = i

							} else if cursus.IsFloat(val.(string)) {

								f, err := strconv.ParseFloat(val.(string), 64)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
									query = ""
									goto extCont5
								}

								val = f
							} else if cursus.IsBool(val.(string)) {

								b, err := strconv.ParseBool(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
									query = ""
									goto extCont5
								}

								val = b
							}
							body["new-values"] = append(body["new-values"].([]interface{}), val)
						}
					}

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 502, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine(fmt.Sprintf("%d Invalid limiting value.", 504))
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 502, err.Error()))
							query = ""
							continue
						}
					}

					err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				extCont5:
					query = ""
					continue

				} else { // With conditions
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["skip"] = 0
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}
					var interface4 []interface{}
					var interface5 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["update-keys"] = interface4
					body["new-values"] = interface5
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					// Get new key values
					if len(strings.Split(query, "set ")) == 1 {
						text.PrintfLine(fmt.Sprintf("%d Update sets are missing.", 4019))
						query = ""
						continue
					}

					for _, s := range strings.Split(query, "set ")[1:] {
						newValues := strings.Split(strings.ReplaceAll(s, "set ", ""), ",")

						for _, nvSet := range newValues {
							spl := strings.Split(nvSet, " = ")
							body["update-keys"] = append(body["update-keys"].([]interface{}), strings.TrimSpace(spl[0]))
							var val interface{}
							if len(spl) != 2 {
								text.PrintfLine(fmt.Sprintf("%d Set is missing =.", 4008))
								query = ""
								goto extCont3
							}

							val = strings.TrimSuffix(strings.TrimSpace(spl[1]), ";")
							if strings.EqualFold(val.(string), "null") {
								val = nil
							} else if cursus.IsString(val.(string)) {

								val = strings.TrimSuffix(val.(string), "\"")
								val = strings.TrimPrefix(val.(string), "\"")
								val = strings.TrimSuffix(val.(string), "'")
								val = strings.TrimPrefix(val.(string), "'")
							} else if cursus.IsInt(val.(string)) {
								i, err := strconv.Atoi(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
									query = ""
									goto extCont3
								}

								val = i

							} else if cursus.IsFloat(val.(string)) {

								f, err := strconv.ParseFloat(val.(string), 64)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
									query = ""
									goto extCont3
								}

								val = f
							} else if cursus.IsBool(val.(string)) {

								b, err := strconv.ParseBool(val.(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
									query = ""
									goto extCont3
								}

								val = b
							}
							body["new-values"] = append(body["new-values"].([]interface{}), val)
						}
					}

					for k, s := range andOrSplit {
						var querySplitNested []string
						if strings.Contains(s, "set") {
							if strings.Count(s, "where") > 1 {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.TrimSuffix(s[:strings.Index(s, "set")], ";"), "where", "", 1)), -1)

							} else {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.TrimSuffix(s[:strings.Index(s, "set")], ";")), -1)
							}

							if len(querySplitNested) < 3 {
								text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
								query = ""
								continue
							}

						} else {
							if strings.Count(s, "where") > 1 {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.TrimSuffix(s, ";"), "where", "", 1)), -1)
								if len(querySplitNested) < 3 {
									text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
									query = ""
									continue
								}
							} else {
								querySplitNested = qsreg.FindAllString(strings.TrimSpace(strings.TrimSuffix(s, ";")), -1)
								if len(querySplitNested) < 3 {
									text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
									query = ""
									continue
								}
							}
						}

						body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(querySplitNested[len(querySplitNested)-3]))
						body["oprs"] = append(body["oprs"].([]interface{}), strings.TrimSpace(querySplitNested[len(querySplitNested)-2]))
						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "="):
							body["oprs"].([]interface{})[k] = "=="
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "like"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!like"):
						default:
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
							query = ""
							goto extCont3
						}

						body["values"] = append(body["values"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";")))

						lindx := strings.Index(strings.TrimRight(strings.Split(query, "set")[0], "set "), fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
						valLen := len(fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

						if len(strings.Split(query, "set")[0][lindx+valLen:]) > 2 {
							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(strings.Split(query, "set")[0][lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cursus.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cursus.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						} else if cursus.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cursus.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						}

					}

					if len(body["values"].([]interface{})) == 0 {
						text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 506))
						query = ""
						continue
					}

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine("%d Invalid limiting value.", 504)
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
							query = ""
							continue
						}
					}

					err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue
				extCont3:
					continue
				}
				// end update
			case strings.HasPrefix(query, "delete user"):
				// start delete user
				// delete user USERNAME
				splQ := strings.Split(query, "delete user ")

				if len(splQ) != 2 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				err = cursus.RemoveUser(splQ[1])
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				text.PrintfLine("%d Database user %s removed successfully.", 201, strings.TrimSuffix(splQ[1], ";"))

				query = ""
				continue
				// end delete user
			case strings.HasPrefix(query, "delete key "):
				// start delete key
				// delete key KEY in COLLECTION;
				// removes key from all documents within a collection

				body := make(map[string]interface{})
				body["action"] = "delete key"

				if !strings.Contains(query, "in") {
					text.PrintfLine(fmt.Sprintf("%d delete key missing in.", 213))
					query = ""
					continue
				}

				querySplit := strings.Split(strings.TrimPrefix(query, "delete key "), "in")

				if len(querySplit) < 2 {
					text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
					query = ""
					continue
				}

				body["key"] = strings.TrimSpace(querySplit[0])
				body["collection"] = strings.TrimSpace(strings.TrimSuffix(querySplit[1], ";"))

				err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				query = ""
				continue
				// end delete key
			case strings.HasPrefix(query, "delete "):
				// start delete
				// delete 1 from users where name == 'alex' && last == 'padula';
				if !strings.Contains(query, "from ") {
					text.PrintfLine(fmt.Sprintf("%d From is required.", 4006))
					query = ""
					continue
				}

				query = strings.ReplaceAll(query, "not like", "!like")

				sortPos := ""
				sortKey := ""

				if strings.Contains(query, "order by ") {
					sortKey = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(query[strings.Index(query, "order by "):], "order by "), "asc;"), "desc;"))
					if strings.HasSuffix(query, "asc;") {
						sortPos = "asc"
					} else {
						sortPos = "desc"
					}

					query = query[:strings.Index(query, "order by ")]
				}

				qsreg := regexp.MustCompile("'.+'|\".+\"|\\S+")

				querySplit := qsreg.FindAllString(strings.Replace(strings.Replace(query, "from", "", 1), "where", "", 1), -1)

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]

					if len(querySplit) == 2 {
						text.PrintfLine(fmt.Sprintf("%d Missing limit value.", 4016))
						query = ""
						continue
					}

					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["conditions"] = []string{""}
					body["lock"] = false
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey
					body["skip"] = 0

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 502, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine(fmt.Sprintf("%d Invalid limiting value.", 504))
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Could not convert limit value to integer. %s", 502, err.Error()))
							query = ""
							continue
						}
					}

					err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				} else {
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["skip"] = 0
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["sort-pos"] = sortPos
					body["sort-key"] = sortKey

					for k, s := range andOrSplit {
						querySplitNested := qsreg.FindAllString(strings.TrimSpace(strings.Replace(strings.Replace(strings.TrimSuffix(s, ";"), "from", "", 1), "where", "", 1)), -1)

						if len(querySplitNested) < 3 {
							text.PrintfLine(fmt.Sprintf("%d Invalid query.", 4017))
							query = ""
							goto extCont4
						}

						body["keys"] = append(body["keys"].([]interface{}), querySplitNested[len(querySplitNested)-3])
						body["oprs"] = append(body["oprs"].([]interface{}), querySplitNested[len(querySplitNested)-2])
						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "="):
							body["oprs"].([]interface{})[k] = "=="
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "like"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!like"):
						default:
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
							query = ""
							goto extCont4
						}

						body["values"] = append(body["values"].([]interface{}), strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";"))

						lindx := strings.Index(query, fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
						valLen := len(fmt.Sprintf("%s %s %v", querySplitNested[len(querySplitNested)-3], querySplitNested[len(querySplitNested)-2], body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

						if len(query[lindx+valLen:]) > 3 {
							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(query[lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cursus.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cursus.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable int value.", 4015))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						} else if cursus.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable float value.", 4014))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cursus.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unparsable boolean value.", 4013))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						}

					}

					if len(body["values"].([]interface{})) == 0 {
						text.PrintfLine(fmt.Sprintf("%d Where is missing values.", 506))
						query = ""
						continue
					}

					if body["limit"].(string) == "*" {
						body["limit"] = -1
					} else if strings.Contains(body["limit"].(string), ",") {
						if len(strings.Split(body["limit"].(string), ",")) == 2 {
							var err error
							body["skip"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[0])
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(body["limit"].(string), ",")[1], "*") {
								body["limit"], err = strconv.Atoi(strings.Split(body["limit"].(string), ",")[1])
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
									query = ""
									continue
								}
							} else {
								body["limit"] = -1
							}
						} else {
							text.PrintfLine("%d Invalid limiting value.", 504)
							query = ""
							continue
						}
					} else {
						var err error
						body["limit"], err = strconv.Atoi(body["limit"].(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Limit skip must be an integer. %s", 501, err.Error()))
							query = ""
							continue
						}
					}

					err = cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: nil}, body)
					if err != nil {
						text.PrintfLine(err.Error())
						query = ""
						continue
					}

					query = ""
					continue
				extCont4:
					continue
				}
				// end delete
			case strings.HasPrefix(query, "new user "):
				// start new user
				// new user username, password, RW
				splitQuery := strings.Split(query, "new user ")

				// now we split at comma if value is equal to 2
				if len(splitQuery) != 2 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				splQComma := strings.Split(splitQuery[1], ",")

				if len(splQComma) != 3 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				_, _, err = cursus.NewUser(strings.TrimSpace(splQComma[0]), strings.TrimSpace(splQComma[1]), strings.TrimSpace(splQComma[2]))
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				text.PrintfLine(fmt.Sprintf("%d New database user %s created successfully.", 200, strings.TrimSpace(splQComma[0])))
				query = ""
				continue
				// end new user
			case strings.HasPrefix(query, "users"):
				// start users
				var users []string

				// Get users based on in memory config
				for _, u := range cursus.Config.Users {
					username, err := base64.StdEncoding.DecodeString(strings.Split(u, ":")[0]) // get username splitting at :
					if err != nil {
						cursus.Printl("HandleClientConnection(): "+fmt.Sprintf("%d Could not decode user username.", 202), "ERROR")
						continue
					}
					users = append(users, string(username))
				}

				usersJsonArr, err := json.Marshal(users)
				if err != nil {
					text.PrintfLine("%d Could not marshal users list array.", 203)
					query = ""
					continue
				}

				text.PrintfLine(string(usersJsonArr)) // returns ["john", "jane", "jill"]

				query = ""
				continue
				// end users
			default:
				// start invalid
				text.PrintfLine("%d Invalid command/query.", 4005)
				query = ""
				continue
				// end invalid
			}

		}
	}
}

// IsString is a provided string a string literal?  "hello world"  OR 'hello world'
func (cursus *Cursus) IsString(str string) bool {
	switch {
	case strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\""): // has " and "
		return true
	case strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'"): // has ' and '
		return true
	default:
		return false
	}
}

// IsInt is a provided int an int?
func (cursus *Cursus) IsInt(str string) bool {
	if _, err := strconv.Atoi(str); err == nil { // Atoi because, why not?
		return true
	}
	return false
}

// IsFloat is a provided float a float64?
func (cursus *Cursus) IsFloat(str string) bool {
	if _, err := strconv.ParseFloat(str, 64); err == nil {
		return true
	}
	return false
}

// IsBool is a provided bool a bool?
func (cursus *Cursus) IsBool(str string) bool {
	if _, err := strconv.ParseBool(str); err == nil {
		return true
	}
	return false
}

// RemoveUser removes a user by username
func (cursus *Cursus) RemoveUser(username string) error {

	if len(cursus.Config.Users) == 1 {
		return errors.New(fmt.Sprintf("%d There must always be one database user available.", 204))
	}

	username = strings.TrimSuffix(username, ";")

	for j := 0; j < 50; j++ { // retry as gorm will serialize the bytes a bit different sometimes
		encodeUsername := base64.StdEncoding.EncodeToString([]byte(username))
		for i, user := range cursus.Config.Users {
			if strings.Split(user, ":")[0] == encodeUsername {
				cursus.ConfigMu.Lock() // Always lock config when modifying it for concurrent client cases
				cursus.Config.Users[i] = cursus.Config.Users[len(cursus.Config.Users)-1]
				cursus.Config.Users[len(cursus.Config.Users)-1] = ""
				cursus.Config.Users = cursus.Config.Users[:len(cursus.Config.Users)-1]
				cursus.ConfigMu.Unlock()
				return nil
			}
		}
	}

	return errors.New(fmt.Sprintf("%d No user found %s.", 102, username))
}

// LostReconnect connects to lost node or node replica connections, or will try to.
func (cursus *Cursus) LostReconnect() {
	defer cursus.Wg.Done() // Defer to return to waitgroup

	for {
		if cursus.Context.Err() != nil { // On signal break out of for loop
			break
		}

		for i, nc := range cursus.NodeConnections { // Iterate over node connections
			if !nc.Ok { // Check if node connection is not ok
				if cursus.Config.TLSNode { // Is TLS node configured?

					// Resolve TCP addr
					tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port))
					if err != nil {
						cursus.Printl(fmt.Sprintf("LostReconnect(): %s", err.Error()), "ERROR")
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// Dial tcp address up
					conn, err := net.DialTCP("tcp", nil, tcpAddr)
					if err != nil {
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// We will keep the node connection alive until shutdown
					conn.SetKeepAlive(true) // forever

					// Configure TLS
					config := tls.Config{ServerName: nc.Node.Host} // Either ServerName or InsecureSkipVerify will do it

					// Create TLS client connection
					secureConn := tls.Client(conn, &config)

					// Authenticate with node passing shared key wrapped in base64
					conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

					// Authentication response buffer
					authBuf := make([]byte, 1024)

					// Read response back from node
					r, _ := conn.Read(authBuf[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:r]), "0") {

						if !nc.Replica {
							cursus.NodeConnections[i] = &NodeConnection{
								Conn:       conn,
								SecureConn: secureConn,
								Text:       textproto.NewConn(secureConn),
								Node:       nc.Node,
								Mu:         &sync.Mutex{},
								Ok:         true,
							}
						} else {
							cursus.NodeConnections[i] = &NodeConnection{
								Conn:       conn,
								SecureConn: secureConn,
								Text:       textproto.NewConn(secureConn),
								Node:       nc.Node,
								Mu:         &sync.Mutex{},
								Ok:         true,
								Replica:    true,
							}
						}

						cursus.Printl(fmt.Sprintf("LostReconnect(): %d Reconnected to lost connection ", 116)+fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port), "INFO")
						time.Sleep(time.Nanosecond * 1000000)

					}
				} else {

					// Resolve TCP addr
					tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port))
					if err != nil {
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// Dial tcp address up
					conn, err := net.DialTCP("tcp", nil, tcpAddr)
					if err != nil {
						time.Sleep(time.Nanosecond * 1000000)
						continue
					}

					// We will keep the node connection alive until shutdown
					conn.SetKeepAlive(true) // forever

					// Authenticate with node passing shared key wrapped in base64
					conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

					// Authentication response buffer
					authBuf := make([]byte, 1024)

					// Read response back from node
					r, _ := conn.Read(authBuf[:])

					// Did response start with a 0?  This indicates successful authentication
					if strings.HasPrefix(string(authBuf[:r]), "0") {

						if !nc.Replica {
							cursus.NodeConnections[i] = &NodeConnection{
								Conn:    conn,
								Text:    textproto.NewConn(conn),
								Node:    nc.Node,
								Mu:      &sync.Mutex{},
								Ok:      true,
								Replica: false,
							}
						} else {
							cursus.NodeConnections[i] = &NodeConnection{
								Conn:    conn,
								Text:    textproto.NewConn(conn),
								Node:    nc.Node,
								Mu:      &sync.Mutex{},
								Ok:      true,
								Replica: true,
							}
						}

						cursus.Printl(fmt.Sprintf("LostReconnect(): %d Reconnected to lost connection ", 116)+fmt.Sprintf("%s:%d", nc.Node.Host, nc.Node.Port), "INFO")
						time.Sleep(time.Nanosecond * 1000000)
					}

					time.Sleep(time.Nanosecond * 1000000)

				}
			}
		}
		time.Sleep(time.Nanosecond * 1000000)
	}

}
