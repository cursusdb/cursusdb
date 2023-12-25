/*
* CursusDB
* Cluster Node
* ******************************************************************
* Originally authored by Alex Gaetano Padula
* Copyright (C) 2023 CursusDB
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
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"net"
	"net/textproto"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

// Curode is the main CursusDB cluster node struct
type Curode struct {
	TCPAddr       *net.TCPAddr       // TCPAddr represents the address of the nodes TCP end point
	TCPListener   *net.TCPListener   // TCPListener is the node TCP network listener.
	Wg            *sync.WaitGroup    // Node WaitGroup waits for all goroutines to finish up
	SignalChannel chan os.Signal     // Catch operating system signal
	Config        Config             // Node  config
	TLSConfig     *tls.Config        // Node TLS config if TLS is true
	ContextCancel context.CancelFunc // For gracefully shutting down
	ConfigMu      *sync.RWMutex      // Node config mutex
	Data          *Data              // Node data
	Context       context.Context    // Main looped go routine context.  This is for listeners, event loops and so forth
	LogMu         *sync.Mutex        // Log file mutex
	LogFile       *os.File           // Opened log file
}

// Config is the CursusDB cluster config struct
type Config struct {
	Replicas            []Replica `yaml:"replicas"`                        // Replicas are replica of this current node
	TLSCert             string    `yaml:"tls-cert"`                        // TLS cert path
	TLSKey              string    `yaml:"tls-key"`                         // TLS cert key
	Host                string    `yaml:"host"`                            // Node host i.e 0.0.0.0 usually
	TLS                 bool      `default:"false" yaml:"tls"`             // Use TLS?
	Port                int       `yaml:"port"`                            // Node port
	Key                 string    `yaml:"key"`                             // Key for a cluster to communicate with the node and also used to resting data.
	MaxMemory           uint64    `yaml:"max-memory"`                      // Default 10240MB = 10 GB (1024 * 10)
	LogMaxLines         int       `yaml:"log-max-lines"`                   // At what point to clear logs.  Each log line start's with a [UTC TIME] LOG DATA
	Logging             bool      `default:"false" yaml:"logging"`         // Log to file ?
	ReplicationSyncTime int       `yaml:"replication-sync-time"`           // in minutes default is every 10 minutes
	TLSReplication      bool      `default:"false" yaml:"tls-replication"` // If your cluster node replicas are running TLS then configure this to true
}

// Replica is a cluster node that current node data will be replicated/synced to
type Replica struct {
	Host string `yaml:"host"` // Host of replica i.e an ip or fqdn
	Port int    `yaml:"port"` // Port of replica
}

// Data is the node data struct
type Data struct {
	Map     map[string][]map[string]interface{} // Data hash map
	Writers map[string]*sync.RWMutex            // Collection writers
}

// Global variables
var (
	curode *Curode // main cluster node pointer
)

// Cluster node starts here
func main() {
	curode = &Curode{}                                                              // main cluster node variable
	curode.Wg = &sync.WaitGroup{}                                                   // create cluster node waitgroup
	curode.SignalChannel = make(chan os.Signal, 1)                                  // make signal channel
	curode.Context, curode.ContextCancel = context.WithCancel(context.Background()) // Create context for shutdown

	curode.Data = &Data{
		Map:     make(map[string][]map[string]interface{}),
		Writers: make(map[string]*sync.RWMutex),
	} // Make data map and collection writer mutex map

	gob.Register([]interface{}(nil)) // Fixes {"k": []}

	signal.Notify(curode.SignalChannel, syscall.SIGINT, syscall.SIGTERM)

	// Check if .curodeconfig exists
	if _, err := os.Stat("./.curodeconfig"); errors.Is(err, os.ErrNotExist) {

		// Create .curodeconfig
		nodeConfigFile, err := os.OpenFile("./.curodeconfig", os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Defer close node config
		defer nodeConfigFile.Close()

		curode.Config.Port = 7682              // Set default CursusDB node port
		curode.Config.MaxMemory = 10240        // Max memory 10GB default
		curode.Config.Host = "0.0.0.0"         // Set default host of 0.0.0.0
		curode.Config.LogMaxLines = 1000       // truncate at 1000 lines as default
		curode.Config.ReplicationSyncTime = 10 // default of every 10 minutes

		fmt.Println("Node key is required.  A node key is shared with your cluster and will encrypt all your data at rest and allow for only connections that contain a correct Key: header value matching the hashed key you provide.")
		fmt.Print("key> ")
		key, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Repeat key with * so Alex would be ****
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(key))))
		fmt.Println("")

		// Hash and encode key
		hashedKey := sha256.Sum256(key)
		curode.Config.Key = base64.StdEncoding.EncodeToString(append([]byte{}, hashedKey[:]...))

		// Marshal node config into yaml
		yamlData, err := yaml.Marshal(&curode.Config)
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Write to node config
		nodeConfigFile.Write(yamlData)
	} else {
		// Read node config
		nodeConfigFile, err := os.ReadFile("./.curodeconfig")
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Unmarshal node config yaml
		err = yaml.Unmarshal(nodeConfigFile, &curode.Config)
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

	}

	// Read rested data from .cdat file
	if _, err := os.Stat("./.cdat"); errors.Is(err, os.ErrNotExist) { // Not exists we create it
		curode.Printl(fmt.Sprintf("main(): No previous data to read.  Creating new .cdat file."), "INFO")
	} else {
		curode.Printl(fmt.Sprintf("main(): Node data read into memory."), "INFO")
		dataFile, err := os.Open("./.cdat") // Open .cdat

		// Temporary decrypted data file.. to be unserialized into map
		fDFTmp, err := os.OpenFile(".cdat.tmp", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Read encrypted data file
		reader := bufio.NewReader(dataFile)
		buf := make([]byte, 8192)

		defer dataFile.Close()

		for {
			read, err := reader.Read(buf)

			if err != nil {
				if err != io.EOF {
					fmt.Println("main():", err.Error())
					os.Exit(1)
				}
				break
			}

			if read > 0 {
				decodedKey, err := base64.StdEncoding.DecodeString(curode.Config.Key)
				if err != nil {
					fmt.Println("main():", err.Error())
					os.Exit(1)
					return
				}

				serialized, err := curode.Decrypt(decodedKey[:], buf[:read])
				if err != nil {
					fmt.Println("main():", err.Error())
					os.Exit(1)
					return
				}

				fDFTmp.Write(serialized) // Decrypt serialized
			}
		}
		fDFTmp.Close()

		fDFTmp, err = os.OpenFile(".cdat.tmp", os.O_RDONLY, 0777)
		if err != nil {
			curode.Printl("main(): "+fmt.Sprintf(err.Error()), "ERROR")
			os.Exit(1)
		}

		d := gob.NewDecoder(fDFTmp)

		// Now with all serialized data we encode into data hashmap
		err = d.Decode(&curode.Data.Map)
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		fDFTmp.Close()

		os.Remove(".cdat.tmp") // Remove temp

		// Setup collection mutexes
		for c, _ := range curode.Data.Map {
			curode.Data.Writers[c] = &sync.RWMutex{}
		}
		curode.Printl(fmt.Sprintf("main(): Collection mutexes created."), "INFO")
	}

	// Parse flags
	flag.IntVar(&curode.Config.Port, "port", curode.Config.Port, "port for node")
	flag.Parse()

	// If replicas are configured only then sync
	if len(curode.Config.Replicas) > 0 {
		curode.Wg.Add(1)
		go curode.SyncOut()
	}

	curode.Wg.Add(1)
	go curode.SignalListener() // Listen for system signals

	curode.Wg.Add(1)
	go curode.StartTCP_TLS() // Start listening tcp/tls with config

	curode.Wg.Wait() // Wait for go routines to finish

	os.Exit(0) // exit
}

// Decrypt decrypts .cdat file to temporary serialized data file to be read
func (curode *Curode) Decrypt(key, ciphertext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < aead.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}

	// Split nonce and ciphertext.
	nonce, ciphertext := ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():]

	return aead.Open(nil, nonce, ciphertext, nil)
}

// Encrypt encrypts a temporary serialized .cdat serialized file with chacha
func (curode *Curode) Encrypt(key, plaintext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}

	totalLen := aead.NonceSize() + len(plaintext) + aead.Overhead()
	nonce := make([]byte, aead.NonceSize(), totalLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// SignalListener listens for system signals
func (curode *Curode) SignalListener() {
	defer curode.Wg.Done()
	for {
		select {
		case sig := <-curode.SignalChannel:
			curode.Printl(fmt.Sprintf("SignalListener(): Received signal %s starting database shutdown.", sig), "INFO")
			curode.TCPListener.Close() // Close up TCP/TLS listener
			curode.WriteToFile()       // Write database data to file
			curode.ContextCancel()     // Cancel context, used for loops and so forth
			return
		default:
			time.Sleep(time.Nanosecond * 1000000)
		}
	}
}

// SyncOut syncs current data to replicas at configured interval
func (curode *Curode) SyncOut() {
	defer curode.Wg.Done()

	stateCh := make(chan int)
	// 0 - continue
	// 1 - sleep
	// 2 - cancel

	go func(c *Curode, sc chan int) {
		f := time.Now().Add(time.Minute * time.Duration(curode.Config.ReplicationSyncTime))
		for {
			if c.Context.Err() != nil {
				sc <- 2
				return
			}

			if time.Now().After(f) {
				f = time.Now().Add(time.Minute * time.Duration(curode.Config.ReplicationSyncTime))
				sc <- 0
				time.Sleep(time.Nanosecond * 1000000)
			} else {
				sc <- 1
				time.Sleep(time.Nanosecond * 1000000)
			}
		}
	}(curode, stateCh)

	for {
		select {
		case sc := <-stateCh:

			if sc == 0 {
				for _, r := range curode.Config.Replicas {

					if curode.Config.TLSReplication {
						config := tls.Config{InsecureSkipVerify: false}

						conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", r.Host, r.Port), &config)
						if err != nil {
							curode.Printl("SyncOut():"+err.Error(), "ERROR")
							continue
						}

						// Authenticate with node passing shared key wrapped in base64
						conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", curode.Config.Key)))

						// Authentication response buffer
						authBuf := make([]byte, 1024)

						// Read response back from node
						re, _ := conn.Read(authBuf[:])

						// Did response start with a 0?  This indicates successful authentication
						if strings.HasPrefix(string(authBuf[:re]), "0") {

							conn.Write([]byte(fmt.Sprintf("SYNC DATA\r\n")))
							syncDataResponseBuf := make([]byte, 1024)

							// Read response back from node
							syncDataResponse, _ := conn.Read(syncDataResponseBuf[:])
							if strings.HasPrefix(string(syncDataResponseBuf[:syncDataResponse]), "106") {

								// Handle sync after auth
								// Serialize current data
								b := new(bytes.Buffer)

								e := gob.NewEncoder(b)

								err = e.Encode(curode.Data.Map)
								if err != nil {
									conn.Close()
									curode.Printl(fmt.Sprintf("SyncOut(): %s", err.Error()), "ERROR")
									break
								}

								rdbuf := make([]byte, 8192)
								for {
									_, err = b.Read(rdbuf)
									if err != nil {
										break
									}

									conn.Write(append(rdbuf, []byte("\r\n")...))

								}

								conn.Write([]byte(fmt.Sprintf("SYNC FINISHED\r\n")))
								syncFinishResponseBuf := make([]byte, 1024)

								// Read response back from node
								syncFinishResponse, _ := conn.Read(syncFinishResponseBuf[:])

								curode.Printl("SyncOut(): "+string(syncFinishResponseBuf[:syncFinishResponse]), "INFO")

								conn.Close()
							}
						} else {
							curode.Printl("SyncOut():"+fmt.Sprintf("%d Failed node sync auth %s", 5, string(authBuf[:re])), "ERROR")
						}
					} else {
						// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
						tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", r.Host, r.Port))
						if err != nil {
							fmt.Println("SyncOut():", err.Error())
							curode.Printl(fmt.Sprintf("SyncOut(): %s", err.Error()), "ERROR")
							continue
						}

						// Dial tcp address up
						conn, err := net.DialTCP("tcp", nil, tcpAddr)
						if err != nil {
							curode.Printl(fmt.Sprintf("SyncOut(): %s", err.Error()), "ERROR")
							continue
						}

						// Authenticate with node passing shared key wrapped in base64
						conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", curode.Config.Key)))

						// Authentication response buffer
						authBuf := make([]byte, 1024)

						// Read response back from node
						re, _ := conn.Read(authBuf[:])

						// Did response start with a 0?  This indicates successful authentication
						if strings.HasPrefix(string(authBuf[:re]), "0") {

							conn.Write([]byte(fmt.Sprintf("SYNC DATA\r\n")))
							syncDataResponseBuf := make([]byte, 1024)

							// Read response back from node
							syncDataResponse, _ := conn.Read(syncDataResponseBuf[:])
							if strings.HasPrefix(string(syncDataResponseBuf[:syncDataResponse]), "106") {

								// Handle sync after auth
								// Serialize current data
								b := new(bytes.Buffer)

								e := gob.NewEncoder(b)

								err = e.Encode(curode.Data.Map)
								if err != nil {
									conn.Close()
									curode.Printl(fmt.Sprintf("SyncOut(): %s", err.Error()), "ERROR")
									break
								}

								rdbuf := make([]byte, 8192)
								for {
									_, err = b.Read(rdbuf)
									if err != nil {
										break
									}

									conn.Write(append(rdbuf, []byte("\r\n")...))

								}

								conn.Write([]byte(fmt.Sprintf("SYNC FINISHED\r\n")))
								syncFinishResponseBuf := make([]byte, 1024)

								// Read response back from node
								syncFinishResponse, _ := conn.Read(syncFinishResponseBuf[:])

								curode.Printl("SyncOut(): "+string(syncFinishResponseBuf[:syncFinishResponse]), "INFO")

								conn.Close()
							}
						} else {
							curode.Printl("SyncOut():"+fmt.Sprintf("%d Failed node sync auth %s", 5, string(authBuf[:re])), "ERROR")
						}
					}

				}
			} else if sc == 2 {
				return
			} else if sc == 1 {
				time.Sleep(time.Nanosecond * 1000000)
				continue
			}
		default:
			time.Sleep(time.Nanosecond * 1000000)
		}
	}
}

// CountLog counts amount of lines within log file
func (curode *Curode) CountLog(r io.Reader) int {
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
			curode.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s - %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
			return 99999999
		}
	}
}

// Printl prints a line to the curode.log file also will clear at LogMaxLines.
// Appropriate levels: ERROR, INFO, FATAL, WARN
func (curode *Curode) Printl(data string, level string) {
	if curode.Config.Logging {
		if curode.CountLog(curode.LogFile)+1 >= curode.Config.LogMaxLines {
			curode.LogMu.Lock()
			defer curode.LogMu.Unlock()
			curode.LogFile.Close()
			err := os.Truncate(curode.LogFile.Name(), 0)
			if err != nil {
				curode.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s - %s\r\n", "ERROR", time.Now().UTC(), "Count not count up log lines.", err.Error())))
				return
			}

			curode.LogFile, err = os.OpenFile("curode.log", os.O_CREATE|os.O_RDWR, 0777)
			if err != nil {
				return
			}
			curode.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().UTC(), fmt.Sprintf("Printl(): Log truncated at %d", curode.Config.LogMaxLines))))
			curode.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().UTC(), data)))
		} else {
			curode.LogFile.Write([]byte(fmt.Sprintf("[%s][%s] %s\r\n", level, time.Now().UTC(), data)))
		}
	} else {
		log.Println(fmt.Sprintf("[%s] %s", level, data))
	}

}

// WriteToFile will write the current node data to a .cdat file encrypted with your node key.
func (curode *Curode) WriteToFile() {
	curode.Printl(fmt.Sprintf("WriteToFile(): Starting to write node data to file."), "INFO")

	// Create temporary .cdat which is all serialized data.  An encryption is performed after the fact to not consume memory.
	fTmp, err := os.OpenFile(".cdat.tmp", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		curode.Printl(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
		curode.SignalChannel <- os.Interrupt
		return
	}

	e := gob.NewEncoder(fTmp)

	// Encoding the map
	err = e.Encode(curode.Data.Map)
	if err != nil {
		curode.Printl(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
		curode.SignalChannel <- os.Interrupt
		return
	}

	fTmp.Close()

	// After serialization encrypt temp data file
	fTmp, err = os.OpenFile(".cdat.tmp", os.O_RDONLY, 0777)
	if err != nil {
		curode.Printl(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
		curode.SignalChannel <- os.Interrupt
		return
	}

	//
	reader := bufio.NewReader(fTmp)
	buf := make([]byte, 8192)
	f, err := os.OpenFile(".cdat", os.O_TRUNC|os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		curode.Printl(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
		curode.SignalChannel <- os.Interrupt
		return
	}
	defer f.Close()

	for {
		read, err := reader.Read(buf)

		if err != nil {
			if err != io.EOF {
				curode.Printl(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
				curode.SignalChannel <- os.Interrupt
				return
			}
			break
		}

		if read > 0 {
			decodedKey, err := base64.StdEncoding.DecodeString(curode.Config.Key)
			if err != nil {
				curode.Printl(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
				curode.SignalChannel <- os.Interrupt
				return
			}

			cipherblock, err := curode.Encrypt(decodedKey[:], buf[:read])
			if err != nil {
				curode.Printl(fmt.Sprintf("WriteToFile(): %s", err.Error()), "ERROR")
				curode.SignalChannel <- os.Interrupt
				return
			}

			f.Write(cipherblock)
		}
	}

	os.Remove(".cdat.tmp")
	curode.Printl(fmt.Sprintf("WriteToFile(): Node data written to file successfully."), "INFO")
}

// StartTCP_TLS starts listening on tcp/tls on configured host and port
func (curode *Curode) StartTCP_TLS() {
	var err error
	defer curode.Wg.Done()

	curode.TCPAddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", curode.Config.Host, curode.Config.Port))
	if err != nil {
		curode.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
		curode.SignalChannel <- os.Interrupt
		return
	}

	// Start listening for TCP connections on the given address
	curode.TCPListener, err = net.ListenTCP("tcp", curode.TCPAddr)
	if err != nil {
		curode.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
		curode.SignalChannel <- os.Interrupt
		return
	}

	for {
		conn, err := curode.TCPListener.Accept()
		if err != nil {
			curode.SignalChannel <- os.Interrupt
			return
		}

		// If TLS is set to true within config let's make the connection secure
		if curode.Config.TLS {
			cert, err := tls.LoadX509KeyPair(curode.Config.TLSCert, curode.Config.TLSKey)
			if err != nil {
				curode.Printl("StartTCP_TLS(): "+err.Error(), "FATAL")
				curode.SignalChannel <- os.Interrupt
				return
			}

			curode.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}

			tlsUpgrade := tls.Server(conn, curode.TLSConfig)
			tlsUpgrade.Handshake()
			conn = net.Conn(tlsUpgrade)
		}

		auth, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			curode.Printl("StartTCP_TLS(): "+fmt.Sprintf("StartTCPListener(): %s", err.Error()), "ERROR")
			continue
		}

		authSpl := strings.Split(strings.TrimSpace(auth), "Key:")
		if len(authSpl) != 2 {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 1, "Missing authentication header.")))
			conn.Close()
			continue
		}

		if curode.Config.Key == strings.TrimSpace(authSpl[1]) {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 0, "Authentication successful.")))

			curode.Wg.Add(1)
			go curode.HandleClientConnection(conn)
		} else {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}
	}
}

// HandleClientConnection handles tcp/tls client connection
func (curode *Curode) HandleClientConnection(conn net.Conn) {
	defer curode.Wg.Done()
	defer conn.Close()
	text := textproto.NewConn(conn)
	defer text.Close()

	syncStarted := false                     // Whether a sync has started
	serializedToMarshal := new(bytes.Buffer) // incoming serialized data for replica sync

	for {
		conn.SetReadDeadline(time.Now().Add(time.Nanosecond * 1000000))
		read, err := text.ReadLine()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if curode.Context.Err() != nil {
					break
				}
				continue
			} else {
				break
			}
		}

		// Only another node would send SYNC DATA after passing shared node-cluster key at which point the current node will start to consume serialized data to marshal into database hashmap
		if strings.HasPrefix(read, "SYNC DATA") {
			curode.Printl("HandleClientConnection(): "+fmt.Sprintf("Node sync starting for replica %s", conn.RemoteAddr().String()), "INFO")
			// Handle sync
			conn.Write([]byte(fmt.Sprintf("%d Node ready for sync\r\n", 106)))

			syncStarted = true

			continue
		} else if strings.HasPrefix(read, "SYNC FINISHED") {
			curode.Printl("HandleClientConnection(): "+fmt.Sprintf("Node sync finished for replica %s", conn.RemoteAddr().String()), "INFO")
			d := gob.NewDecoder(serializedToMarshal)

			err = d.Decode(&curode.Data.Map)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("%d Could not decode serialized sync data into hashmap.\r\n", 108)))
				continue
			}
			curode.Printl("HandleClientConnection(): "+fmt.Sprintf("Node sync was succesful for replica %s", conn.RemoteAddr().String()), "INFO")
			conn.Write([]byte(fmt.Sprintf("%d Node replica synced successfully.\r\n", 107)))
			continue
		} else if syncStarted {
			serializedToMarshal.Write([]byte(read))
			continue
		}

		response := make(map[string]interface{}) // response back to cluster

		request := make(map[string]interface{}) // request incoming from cluster

		err = json.Unmarshal([]byte(strings.TrimSpace(string(read))), &request)
		if err != nil {
			response["statusCode"] = 4000
			response["message"] = "Unmarshalable JSON"
			r, _ := json.Marshal(response)
			text.PrintfLine(string(r))
			continue
		}

		action, ok := request["action"] // An action is insert, select, delete, ect..
		if ok {
			switch {
			case strings.EqualFold(action.(string), "delete"):

				results := curode.Delete(request["collection"].(string), request["keys"], request["values"], int(request["limit"].(float64)), int(request["skip"].(float64)), request["oprs"], request["lock"].(bool), request["conditions"].([]interface{}), request["sort-pos"].(string), request["sort-key"].(string))
				r, _ := json.Marshal(results)
				response["statusCode"] = 2000

				if reflect.DeepEqual(results, nil) || len(results) == 0 {
					response["message"] = "No documents deleted."
				} else {
					response["message"] = fmt.Sprintf("%d Document(s) deleted successfully.", len(results))
				}

				response["deleted"] = results

				r, _ = json.Marshal(response)
				text.PrintfLine(string(r))
				continue
			case strings.EqualFold(action.(string), "select"):

				if request["count"] == nil {
					request["count"] = false
				}

				results := curode.Select(request["collection"].(string), request["keys"], request["values"], int(request["limit"].(float64)), int(request["skip"].(float64)), request["oprs"], request["lock"].(bool), request["conditions"].([]interface{}), false, request["sort-pos"].(string), request["sort-key"].(string), request["count"].(bool))
				r, _ := json.Marshal(results)
				text.PrintfLine(string(r))
				continue
			case strings.EqualFold(action.(string), "update"):

				results := curode.Update(request["collection"].(string),
					request["keys"].([]interface{}), request["values"].([]interface{}),
					int(request["limit"].(float64)), int(request["skip"].(float64)), request["oprs"].([]interface{}),
					request["lock"].(bool),
					request["conditions"].([]interface{}),
					request["update-keys"].([]interface{}), request["new-values"].([]interface{}),
					request["sort-pos"].(string), request["sort-key"].(string))
				r, _ := json.Marshal(results)

				response["statusCode"] = 2000

				if reflect.DeepEqual(results, nil) || len(results) == 0 {
					response["message"] = "No documents updated."
				} else {
					response["message"] = fmt.Sprintf("%d Document(s) updated successfully.", len(results))
				}

				response["updated"] = results
				r, _ = json.Marshal(response)

				text.PrintfLine(string(r))
				continue
			case strings.EqualFold(action.(string), "insert"):

				collection := request["collection"]
				doc := request["document"]

				err := curode.Insert(collection.(string), doc.(map[string]interface{}), text)
				if err != nil {
					// Only error returned is a 4003 which means cannot insert nested object
					response["statusCode"] = strings.Split(err.Error(), " ")[0]
					response["message"] = strings.Split(err.Error(), " ")[1]
					r, _ := json.Marshal(response)
					text.PrintfLine(string(r))
					continue
				}

				continue
			default:

				response["statusCode"] = 4002
				response["message"] = "Invalid/Non-existent action"
				r, _ := json.Marshal(response)

				text.PrintfLine(string(r))
				continue
			}
		} else {
			response["statusCode"] = 4001
			response["message"] = "Missing action" // Missing select, insert
			r, _ := json.Marshal(response)

			text.PrintfLine(string(r))
			continue
		}

	}
}

// CurrentMemoryUsage returns current memory usage in mb
func (curode *Curode) CurrentMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.Alloc / 1024 / 1024
}

// Insert into node collection
func (curode *Curode) Insert(collection string, jsonMap map[string]interface{}, text *textproto.Conn) error {
	if curode.CurrentMemoryUsage() >= curode.Config.MaxMemory {
		return errors.New(fmt.Sprintf("%d node is at peak allocation", 100))
	}

	jsonStr, err := json.Marshal(jsonMap)
	if err != nil {
		return errors.New(fmt.Sprintf("%d Could not marshal JSON", 4012))
	}

	if strings.Contains(string(jsonStr), "[{\"") {
		return errors.New("nested JSON objects not permitted")
	} else if strings.Contains(string(jsonStr), ": {\"") {
		return errors.New("nested JSON objects not permitted")
	} else if strings.Contains(string(jsonStr), ":{\"") {
		return errors.New("nested JSON objects not permitted")
	}

	doc := make(map[string]interface{})
	err = json.Unmarshal([]byte(jsonStr), &doc)
	if err != nil {
		return errors.New(fmt.Sprintf("%d Unmarsharable JSON insert", 4000))
	}
	writeMu, ok := curode.Data.Writers[collection]
	if ok {
		writeMu.Lock()

		curode.Data.Map[collection] = append(curode.Data.Map[collection], doc)
		writeMu.Unlock()
	} else {
		curode.Data.Writers[collection] = &sync.RWMutex{}
		curode.Data.Map[collection] = append(curode.Data.Map[collection], doc)
	}

	response := make(map[string]interface{})
	response["statusCode"] = 2000
	response["message"] = "Document inserted"

	response["insert"] = doc

	responseMap, err := json.Marshal(response)
	if err != nil {
		return errors.New(fmt.Sprintf("%d Could not marshal JSON", 4012))
	}

	text.PrintfLine(string(responseMap))

	return nil
}

// Select is the node data select method
func (curode *Curode) Select(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}, del bool, sortPos string, sortKey string, count bool) []interface{} {
	// sortPos = desc OR asc
	// sortKey = createdAt for example a unix timestamp of 1703234712 or firstName with a value of Alex sorting will sort alphabetically

	// If a lock was sent from cluster lock the collection on this read
	if lock {
		l, ok := curode.Data.Writers[collection]
		if ok {
			l.Lock()
		}

	}

	// Unlock when completed, by defering
	defer func() {
		if lock {
			l, ok := curode.Data.Writers[collection]
			if ok {
				l.Unlock()
			}
		}
	}()

	// Results
	var objects []interface{}

	// To be deleted
	var tbd []int64

	//The && operator displays a document if all the conditions are TRUE.
	//The || operator displays a record if any of the conditions are TRUE.

	// Linearly search collection documents by using a range loop
	for i, d := range curode.Data.Map[collection] {

		conditionsMetDocument := 0 // conditions met as in th first condition would be key == v lets say the next would be && or || etc..

		// if keys, values and operators are nil
		// This could be a case of "select * from users;" for example if passing skip and volume checks
		if ks == nil && ks == nil && oprs == nil {

			// decrement skip and continue
			if skip != 0 {
				skip = skip - 1
				continue
			}

			// if a volume is set check if we are at wanted document volume for query
			if vol != -1 {
				if len(objects) == vol { // Does currently collected documents equal desired volume?
					goto cont // return pretty much
				}
			}

			// add document to objects
			objects = append(objects, d)

			if del {
				tbd = append(tbd, int64(i))
			}

			continue
		} else {

			// range over provided keys
			for m, k := range ks.([]interface{}) {

				if oprs.([]interface{})[m] == "" {
					return nil
				}

				if vol != -1 {
					if len(objects) == vol {
						goto cont
					}
				}

				vType := fmt.Sprintf("%T", vs.([]interface{})[m])

				_, ok := d[k.(string)]
				if ok {

					if d[k.(string)] == nil {
						conditionsMetDocument += 1
						continue
					}

					if reflect.TypeOf(d[k.(string)]).Kind() == reflect.Slice {
						for _, dd := range d[k.(string)].([]interface{}) {

							if vol != -1 {
								if len(objects) == vol {
									goto cont
								}
							}

							if reflect.TypeOf(dd).Kind() == reflect.Float64 {
								if vType == "int" {
									var interfaceI int = int(dd.(float64))

									if oprs.([]interface{})[m] == "==" {
										if reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}
									} else if oprs.([]interface{})[m] == "!=" {
										if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == ">" {
										if vType == "int" {
											if interfaceI > vs.([]interface{})[m].(int) {

												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													if skip != 0 {
														skip = skip - 1
														goto exists
													}
													conditionsMetDocument += 1
												exists:
												})()

											}
										}
									} else if oprs.([]interface{})[m] == "<" {
										if vType == "int" {
											if interfaceI < vs.([]interface{})[m].(int) {

												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													if skip != 0 {
														skip = skip - 1
														goto exists
													}
													conditionsMetDocument += 1
												exists:
												})()

											}
										}
									} else if oprs.([]interface{})[m] == ">=" {
										if vType == "int" {
											if interfaceI >= vs.([]interface{})[m].(int) {

												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													if skip != 0 {
														skip = skip - 1
														goto exists
													}
													conditionsMetDocument += 1
												exists:
												})()

											}
										}
									} else if oprs.([]interface{})[m] == "<=" {
										if vType == "int" {
											if interfaceI <= vs.([]interface{})[m].(int) {

												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													if skip != 0 {
														skip = skip - 1
														goto exists
													}
													conditionsMetDocument += 1
												exists:
												})()

											}
										}
									}
								} else if vType == "float64" {
									var interfaceI float64 = dd.(float64)

									if oprs.([]interface{})[m] == "==" {

										if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}
									} else if oprs.([]interface{})[m] == "!=" {
										if float64(interfaceI) != vs.([]interface{})[m].(float64) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}
									} else if oprs.([]interface{})[m] == ">" {
										if float64(interfaceI) > vs.([]interface{})[m].(float64) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}

									} else if oprs.([]interface{})[m] == "<" {
										if float64(interfaceI) < vs.([]interface{})[m].(float64) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}

									} else if oprs.([]interface{})[m] == ">=" {

										if float64(interfaceI) >= vs.([]interface{})[m].(float64) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}

									} else if oprs.([]interface{})[m] == "<=" {
										if float64(interfaceI) <= vs.([]interface{})[m].(float64) {

											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												if skip != 0 {
													skip = skip - 1
													goto exists
												}
												conditionsMetDocument += 1
											exists:
											})()

										}

									}
								}
							} else if reflect.TypeOf(dd).Kind() == reflect.Map {
								//for kkk, ddd := range dd.(map[string]interface{}) {
								//	// unimplemented
								//}
							} else {
								// string
								if oprs.([]interface{})[m] == "like" {
									vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
									vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
									vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
									vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
									vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
									vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

									if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
										// Get index of % and check if on left or right of string
										percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
										sMiddle := len(vs.([]interface{})[m].(string)) / 2
										right := sMiddle <= percIndex

										if right {
											r := regexp.MustCompile(`^(.*?)%`)
											patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

											for j, _ := range patterns {
												patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
											}

											for _, p := range patterns {
												// does value start with p

												if strings.HasPrefix(dd.(string), p) {
													if skip != 0 {
														skip = skip - 1
														goto s
													}
													conditionsMetDocument += 1

												s:
													continue
												}
											}
										} else {
											r := regexp.MustCompile(`\%(.*)`)
											patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

											for j, _ := range patterns {
												patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
											}

											for _, p := range patterns {
												// does value end with p
												if strings.HasSuffix(dd.(string), p) {
													if skip != 0 {
														skip = skip - 1
														goto s2
													}
													conditionsMetDocument += 1

												s2:
													continue
												}
											}
										}
									} else {

										r := regexp.MustCompile(`%(.*?)%`)
										patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

										for j, _ := range patterns {
											patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
										}

										for _, p := range patterns {
											// does value contain p
											if strings.Count(dd.(string), p) > 0 {
												if skip != 0 {
													skip = skip - 1
													goto s3
												}
												conditionsMetDocument += 1

											s3:
												continue
											}
										}
									}
								} else if oprs.([]interface{})[m] == "!like" {
									vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
									vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
									vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
									vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
									vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
									vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

									// select * from users where firstName not like 'alex%'
									if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
										// Get index of % and check if on left or right of string
										percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
										sMiddle := len(vs.([]interface{})[m].(string)) / 2
										right := sMiddle <= percIndex

										if right {
											r := regexp.MustCompile(`^(.*?)%`)
											patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

											for j, _ := range patterns {
												patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
											}

											for _, p := range patterns {
												// does value start with p
												if !strings.HasPrefix(dd.(string), p) {
													if skip != 0 {
														skip = skip - 1
														goto s4
													}
													conditionsMetDocument += 1

												s4:
													continue
												}
											}
										} else {
											r := regexp.MustCompile(`\%(.*)`)
											patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

											for j, _ := range patterns {
												patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
											}

											for _, p := range patterns {
												// does value end with p
												if !strings.HasSuffix(dd.(string), p) {
													if skip != 0 {
														skip = skip - 1
														goto s5
													}
													conditionsMetDocument += 1

												s5:
													continue
												}
											}
										}
									} else {

										r := regexp.MustCompile(`%(.*?)%`)
										patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

										for j, _ := range patterns {
											patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
										}

										for _, p := range patterns {
											// does value contain p
											if strings.Count(dd.(string), p) == 0 {
												if skip != 0 {
													skip = skip - 1
													goto s6
												}
												conditionsMetDocument += 1

											s6:
												continue
											}
										}
									}
								} else if oprs.([]interface{})[m] == "==" {
									if reflect.DeepEqual(dd, vs.([]interface{})[m]) {

										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}
								} else if oprs.([]interface{})[m] == "!=" {
									if !reflect.DeepEqual(dd, vs.([]interface{})[m]) {

										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											if skip != 0 {
												skip = skip - 1
												goto exists
											}
											conditionsMetDocument += 1
										exists:
										})()

									}
								}
							}

						}
					} else if vType == "int" {
						var interfaceI int = int(d[k.(string)].(float64))

						if oprs.([]interface{})[m] == "==" {
							if reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						} else if oprs.([]interface{})[m] == "!=" {
							if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						} else if oprs.([]interface{})[m] == ">" {
							if vType == "int" {
								if interfaceI > vs.([]interface{})[m].(int) {

									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										if skip != 0 {
											skip = skip - 1
											goto exists
										}
										conditionsMetDocument += 1
									exists:
									})()

								}
							}
						} else if oprs.([]interface{})[m] == "<" {
							if vType == "int" {
								if interfaceI < vs.([]interface{})[m].(int) {

									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										if skip != 0 {
											skip = skip - 1
											goto exists
										}
										conditionsMetDocument += 1
									exists:
									})()

								}
							}
						} else if oprs.([]interface{})[m] == ">=" {
							if vType == "int" {
								if interfaceI >= vs.([]interface{})[m].(int) {

									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										if skip != 0 {
											skip = skip - 1
											goto exists
										}
										conditionsMetDocument += 1
									exists:
									})()

								}
							}
						} else if oprs.([]interface{})[m] == "<=" {
							if vType == "int" {
								if interfaceI <= vs.([]interface{})[m].(int) {

									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										if skip != 0 {
											skip = skip - 1
											goto exists
										}
										conditionsMetDocument += 1
									exists:
									})()

								}
							}
						}
					} else if vType == "float64" {
						var interfaceI float64 = d[k.(string)].(float64)

						if oprs.([]interface{})[m] == "==" {

							if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						} else if oprs.([]interface{})[m] == "!=" {
							if float64(interfaceI) != vs.([]interface{})[m].(float64) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						} else if oprs.([]interface{})[m] == ">" {
							if float64(interfaceI) > vs.([]interface{})[m].(float64) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}

						} else if oprs.([]interface{})[m] == "<" {
							if float64(interfaceI) < vs.([]interface{})[m].(float64) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}

						} else if oprs.([]interface{})[m] == ">=" {

							if float64(interfaceI) >= vs.([]interface{})[m].(float64) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}

						} else if oprs.([]interface{})[m] == "<=" {
							if float64(interfaceI) <= vs.([]interface{})[m].(float64) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}

						}
					} else { // string

						if oprs.([]interface{})[m] == "like" {
							vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
							vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
							vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
							vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
							vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
							vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

							// select * from users where firstName like 'alex%'
							if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
								// Get index of % and check if on left or right of string
								percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
								sMiddle := len(vs.([]interface{})[m].(string)) / 2
								right := sMiddle <= percIndex

								if right {
									r := regexp.MustCompile(`^(.*?)%`)
									patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

									for j, _ := range patterns {
										patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
									}

									for _, p := range patterns {
										// does value start with p
										if strings.HasPrefix(d[k.(string)].(string), p) {
											if skip != 0 {
												skip = skip - 1
												goto sk
											}
											conditionsMetDocument += 1

										sk:
											continue
										}
									}
								} else {
									r := regexp.MustCompile(`\%(.*)`)
									patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

									for j, _ := range patterns {
										patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
									}

									for _, p := range patterns {
										// does value end with p
										if strings.HasSuffix(d[k.(string)].(string), p) {
											if skip != 0 {
												skip = skip - 1
												goto sk2
											}
											conditionsMetDocument += 1

										sk2:
											continue
										}
									}
								}
							} else {

								r := regexp.MustCompile(`%(.*?)%`)
								patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

								for j, _ := range patterns {
									patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
								}

								for _, p := range patterns {
									// does value contain p
									if strings.Count(d[k.(string)].(string), p) > 0 {
										if skip != 0 {
											skip = skip - 1
											goto sk3
										}
										conditionsMetDocument += 1

									sk3:
										continue
									}
								}
							}
						} else if oprs.([]interface{})[m] == "!like" {
							vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!'(MISSING)", "'")
							vs.([]interface{})[m] = strings.ReplaceAll(vs.([]interface{})[m].(string), "!\"(MISSING)", "\"")
							vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "'")
							vs.([]interface{})[m] = strings.TrimPrefix(vs.([]interface{})[m].(string), "\"")
							vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "'")
							vs.([]interface{})[m] = strings.TrimSuffix(vs.([]interface{})[m].(string), "\"")

							// select * from users where firstName not like 'alex%'
							if strings.Count(vs.([]interface{})[m].(string), "%") == 1 {
								// Get index of % and check if on left or right of string
								percIndex := strings.Index(vs.([]interface{})[m].(string), "%")
								sMiddle := len(vs.([]interface{})[m].(string)) / 2
								right := sMiddle <= percIndex

								if right {
									r := regexp.MustCompile(`^(.*?)%`)
									patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

									for j, _ := range patterns {
										patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
									}

									for _, p := range patterns {
										// does value start with p

										if !strings.HasPrefix(d[k.(string)].(string), p) {
											if skip != 0 {
												skip = skip - 1
												goto sk4
											}
											conditionsMetDocument += 1

										sk4:
											continue
										}
									}
								} else {
									r := regexp.MustCompile(`\%(.*)`)
									patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

									for j, _ := range patterns {
										patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
									}

									for _, p := range patterns {
										// does value end with p
										if !strings.HasSuffix(d[k.(string)].(string), p) {
											if skip != 0 {
												skip = skip - 1
												goto sk5
											}
											conditionsMetDocument += 1

										sk5:
											continue
										}
									}
								}
							} else {

								r := regexp.MustCompile(`%(.*?)%`)
								patterns := r.FindAllString(vs.([]interface{})[m].(string), -1)

								for j, _ := range patterns {
									patterns[j] = strings.TrimSuffix(strings.TrimPrefix(patterns[j], "%"), "%")
								}

								for _, p := range patterns {
									// does value contain p
									if strings.Count(d[k.(string)].(string), p) == 0 {
										if skip != 0 {
											skip = skip - 1
											goto sk6
										}
										conditionsMetDocument += 1

									sk6:
										continue
									}
								}
							}
						} else if oprs.([]interface{})[m] == "==" {
							if reflect.DeepEqual(d[k.(string)], vs.([]interface{})[m]) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						} else if oprs.([]interface{})[m] == "!=" {
							if !reflect.DeepEqual(d[k.(string)], vs.([]interface{})[m]) {

								(func() {

									if skip != 0 {
										skip = skip - 1
										goto exists
									}
									conditionsMetDocument += 1
								exists:
								})()

							}
						}

					}
				}
			}

			if slices.Contains(conditions, "&&") {
				if conditionsMetDocument >= len(conditions) {
					objects = append(objects, d)
					if del {
						tbd = append(tbd, int64(i))
					}
				} else if slices.Contains(conditions, "||") && conditionsMetDocument > 0 {
					objects = append(objects, d)
					if del {
						tbd = append(tbd, int64(i))
					}
				}
			} else if slices.Contains(conditions, "||") && conditionsMetDocument > 0 {
				objects = append(objects, d)
				if del {
					tbd = append(tbd, int64(i))
				}
			} else if conditionsMetDocument > 0 && len(conditions) == 1 {
				objects = append(objects, d)
				if del {
					tbd = append(tbd, int64(i))
				}
			}

		}

	}

	goto cont

cont:

	// Should only sort integers, floats and strings
	if sortKey != "" && sortPos != "" {

		for _, d := range objects {

			doc, ok := d.(map[string]interface{})[sortKey]
			if ok {
				if reflect.TypeOf(doc).Kind().String() == "string" {
					// alphabetical sorting based on string[0] value A,B,C asc C,B,A desc
					sort.Slice(objects[:], func(z, x int) bool {
						if sortPos == "asc" {
							return objects[z].(map[string]interface{})[sortKey].(string) < objects[x].(map[string]interface{})[sortKey].(string)
						} else {
							return objects[z].(map[string]interface{})[sortKey].(string) > objects[x].(map[string]interface{})[sortKey].(string)
						}
					})
				} else if reflect.TypeOf(d.(map[string]interface{})[sortKey]).Kind().String() == "float64" {
					// numerical sorting based on float64[0] value 1.1,1.0,0.9 desc 0.9,1.0,1.1 asc
					sort.Slice(objects[:], func(z, x int) bool {
						if sortPos == "asc" {
							return objects[z].(map[string]interface{})[sortKey].(float64) < objects[x].(map[string]interface{})[sortKey].(float64)
						} else {
							return objects[z].(map[string]interface{})[sortKey].(float64) > objects[x].(map[string]interface{})[sortKey].(float64)
						}
					})
				} else if reflect.TypeOf(d.(map[string]interface{})[sortKey]).Kind().String() == "int" {
					// numerical sorting based on int[0] value 22,12,3 desc 3,12,22 asc
					sort.Slice(objects[:], func(z, x int) bool {
						if sortPos == "asc" {
							return objects[z].(map[string]interface{})[sortKey].(int) < objects[x].(map[string]interface{})[sortKey].(int)
						} else {
							return objects[z].(map[string]interface{})[sortKey].(int) > objects[x].(map[string]interface{})[sortKey].(int)
						}
					})
				}

			}

		}
	}

	if count {
		var countResponse []interface{}
		countObject := make(map[string]interface{})

		countObject["count"] = len(objects)
		countResponse = append(countResponse, countObject)
		return countResponse
	}

	if len(tbd) > 0 {
		curode.Data.Writers[collection].Lock()
		for i := len(tbd) - 1; i >= 0; i-- {
			copy(curode.Data.Map[collection][tbd[i]:], curode.Data.Map[collection][tbd[i]+1:])
			curode.Data.Map[collection][len(curode.Data.Map[collection])-1] = nil
			curode.Data.Map[collection] = curode.Data.Map[collection][:len(curode.Data.Map[collection])-1]

			// if no entries in collection, remove it.
			if len(curode.Data.Map[collection]) == 0 {
				delete(curode.Data.Map, collection)
			}
		}
		curode.Data.Writers[collection].Unlock()
	}

	return objects
}

// Delete is the node data delete method
func (curode *Curode) Delete(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}, sortPos string, sortKey string) []interface{} {
	var deleted []interface{}
	for _, doc := range curode.Select(collection, ks, vs, vol, skip, oprs, lock, conditions, true, sortPos, sortKey, false) {
		deleted = append(deleted, doc)
	}

	return deleted
}

// Update is the node data update method
func (curode *Curode) Update(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}, uks []interface{}, nvs []interface{}, sortPos string, sortKey string) []interface{} {
	var updated []interface{}
	for i, doc := range curode.Select(collection, ks, vs, vol, skip, oprs, lock, conditions, false, sortPos, sortKey, false) {
		for m, _ := range uks {

			curode.Data.Writers[collection].Lock()
			ne := make(map[string]interface{})

			for kk, vv := range doc.(map[string]interface{}) {
				ne[kk] = vv
			}

			ne[uks[m].(string)] = nvs[m]

			curode.Data.Map[collection][i] = ne
			updated = append(updated, curode.Data.Map[collection][i])
			curode.Data.Writers[collection].Unlock()

		}
	}

	return updated
}
