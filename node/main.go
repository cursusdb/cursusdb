/*
* CursusDB
* Node
* ******************************************************************
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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

// Node is the main Node struct
type Node struct {
	Listener      net.Listener    // Tcp listener
	Wg            *sync.WaitGroup // Main node wait group
	SignalChannel chan os.Signal  // Signal channel
	Connections   []*Connection   // TCP connections struct slice
	Data          Data            // Node data
	Config        Config          // Node config
}

// Config is the cluster config struct
type Config struct {
	TLSCert   string `yaml:"tls-cert"`            // TLS cert path
	TLSKey    string `yaml:"tls-key"`             // TLS cert key
	TLS       bool   `default:"false" yaml:"tls"` // Use TLS?
	Port      int    `yaml:"port"`
	Key       string `yaml:"key"`        // Key for a cluster to communicate with the node and also used to resting data.
	MaxMemory uint64 `yaml:"max-memory"` // Default 10240MB = 10 GB (1024 * 10)
}

// Data is the node data struct
type Data struct {
	Map     map[string][]map[string]interface{} // Data hash map
	Writers map[string]*sync.RWMutex            // Collection writers
}

// Connection is a node tcp connection struct
type Connection struct {
	Conn net.Conn
	Text *textproto.Conn // Connection writer and reader
}

// TCP_TLSListener start listening on provided port on tls or regular tcp
func (n *Node) TCP_TLSListener() {
	defer n.Wg.Done() // defer go routine complete
	var err error     // local to function error variable

	if n.Config.TLS { // if node is set to use TLS

		if n.Config.TLSCert == "" || n.Config.TLSKey == "" { // Check if TLS cert or key is missing
			log.Println("TCP_TLSListener():", "TLS cert and key missing.") // Log an error
			n.SignalChannel <- os.Interrupt                                // Send interrupt to signal channel
			return
		}

		// Load key pair
		cer, err := tls.LoadX509KeyPair(n.Config.TLSCert, n.Config.TLSKey)
		if err != nil {
			log.Println(err.Error())
			n.SignalChannel <- os.Interrupt
			return
		}

		config := &tls.Config{Certificates: []tls.Certificate{cer}} // Set config for tls listener

		n.Listener, err = tls.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", n.Config.Port), config)
		if err != nil {
			log.Println(err.Error())
			n.SignalChannel <- os.Interrupt
			return
		}
	} else {
		n.Listener, err = net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", n.Config.Port))
		if err != nil {
			log.Println(err.Error())
			n.SignalChannel <- os.Interrupt
			return
		}
	}

	// Start handling cluster connections
	for {
		conn, err := n.Listener.Accept()
		if err != nil {
			return
		}

		n.Wg.Add(1)
		go n.HandleConnection(&Connection{
			Conn: conn,
		})
	}

}

// CurrentMemoryUsage returns current memory usage in mb
func (n *Node) CurrentMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.Alloc / 1024 / 1024
}

// encrypt encrypts a temporary serialized .cdat serialized file with chacha
func (n *Node) encrypt(key, plaintext []byte) ([]byte, error) {
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

// decrypt decrypts .cdat file to temporary serialized data file to be read
func (n *Node) decrypt(key, ciphertext []byte) ([]byte, error) {
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

// WriteToFile will write the current node data to a .cdat file encrypted with your node key.
func (n *Node) WriteToFile() {

	// Create temporary .cdat which is all serialized data.  An encryption is performed after the fact to not consume memory.
	fTmp, err := os.OpenFile(".cdat.tmp", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		log.Println("WriteToFile():", err.Error())
		n.SignalChannel <- os.Interrupt
		return
	}

	e := gob.NewEncoder(fTmp)

	// Encoding the map
	err = e.Encode(n.Data.Map)
	if err != nil {
		log.Println("WriteToFile():", err.Error())
		n.SignalChannel <- os.Interrupt
		return
	}

	fTmp.Close()

	// After serialization encrypt temp data file
	fTmp, err = os.OpenFile(".cdat.tmp", os.O_RDONLY, 0777)
	if err != nil {
		log.Println("WriteToFile():", err.Error())
		n.SignalChannel <- os.Interrupt
		return
	}

	//
	reader := bufio.NewReader(fTmp)
	buf := make([]byte, 1024)
	f, err := os.OpenFile(".cdat", os.O_TRUNC|os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		log.Println("WriteToFile():", err.Error())
		n.SignalChannel <- os.Interrupt
		return
	}
	defer f.Close()

	for {
		read, err := reader.Read(buf)

		if err != nil {
			if err != io.EOF {
				log.Println("WriteToFile():", err.Error())
				n.SignalChannel <- os.Interrupt
				return
			}
			break
		}

		if read > 0 {
			decodedKey, err := base64.StdEncoding.DecodeString(n.Config.Key)
			if err != nil {
				log.Println("WriteToFile():", err.Error())
				n.SignalChannel <- os.Interrupt
				return
			}

			ciphertext, err := n.encrypt(decodedKey[:], buf[:read])
			if err != nil {
				log.Println("WriteToFile():", err.Error())
				n.SignalChannel <- os.Interrupt
				return
			}

			f.Write(ciphertext)
		}
	}

	os.Remove(".cdat.tmp")

	log.Println("WriteToFile(): Completed.")
}

// update is a function to update the nodes data map
func (n *Node) update(collection string, ks []interface{}, vs []interface{}, uks []interface{}, nvs []interface{}, vol int, skip int, oprs []interface{}, conditions []interface{}) []interface{} {

	var objects []*map[string]interface{}

	var conditionsMet uint64
	//The && operator updates documents if all the conditions are TRUE.
	//The || operator updates documents if any of the conditions are TRUE.

	for i, d := range n.Data.Map[collection] {
		if ks == nil && vs == nil && oprs == nil {
			if skip != 0 {
				skip = skip - 1
				continue
			}

			if vol != -1 {
				if i-1 == vol-1 {
					break
				}
			}

			objects = append(objects, &n.Data.Map[collection][i])
			continue
		} else {

			for m, k := range ks {

				if oprs[m] == "" {
					fmt.Sprintf("Query operator required.")
					return nil
				}

				if skip != 0 {
					skip = skip - 1
					continue
				}

				if vol != -1 {
					if len(objects) == vol {
						break
					}
				}

				vType := fmt.Sprintf("%T", vs[m])

				_, ok := d[k.(string)]
				if ok {

					if d[k.(string)] == nil {
						objects = append(objects, &n.Data.Map[collection][i])
						continue
					}

					if reflect.TypeOf(d[k.(string)]).Kind() == reflect.Slice {
						for _, dd := range d[k.(string)].([]interface{}) {

							if len(objects) == vol {
								//return objects
								break
							}

							if reflect.TypeOf(dd).Kind() == reflect.Float64 {
								if vType == "int" {
									var interfaceI int = int(dd.(float64))

									if oprs[m] == "==" {
										if reflect.DeepEqual(interfaceI, vs[m]) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &n.Data.Map[collection][i])
											exists:
											})()
										}
									} else if oprs[m] == "!=" {
										if !reflect.DeepEqual(interfaceI, vs[m]) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &n.Data.Map[collection][i])
											exists:
											})()
										}
									} else if oprs[m] == ">" {
										if vType == "int" {
											if interfaceI > vs[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, &n.Data.Map[collection][i])
												exists:
												})()
											}
										}
									} else if oprs[m] == "<" {
										if vType == "int" {
											if interfaceI < vs[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, &n.Data.Map[collection][i])
												exists:
												})()
											}
										}
									} else if oprs[m] == ">=" {
										if vType == "int" {
											if interfaceI >= vs[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, &n.Data.Map[collection][i])
												exists:
												})()
											}
										}
									} else if oprs[m] == "<=" {
										if vType == "int" {
											if interfaceI <= vs[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, &n.Data.Map[collection][i])
												exists:
												})()
											}
										}
									}
								} else if vType == "float64" {
									var interfaceI float64 = dd.(float64)

									if oprs[m] == "==" {

										if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs[m].(float64))))) {

											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &n.Data.Map[collection][i])
											exists:
											})()
										}
									} else if oprs[m] == "!=" {
										if float64(interfaceI) != vs[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &d)
											exists:
											})()
										}
									} else if oprs[m] == ">" {
										if float64(interfaceI) > vs[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &n.Data.Map[collection][i])
											exists:
											})()
										}

									} else if oprs[m] == "<" {
										if float64(interfaceI) < vs[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &n.Data.Map[collection][i])
											exists:
											})()
										}

									} else if oprs[m] == ">=" {

										if float64(interfaceI) >= vs[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &n.Data.Map[collection][i])
											exists:
											})()
										}

									} else if oprs[m] == "<=" {
										if float64(interfaceI) <= vs[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, &n.Data.Map[collection][i])
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
								if oprs[m] == "==" {
									if reflect.DeepEqual(dd, vs[m]) {
										conditionsMet += 1
										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											objects = append(objects, &n.Data.Map[collection][i])
										exists:
										})()
									}
								} else if oprs[m] == "!=" {
									if !reflect.DeepEqual(dd, vs[m]) {
										conditionsMet += 1
										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											objects = append(objects, &n.Data.Map[collection][i])
										exists:
										})()
									}
								}
							}

						}
					} else if vType == "int" {
						var interfaceI int = int(d[k.(string)].(float64))

						if oprs[m] == "==" {
							if reflect.DeepEqual(interfaceI, vs[m]) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}
						} else if oprs[m] == "!=" {
							if !reflect.DeepEqual(interfaceI, vs[m]) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}
						} else if oprs[m] == ">" {
							if vType == "int" {
								if interfaceI > vs[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, &n.Data.Map[collection][i])
									exists:
									})()
								}
							}
						} else if oprs[m] == "<" {
							if vType == "int" {
								if interfaceI < vs[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, &n.Data.Map[collection][i])
									exists:
									})()
								}
							}
						} else if oprs[m] == ">=" {
							if vType == "int" {
								if interfaceI >= vs[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, &n.Data.Map[collection][i])
									exists:
									})()
								}
							}
						} else if oprs[m] == "<=" {
							if vType == "int" {
								if interfaceI <= vs[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, &n.Data.Map[collection][i])
									exists:
									})()
								}
							}
						}
					} else if vType == "float64" {
						var interfaceI float64 = d[k.(string)].(float64)

						if oprs[m] == "==" {

							if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs[m].(float64))))) {

								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}
						} else if oprs[m] == "!=" {
							if float64(interfaceI) != vs[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}
						} else if oprs[m] == ">" {
							if float64(interfaceI) > vs[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}

						} else if oprs[m] == "<" {
							if float64(interfaceI) < vs[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}

						} else if oprs[m] == ">=" {

							if float64(interfaceI) >= vs[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}

						} else if oprs[m] == "<=" {
							if float64(interfaceI) <= vs[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}

						}
					} else {
						if oprs[m] == "==" {
							if reflect.DeepEqual(d[k.(string)], vs[m]) {
								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()

							}
						} else if oprs[m] == "!=" {
							if !reflect.DeepEqual(d[k.(string)], vs[m]) {
								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, &n.Data.Map[collection][i])
								exists:
								})()
							}
						}

					}
				}
			}
		}

	}

	var updated []interface{}

	if slices.Contains(conditions, "&&") {
		var nullObjects []interface{}

		if uint64(len(conditions)) != conditionsMet {

			if !slices.Contains(conditions, "||") {
				return nullObjects
			} else if conditionsMet > 0 {
				for _, d := range objects {
					for m, _ := range uks {

						n.Data.Writers[collection].Lock()
						ne := make(map[string]interface{})

						for kk, vv := range *d {
							ne[kk] = vv
						}

						ne[uks[m].(string)] = nvs[m]

						*d = ne
						updated = append(updated, *d)
						n.Data.Writers[collection].Unlock()

					}

				}
			}
		}

	} else if conditionsMet > 0 {
		for _, d := range objects {
			for m, _ := range uks {

				n.Data.Writers[collection].Lock()
				ne := make(map[string]interface{})

				for kk, vv := range *d {
					ne[kk] = vv
				}

				ne[uks[m].(string)] = nvs[m]

				*d = ne
				updated = append(updated, *d)
				n.Data.Writers[collection].Unlock()

			}

		}
	}

	return updated
}

// del delete from node map
func (n *Node) del(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}) []interface{} {

	var objects []uint64

	var conditionsMet uint64

	for i, d := range n.Data.Map[collection] {
		if ks == nil && vs == nil && oprs == nil {
			if skip != 0 {
				skip = skip - 1
				continue
			}

			if vol != -1 {
				if i-1 == vol-1 {
					break
				}
			}
			objects = append(objects, uint64(i))

			continue
		} else {

			for m, k := range ks.([]interface{}) {

				if oprs.([]interface{})[m] == "" {
					fmt.Sprintf("Query operator required.")
					return nil
				}

				if skip != 0 {
					skip = skip - 1
					continue
				}

				if vol != -1 {
					if len(objects) == vol {
						break
					}
				}

				vType := fmt.Sprintf("%T", vs.([]interface{})[m])

				_, ok := d[k.(string)]
				if ok {

					if d[k.(string)] == nil {
						objects = append(objects, uint64(i))
						continue
					}

					if reflect.TypeOf(d[k.(string)]).Kind() == reflect.Slice {
						for _, dd := range d[k.(string)].([]interface{}) {

							if len(objects) == vol {
								break
							}

							if reflect.TypeOf(dd).Kind() == reflect.Float64 {
								if vType == "int" {
									var interfaceI int = int(dd.(float64))

									if oprs.([]interface{})[m] == "==" {
										if reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == "!=" {
										if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == ">" {
										if vType == "int" {
											if interfaceI > vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, uint64(i))

												exists:
												})()
											}
										}
									} else if oprs.([]interface{})[m] == "<" {
										if vType == "int" {
											if interfaceI < vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, uint64(i))

												exists:
												})()
											}
										}
									} else if oprs.([]interface{})[m] == ">=" {
										if vType == "int" {
											if interfaceI >= vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, uint64(i))

												exists:
												})()
											}
										}
									} else if oprs.([]interface{})[m] == "<=" {
										if vType == "int" {
											if interfaceI <= vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, uint64(i))

												exists:
												})()
											}
										}
									}
								} else if vType == "float64" {
									var interfaceI float64 = dd.(float64)

									if oprs.([]interface{})[m] == "==" {

										if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == "!=" {
										if float64(interfaceI) != vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == ">" {
										if float64(interfaceI) > vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

											exists:
											})()
										}

									} else if oprs.([]interface{})[m] == "<" {
										if float64(interfaceI) < vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

											exists:
											})()
										}

									} else if oprs.([]interface{})[m] == ">=" {

										if float64(interfaceI) >= vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

											exists:
											})()
										}

									} else if oprs.([]interface{})[m] == "<=" {
										if float64(interfaceI) <= vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, uint64(i))

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
								if oprs.([]interface{})[m] == "==" {
									if reflect.DeepEqual(dd, vs.([]interface{})[m]) {
										conditionsMet += 1
										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											objects = append(objects, uint64(i))

										exists:
										})()
									}
								} else if oprs.([]interface{})[m] == "!=" {
									if !reflect.DeepEqual(dd, vs.([]interface{})[m]) {
										conditionsMet += 1
										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											objects = append(objects, uint64(i))

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
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == "!=" {
							if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == ">" {
							if vType == "int" {
								if interfaceI > vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, uint64(i))

									exists:
									})()
								}
							}
						} else if oprs.([]interface{})[m] == "<" {
							if vType == "int" {
								if interfaceI < vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, uint64(i))

									exists:
									})()
								}
							}
						} else if oprs.([]interface{})[m] == ">=" {
							if vType == "int" {
								if interfaceI >= vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, uint64(i))

									exists:
									})()
								}
							}
						} else if oprs.([]interface{})[m] == "<=" {
							if vType == "int" {
								if interfaceI <= vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, uint64(i))

									exists:
									})()
								}
							}
						}
					} else if vType == "float64" {
						var interfaceI float64 = d[k.(string)].(float64)

						if oprs.([]interface{})[m] == "==" {

							if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == "!=" {
							if float64(interfaceI) != vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == ">" {
							if float64(interfaceI) > vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}

						} else if oprs.([]interface{})[m] == "<" {
							if float64(interfaceI) < vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}

						} else if oprs.([]interface{})[m] == ">=" {

							if float64(interfaceI) >= vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}

						} else if oprs.([]interface{})[m] == "<=" {
							if float64(interfaceI) <= vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))

								exists:
								})()
							}

						}
					} else {
						if oprs.([]interface{})[m] == "==" {
							if reflect.DeepEqual(d[k.(string)], vs.([]interface{})[m]) {
								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))
								exists:
								})()

							}
						} else if oprs.([]interface{})[m] == "!=" {
							if !reflect.DeepEqual(d[k.(string)], vs.([]interface{})[m]) {
								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, uint64(i))
								exists:
								})()
							}
						}

					}
				}
			}
		}

	}

	var deleted []interface{}

	if slices.Contains(conditions, "&&") {
		var nullObjects []interface{}

		if uint64(len(conditions)) != conditionsMet {

			if !slices.Contains(conditions, "||") {
				return nullObjects
			} else if conditionsMet > 0 {
				for _, i := range objects {
					if i < uint64(len(n.Data.Map[collection])) {
						deleted = append(deleted, n.Data.Map[collection][i])
						n.Data.Writers[collection].Lock()
						n.Data.Map[collection][i] = n.Data.Map[collection][len(n.Data.Map[collection])-1]
						n.Data.Map[collection][len(n.Data.Map[collection])-1] = nil
						n.Data.Map[collection] = n.Data.Map[collection][:len(n.Data.Map[collection])-1]
						n.Data.Writers[collection].Unlock()
					}
				}
			}
		}

	} else if conditionsMet > 0 {
		for _, i := range objects {
			if i < uint64(len(n.Data.Map[collection])) {
				deleted = append(deleted, n.Data.Map[collection][i])
				n.Data.Writers[collection].Lock()
				n.Data.Map[collection][i] = n.Data.Map[collection][len(n.Data.Map[collection])-1]
				n.Data.Map[collection][len(n.Data.Map[collection])-1] = nil
				n.Data.Map[collection] = n.Data.Map[collection][:len(n.Data.Map[collection])-1]
				n.Data.Writers[collection].Unlock()
			}
		}
	}

	return deleted
}

func (n *Node) sel(collection string, ks interface{}, vs interface{}, vol int, skip int, oprs interface{}, lock bool, conditions []interface{}) []interface{} {
	if lock {
		l, ok := n.Data.Writers[collection]
		if ok {
			l.Lock()
		}

	}

	defer func() {
		if lock {
			l, ok := n.Data.Writers[collection]
			if ok {
				l.Unlock()
			}
		}
	}()

	var objects []interface{}

	var conditionsMet uint64
	//The && operator displays a document if all the conditions are TRUE.
	//The || operator displays a record if any of the conditions are TRUE.

	for i, d := range n.Data.Map[collection] {
		if ks == nil && vs == nil && oprs == nil {
			if skip != 0 {
				skip = skip - 1
				continue
			}

			if vol != -1 {
				if i-1 == vol-1 {
					return objects
				}
			}

			objects = append(objects, d)
			continue
		} else {

			for m, k := range ks.([]interface{}) {

				if oprs.([]interface{})[m] == "" {
					fmt.Sprintf("Query operator required.")
					return nil
				}

				if skip != 0 {
					skip = skip - 1
					continue
				}

				if vol != -1 {
					if len(objects) == vol {
						return objects
					}
				}

				vType := fmt.Sprintf("%T", vs.([]interface{})[m])

				_, ok := d[k.(string)]
				if ok {

					if d[k.(string)] == nil {
						objects = append(objects, d)
						continue
					}

					if reflect.TypeOf(d[k.(string)]).Kind() == reflect.Slice {
						for _, dd := range d[k.(string)].([]interface{}) {

							if len(objects) == vol {
								return objects
							}

							if reflect.TypeOf(dd).Kind() == reflect.Float64 {
								if vType == "int" {
									var interfaceI int = int(dd.(float64))

									if oprs.([]interface{})[m] == "==" {
										if reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == "!=" {
										if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == ">" {
										if vType == "int" {
											if interfaceI > vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, d)
												exists:
												})()
											}
										}
									} else if oprs.([]interface{})[m] == "<" {
										if vType == "int" {
											if interfaceI < vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, d)
												exists:
												})()
											}
										}
									} else if oprs.([]interface{})[m] == ">=" {
										if vType == "int" {
											if interfaceI >= vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, d)
												exists:
												})()
											}
										}
									} else if oprs.([]interface{})[m] == "<=" {
										if vType == "int" {
											if interfaceI <= vs.([]interface{})[m].(int) {
												conditionsMet += 1
												(func() {
													for _, o := range objects {
														if reflect.DeepEqual(o, d) {
															goto exists
														}
													}
													objects = append(objects, d)
												exists:
												})()
											}
										}
									}
								} else if vType == "float64" {
									var interfaceI float64 = dd.(float64)

									if oprs.([]interface{})[m] == "==" {

										if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == "!=" {
										if float64(interfaceI) != vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
											exists:
											})()
										}
									} else if oprs.([]interface{})[m] == ">" {
										if float64(interfaceI) > vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
											exists:
											})()
										}

									} else if oprs.([]interface{})[m] == "<" {
										if float64(interfaceI) < vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
											exists:
											})()
										}

									} else if oprs.([]interface{})[m] == ">=" {

										if float64(interfaceI) >= vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
											exists:
											})()
										}

									} else if oprs.([]interface{})[m] == "<=" {
										if float64(interfaceI) <= vs.([]interface{})[m].(float64) {
											conditionsMet += 1
											(func() {
												for _, o := range objects {
													if reflect.DeepEqual(o, d) {
														goto exists
													}
												}
												objects = append(objects, d)
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
								if oprs.([]interface{})[m] == "==" {
									if reflect.DeepEqual(dd, vs.([]interface{})[m]) {
										conditionsMet += 1
										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											objects = append(objects, d)
										exists:
										})()
									}
								} else if oprs.([]interface{})[m] == "!=" {
									if !reflect.DeepEqual(dd, vs.([]interface{})[m]) {
										conditionsMet += 1
										(func() {
											for _, o := range objects {
												if reflect.DeepEqual(o, d) {
													goto exists
												}
											}
											objects = append(objects, d)
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
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == "!=" {
							if !reflect.DeepEqual(interfaceI, vs.([]interface{})[m]) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == ">" {
							if vType == "int" {
								if interfaceI > vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, d)
									exists:
									})()
								}
							}
						} else if oprs.([]interface{})[m] == "<" {
							if vType == "int" {
								if interfaceI < vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, d)
									exists:
									})()
								}
							}
						} else if oprs.([]interface{})[m] == ">=" {
							if vType == "int" {
								if interfaceI >= vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, d)
									exists:
									})()
								}
							}
						} else if oprs.([]interface{})[m] == "<=" {
							if vType == "int" {
								if interfaceI <= vs.([]interface{})[m].(int) {
									conditionsMet += 1
									(func() {
										for _, o := range objects {
											if reflect.DeepEqual(o, d) {
												goto exists
											}
										}
										objects = append(objects, d)
									exists:
									})()
								}
							}
						}
					} else if vType == "float64" {
						var interfaceI float64 = d[k.(string)].(float64)

						if oprs.([]interface{})[m] == "==" {

							if bytes.Equal([]byte(fmt.Sprintf("%f", float64(interfaceI))), []byte(fmt.Sprintf("%f", float64(vs.([]interface{})[m].(float64))))) {

								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == "!=" {
							if float64(interfaceI) != vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}
						} else if oprs.([]interface{})[m] == ">" {
							if float64(interfaceI) > vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}

						} else if oprs.([]interface{})[m] == "<" {
							if float64(interfaceI) < vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}

						} else if oprs.([]interface{})[m] == ">=" {

							if float64(interfaceI) >= vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}

						} else if oprs.([]interface{})[m] == "<=" {
							if float64(interfaceI) <= vs.([]interface{})[m].(float64) {
								conditionsMet += 1
								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}

						}
					} else {
						if oprs.([]interface{})[m] == "==" {
							if reflect.DeepEqual(d[k.(string)], vs.([]interface{})[m]) {
								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()

							}
						} else if oprs.([]interface{})[m] == "!=" {
							if !reflect.DeepEqual(d[k.(string)], vs.([]interface{})[m]) {
								conditionsMet += 1

								(func() {
									for _, o := range objects {
										if reflect.DeepEqual(o, d) {
											goto exists
										}
									}
									objects = append(objects, d)
								exists:
								})()
							}
						}

					}
				}
			}
		}

	}

	if slices.Contains(conditions, "&&") {
		if uint64(len(conditions)) != conditionsMet {
			var nullObjects []interface{}

			if !slices.Contains(conditions, "||") {
				objects = nullObjects
			}
		}
	}

	return objects
}

// insert into node map
func (n *Node) insert(collection string, jsonMap map[string]interface{}, connection *Connection) error {
	if n.CurrentMemoryUsage() >= n.Config.MaxMemory {
		return errors.New(fmt.Sprintf("%d node is at peak allocation", 100))
	}

	jsonStr, err := json.Marshal(jsonMap)
	if err != nil {
		return err
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
		return err
	}
	writeMu, ok := n.Data.Writers[collection]
	if ok {
		writeMu.Lock()
		defer writeMu.Unlock()

		n.Data.Map[collection] = append(n.Data.Map[collection], doc)
	} else {
		n.Data.Writers[collection] = &sync.RWMutex{}
		n.Data.Map[collection] = append(n.Data.Map[collection], doc)
	}

	response := make(map[string]interface{})
	response["statusCode"] = 2000
	response["message"] = "Document inserted"

	response["insert"] = doc

	responseMap, err := json.Marshal(response)
	if err != nil {
		return err
	}

	connection.Text.PrintfLine(string(responseMap))

	return nil
}

// HandleConnection handle an incoming tcp connection from a cluster
func (n *Node) HandleConnection(connection *Connection) {
	defer n.Wg.Done()
	n.Connections = append(n.Connections, connection)
	connection.Text = textproto.NewConn(connection.Conn)
	defer connection.Text.Close()
	defer connection.Conn.Close()

	defer func(conn *Connection) {
		for i, c := range n.Connections {
			if c == conn {
				n.Connections = append(n.Connections[:i], n.Connections[i+1:]...)
			}
		}

	}(connection)

	// Node expects an encoded base64 header Key: v the v being the shared cluster and node key

	read, err := connection.Text.ReadLine()
	if err != nil {
		return
	}

	if !strings.HasPrefix(read, "Key:") {
		connection.Text.PrintfLine("Invalid key.  Node expecting cluster key.")
		return
	} else {

		authSpl := strings.Split(read, "Key:")

		if len(authSpl) != 2 {
			connection.Text.PrintfLine("Invalid key.  Node expecting cluster key.")
			return
		}

		if n.Config.Key != strings.TrimSpace(authSpl[1]) {
			connection.Text.PrintfLine("Invalid key.")
			return
		}

		connection.Text.PrintfLine("0 Authentication successful.")

		scanner := bufio.NewScanner(connection.Conn)

		for scanner.Scan() {
			query := scanner.Text()

			result := make(map[string]interface{})

			err := json.Unmarshal([]byte(query), &result)
			if err != nil {
				result["statusCode"] = 4000
				result["message"] = "Unmarshalable JSON"
				r, _ := json.Marshal(result)
				connection.Text.PrintfLine(string(r))
				continue
			}

			result["skip"] = 0

			action, ok := result["action"]
			if ok {
				switch {
				case strings.EqualFold(action.(string), "delete"):

					if result["limit"].(string) == "*" {
						result["limit"] = -1
					} else if strings.Contains(result["limit"].(string), ",") {
						if len(strings.Split(result["limit"].(string), ",")) == 2 {
							result["skip"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[0])
							if err != nil {
								connection.Text.PrintfLine("Limit skip must be an integer. %s", err.Error())
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(result["limit"].(string), ",")[1], "*") {
								result["limit"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[1])
								if err != nil {
									connection.Text.PrintfLine("Something went wrong. %s", err.Error())
									query = ""
									continue
								}
							} else {
								result["limit"] = -1
							}
						} else {
							connection.Text.PrintfLine("Invalid limiting value.")
							query = ""
							continue
						}
					} else {
						result["limit"], err = strconv.Atoi(result["limit"].(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}
					}

					results := n.del(result["collection"].(string), result["keys"], result["values"], result["limit"].(int), result["skip"].(int), result["oprs"], result["lock"].(bool), result["conditions"].([]interface{}))
					r, _ := json.Marshal(results)
					result["statusCode"] = 2000

					if reflect.DeepEqual(results, nil) || len(results) == 0 {
						result["message"] = "No documents deleted."
					} else {
						result["message"] = fmt.Sprintf("%d Document(s) deleted successfully.", len(results))
					}

					delete(result, "document")
					delete(result, "collection")
					delete(result, "action")
					delete(result, "key")
					delete(result, "limit")
					delete(result, "opr")
					delete(result, "value")
					delete(result, "lock")
					delete(result, "new-values")
					delete(result, "update-keys")
					delete(result, "conditions")
					delete(result, "keys")
					delete(result, "oprs")
					delete(result, "values")
					delete(result, "skip")

					result["deleted"] = results

					r, _ = json.Marshal(result)
					connection.Text.PrintfLine(string(r))
					continue
				case strings.EqualFold(action.(string), "select"):

					if result["limit"].(string) == "*" {
						result["limit"] = -1
					} else if strings.Contains(result["limit"].(string), ",") {
						if len(strings.Split(result["limit"].(string), ",")) == 2 {
							result["skip"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[0])
							if err != nil {
								connection.Text.PrintfLine("Limit skip must be an integer. %s", err.Error())
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(result["limit"].(string), ",")[1], "*") {
								result["limit"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[1])
								if err != nil {
									connection.Text.PrintfLine("Something went wrong. %s", err.Error())
									query = ""
									continue
								}
							} else {
								result["limit"] = -1
							}
						} else {
							connection.Text.PrintfLine("Invalid limiting value.")
							query = ""
							continue
						}
					} else {
						result["limit"], err = strconv.Atoi(result["limit"].(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}
					}

					results := n.sel(result["collection"].(string), result["keys"], result["values"], result["limit"].(int), result["skip"].(int), result["oprs"], result["lock"].(bool), result["conditions"].([]interface{}))
					r, _ := json.Marshal(results)
					connection.Text.PrintfLine(string(r))
					continue
				case strings.EqualFold(action.(string), "update"):

					if result["limit"].(string) == "*" {
						result["limit"] = -1
					} else if strings.Contains(result["limit"].(string), ",") {
						if len(strings.Split(result["limit"].(string), ",")) == 2 {
							result["skip"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[0])
							if err != nil {
								connection.Text.PrintfLine("Limit skip must be an integer. %s", err.Error())
								query = ""
								continue
							}

							if !strings.EqualFold(strings.Split(result["limit"].(string), ",")[1], "*") {
								result["limit"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[1])
								if err != nil {
									connection.Text.PrintfLine("Something went wrong. %s", err.Error())
									query = ""
									continue
								}
							} else {
								result["limit"] = -1
							}
						} else {
							connection.Text.PrintfLine("Invalid limiting value.")
							query = ""
							continue
						}
					} else {
						result["limit"], err = strconv.Atoi(result["limit"].(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}
					}

					results := n.update(result["collection"].(string), result["keys"].([]interface{}), result["values"].([]interface{}), result["update-keys"].([]interface{}), result["new-values"].([]interface{}), result["limit"].(int), result["skip"].(int), result["oprs"].([]interface{}), result["conditions"].([]interface{}))
					r, _ := json.Marshal(results)

					delete(result, "document")
					delete(result, "collection")
					delete(result, "action")
					delete(result, "key")
					delete(result, "limit")
					delete(result, "opr")
					delete(result, "value")
					delete(result, "lock")
					delete(result, "new-values")
					delete(result, "update-keys")
					delete(result, "conditions")
					delete(result, "keys")
					delete(result, "oprs")
					delete(result, "values")
					delete(result, "skip")

					result["statusCode"] = 2000

					if reflect.DeepEqual(results, nil) || len(results) == 0 {
						result["message"] = "No documents updated."
					} else {
						result["message"] = fmt.Sprintf("%d Document(s) updated successfully.", len(results))
					}

					result["updated"] = results
					r, _ = json.Marshal(result)

					connection.Text.PrintfLine(string(r))
					continue
				case strings.EqualFold(action.(string), "insert"):

					collection := result["collection"]
					doc := result["document"]
					delete(result, "document")
					delete(result, "collection")
					delete(result, "action")
					delete(result, "skip")

					err := n.insert(collection.(string), doc.(map[string]interface{}), connection)
					if err != nil {
						// Only error returned is a 4003 which means cannot insert nested object
						result["statusCode"] = 4003
						result["message"] = err.Error()
						r, _ := json.Marshal(result)
						connection.Text.PrintfLine(string(r))
						continue
					}

					continue
				default:

					result["statusCode"] = 4002
					result["message"] = "Invalid/Non-existent action"
					r, _ := json.Marshal(result)

					connection.Text.PrintfLine(string(r))
					continue
				}
			} else {
				result["statusCode"] = 4001
				result["message"] = "Missing action"
				r, _ := json.Marshal(result)

				connection.Text.PrintfLine(string(r))
				continue
			}
		}
	}

}

// SignalListener listen for system signal
func (n *Node) SignalListener() {
	defer n.Wg.Done()

	// Wait for signal
	for {
		select {
		case sig := <-n.SignalChannel: // signal received, gracefully shutdown
			log.Println("received", sig)
			log.Println("closing", len(n.Connections), "connections")
			// Close all connections
			for _, c := range n.Connections {
				c.Text.Close()
				c.Conn.Close()
			}

			// Close listener
			if n.Listener != nil {
				n.Listener.Close()
			}

			// Write node data to file using serialization and encryption
			n.WriteToFile()
			return
		default:
			time.Sleep(time.Millisecond * 125)
		}
	}
}

// main is the starting point for the CursusDB node software
func main() {
	var node Node // Node type variable

	node.Data.Map = make(map[string][]map[string]interface{}) // Main hashmap
	node.Data.Writers = make(map[string]*sync.RWMutex)        // Read/Write mutexes per collection

	// Check if .curodeconfig exists
	if _, err := os.Stat("./.curodeconfig"); errors.Is(err, os.ErrNotExist) {

		// Create .curodeconfig
		nodeConfigFile, err := os.OpenFile("./.curodeconfig", os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		// Defer close node config
		defer nodeConfigFile.Close()

		node.Config.Port = 7682       // Set default CursusDB node port
		node.Config.MaxMemory = 10240 // Max memory 10GB default

		fmt.Println("Node key is required.  A node key is shared with your cluster and will encrypt all your data at rest and allow for only connections that contain a correct Key: header value matching the hashed key you provide.")
		fmt.Print("key> ")
		key, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		// Repear key with * so Alex would be ****
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(key))))
		fmt.Println("")

		// Hash and encode key
		hashedKey := sha256.Sum256(key)
		node.Config.Key = base64.StdEncoding.EncodeToString(append([]byte{}, hashedKey[:]...))

		// Marshal node config into yaml
		yamlData, err := yaml.Marshal(&node.Config)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		// Write to node config
		nodeConfigFile.Write(yamlData)
	} else {
		// Read node config
		nodeConfigFile, err := os.ReadFile("./.curodeconfig")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		// Unmarshal node config yaml
		err = yaml.Unmarshal(nodeConfigFile, &node.Config)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

	}

	// Read rested data from .cdat file
	if _, err := os.Stat("./.cdat"); errors.Is(err, os.ErrNotExist) { // Not exists we create it
		fmt.Println("No previous data to read.  Creating new .cdat file.")
	} else {
		fmt.Println("Node data read into memory.")
		dataFile, err := os.Open("./.cdat") // Open .cdat

		// Temporary decrypted data file.. to be unserialized into map
		fDFTmp, err := os.OpenFile(".cdat.tmp", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		// Read encrypted data file
		reader := bufio.NewReader(dataFile)
		buf := make([]byte, 1024)

		defer dataFile.Close()

		for {
			read, err := reader.Read(buf)

			if err != nil {
				if err != io.EOF {
					fmt.Println(err.Error())
					os.Exit(1)
				}
				break
			}

			if read > 0 {
				decodedKey, err := base64.StdEncoding.DecodeString(node.Config.Key)
				if err != nil {
					fmt.Println(err.Error())
					os.Exit(1)
					return
				}

				serialized, err := node.decrypt(decodedKey[:], buf[:read])
				if err != nil {
					fmt.Println(err.Error())
					os.Exit(1)
					return
				}

				fDFTmp.Write(serialized) // Decrypt serialized
			}
		}
		fDFTmp.Close()

		fDFTmp, err = os.OpenFile(".cdat.tmp", os.O_RDONLY, 0777)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		d := gob.NewDecoder(fDFTmp)

		// Now with all serialized data we encode into data hashmap
		err = d.Decode(&node.Data.Map)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		fDFTmp.Close()

		os.Remove(".cdat.tmp") // Remove temp
	}

	// Parse flags
	flag.IntVar(&node.Config.Port, "port", node.Config.Port, "port for node")
	flag.Parse()

	node.SignalChannel = make(chan os.Signal, 1) // Create signal channel

	signal.Notify(node.SignalChannel, syscall.SIGINT, syscall.SIGTERM)
	node.Wg = &sync.WaitGroup{} // Create wait group

	node.Wg.Add(1)
	go node.SignalListener() // Listen for signals to gracefully shutdown

	node.Wg.Add(1)
	go node.TCP_TLSListener() // Listen to tcp or tls cluster connections

	node.Wg.Wait() // Wait for all go routines

}
