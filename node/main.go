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
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"net/textproto"
	"os"
	"os/signal"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Node is the main Node struct
type Node struct {
	Listener      net.Listener    // Tcp listener
	Wg            *sync.WaitGroup // Main node wait group
	SignalChannel chan os.Signal  // Signal channel
	Connections   []*Connection   // TCP connections struct slice
	Data          Data            // Node data
	Config        Config          // Node config
	MaxDocuments  uint64          // Default 15,000,000
}

// Config is the cluster config struct
type Config struct {
	TLSCert string `yaml:"tls-cert"`            // TLS cert path
	TLSKey  string `yaml:"tls-key"`             // TLS cert key
	TLS     bool   `default:"false" yaml:"tls"` // Use TLS?
	Port    int    `yaml:"port"`
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
	defer n.Wg.Done()
	var err error

	if n.Config.TLS {
		if n.Config.TLSCert == "" && n.Config.TLSKey == "" {
			log.Println("TCP_TLSListener():", "TLS cert and key missing.") // Log an error
			n.SignalChannel <- os.Interrupt                                // Send interrupt to signal channel
			return
		}

		cer, err := tls.LoadX509KeyPair(n.Config.TLSCert, n.Config.TLSKey)
		if err != nil {
			log.Println(err.Error())
			n.SignalChannel <- os.Interrupt
			return
		}

		config := &tls.Config{Certificates: []tls.Certificate{cer}}
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

func (n *Node) WriteToFile() {
	f, err := os.OpenFile(".cdat", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		log.Println("WriteToFile():", err.Error())
		n.SignalChannel <- os.Interrupt
		return
	}

	defer f.Close()

	e := gob.NewEncoder(f)

	// Encoding the map
	err = e.Encode(n.Data.Map)
	if err != nil {
		log.Println("WriteToFile():", err.Error())
		n.SignalChannel <- os.Interrupt
		return
	}

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

// SignalListener listen for system signal
func (n *Node) SignalListener() {
	defer n.Wg.Done()

	for {
		select {
		case sig := <-n.SignalChannel:
			log.Println("received", sig)
			log.Println("closing", len(n.Connections), "connections")
			for _, c := range n.Connections {
				c.Text.Close()
				c.Conn.Close()
			}

			if n.Listener != nil {
				n.Listener.Close()
			}

			n.WriteToFile()
			return
		default:
			time.Sleep(time.Millisecond * 125)
		}
	}
}

func main() {
	var node Node
	node.Data.Map = make(map[string][]map[string]interface{})
	node.Data.Writers = make(map[string]*sync.RWMutex)

	if _, err := os.Stat("./.curodeconfig"); errors.Is(err, os.ErrNotExist) {
		nodeConfigFile, err := os.OpenFile("./.curodeconfig", os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		defer nodeConfigFile.Close()

		node.Config.Port = 7682

		yamlData, err := yaml.Marshal(&node.Config)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		nodeConfigFile.Write(yamlData)
	} else {
		nodeConfigFile, err := os.ReadFile("./.curodeconfig")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = yaml.Unmarshal(nodeConfigFile, &node.Config)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

	}

	if _, err := os.Stat("./.cdat"); errors.Is(err, os.ErrNotExist) {
		fmt.Println("New previous data to read.")
	} else {
		fmt.Println("Node data read into memory.")
		dataFile, err := os.Open("./.cdat")
		d := gob.NewDecoder(dataFile)

		err = d.Decode(&node.Data.Map)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}

	flag.IntVar(&node.Config.Port, "port", node.Config.Port, "port for node")
	flag.Parse()

	node.SignalChannel = make(chan os.Signal, 1)

	signal.Notify(node.SignalChannel, syscall.SIGINT, syscall.SIGTERM)
	node.Wg = &sync.WaitGroup{}

	node.Wg.Add(1)
	go node.SignalListener()

	node.Wg.Add(1)
	go node.TCP_TLSListener()

	node.Wg.Wait()

}
