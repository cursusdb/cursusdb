/*
* CursusDB
* Cluster Node Tests
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
	"context"
	"crypto/tls"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestCurode_AddToQueryQueue(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		req map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if got := curode.AddToQueryQueue(tt.args.req); got != tt.want {
				t.Errorf("AddToQueryQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurode_AutomaticBackup(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.AutomaticBackup()
		})
	}
}

func TestCurode_ConnectToObservers(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.ConnectToObservers()
		})
	}
}

func TestCurode_CountLog(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		r io.Reader
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if got := curode.CountLog(tt.args.r); got != tt.want {
				t.Errorf("CountLog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurode_CurrentMemoryUsage(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if got := curode.CurrentMemoryUsage(); got != tt.want {
				t.Errorf("CurrentMemoryUsage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurode_Delete(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		collection string
		ks         interface{}
		vs         interface{}
		vol        int
		skip       int
		oprs       interface{}
		lock       bool
		conditions []interface{}
		sortPos    string
		sortKey    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []interface{}
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if got := curode.Delete(tt.args.collection, tt.args.ks, tt.args.vs, tt.args.vol, tt.args.skip, tt.args.oprs, tt.args.lock, tt.args.conditions, tt.args.sortPos, tt.args.sortKey); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurode_DeleteKeyFromColl(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		collection string
		key        string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if got := curode.DeleteKeyFromColl(tt.args.collection, tt.args.key); got != tt.want {
				t.Errorf("DeleteKeyFromColl() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurode_HandleClientConnection(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		conn net.Conn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.HandleClientConnection(tt.args.conn)
		})
	}
}

func TestCurode_Insert(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		collection string
		jsonMap    map[string]interface{}
		conn       net.Conn
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if err := curode.Insert(tt.args.collection, tt.args.jsonMap, tt.args.conn); (err != nil) != tt.wantErr {
				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCurode_LostReconnectObservers(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.LostReconnectObservers()
		})
	}
}

func TestCurode_Printl(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		data  string
		level string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.Printl(tt.args.data, tt.args.level)
		})
	}
}

func TestCurode_RemoveFromQueryQueue(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		id int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.RemoveFromQueryQueue(tt.args.id)
		})
	}
}

func TestCurode_RenewNodeConfig(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if err := curode.RenewNodeConfig(); (err != nil) != tt.wantErr {
				t.Errorf("RenewNodeConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCurode_Search(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		mu         *sync.RWMutex
		i          int
		tbd        *[]int
		collection string
		ks         interface{}
		vs         interface{}
		vol        int
		skip       int
		oprs       interface{}
		conditions []interface{}
		del        bool
		update     bool
		objs       *[]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.Search(tt.args.mu, tt.args.i, tt.args.tbd, tt.args.collection, tt.args.ks, tt.args.vs, tt.args.vol, tt.args.skip, tt.args.oprs, tt.args.conditions, tt.args.del, tt.args.update, tt.args.objs)
		})
	}
}

func TestCurode_Select(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		collection string
		ks         interface{}
		vs         interface{}
		vol        int
		skip       int
		oprs       interface{}
		lock       bool
		conditions []interface{}
		del        bool
		sortPos    string
		sortKey    string
		count      bool
		update     bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []interface{}
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if got := curode.Select(tt.args.collection, tt.args.ks, tt.args.vs, tt.args.vol, tt.args.skip, tt.args.oprs, tt.args.lock, tt.args.conditions, tt.args.del, tt.args.sortPos, tt.args.sortKey, tt.args.count, tt.args.update); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurode_SendToObservers(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		jsonStr string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.SendToObservers(tt.args.jsonStr)
		})
	}
}

func TestCurode_SetupInitializeCDat(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if err := curode.SetupInitializeCDat(); (err != nil) != tt.wantErr {
				t.Errorf("SetupInitializeCDat() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCurode_SetupNodeConfig(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if err := curode.SetupNodeConfig(); (err != nil) != tt.wantErr {
				t.Errorf("SetupNodeConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCurode_SignalListener(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.SignalListener()
		})
	}
}

func TestCurode_StartRunQueryQueue(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.StartRunQueryQueue()
		})
	}
}

func TestCurode_StartTCP_TLS(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.StartTCP_TLS()
		})
	}
}

func TestCurode_SyncOut(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.SyncOut()
		})
	}
}

func TestCurode_SyncOutQueryQueue(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.SyncOutQueryQueue()
		})
	}
}

func TestCurode_Update(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		collection string
		ks         interface{}
		vs         interface{}
		vol        int
		skip       int
		oprs       interface{}
		lock       bool
		conditions []interface{}
		uks        []interface{}
		nvs        []interface{}
		sortPos    string
		sortKey    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []interface{}
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			if got := curode.Update(tt.args.collection, tt.args.ks, tt.args.vs, tt.args.vol, tt.args.skip, tt.args.oprs, tt.args.lock, tt.args.conditions, tt.args.uks, tt.args.nvs, tt.args.sortPos, tt.args.sortKey); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCurode_WriteToFile(t *testing.T) {
	type fields struct {
		TCPAddr             *net.TCPAddr
		TCPListener         *net.TCPListener
		Wg                  *sync.WaitGroup
		SignalChannel       chan os.Signal
		Config              Config
		TLSConfig           *tls.Config
		ContextCancel       context.CancelFunc
		ConfigMu            *sync.RWMutex
		Data                *Data
		Context             context.Context
		LogMu               *sync.Mutex
		LogFile             *os.File
		ObserverConnections []*ObserverConnection
		QueryQueue          []map[string]interface{}
		QueryQueueMu        *sync.Mutex
	}
	type args struct {
		backup bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curode := &Curode{
				TCPAddr:             tt.fields.TCPAddr,
				TCPListener:         tt.fields.TCPListener,
				Wg:                  tt.fields.Wg,
				SignalChannel:       tt.fields.SignalChannel,
				Config:              tt.fields.Config,
				TLSConfig:           tt.fields.TLSConfig,
				ContextCancel:       tt.fields.ContextCancel,
				ConfigMu:            tt.fields.ConfigMu,
				Data:                tt.fields.Data,
				Context:             tt.fields.Context,
				LogMu:               tt.fields.LogMu,
				LogFile:             tt.fields.LogFile,
				ObserverConnections: tt.fields.ObserverConnections,
				QueryQueue:          tt.fields.QueryQueue,
				QueryQueueMu:        tt.fields.QueryQueueMu,
			}
			curode.WriteToFile(tt.args.backup)
		})
	}
}
