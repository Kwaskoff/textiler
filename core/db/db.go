package db

import (
	"crypto/rand"
	"strings"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipld-format"
	ulid "github.com/oklog/ulid/v2"
)
import (
	"bytes"
	"context"
	"encoding/binary"
	"expvar"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3/options"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/badger/v3/skl"
	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto"
	"github.com/dgraph-io/ristretto/z"
	humanize "github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

var (
	badgerPrefix = []byte("!badger!")       // Prefix for internal keys used by badger.
	txnKey       = []byte("!badger!txn")    // For indicating end of entries in txn.
	bannedNsKey  = []byte("!badger!banned") // For storing the banned namespaces.
)

const (
	maxNumSplits = 128
)

type closers struct {
	updateSize  *z.Closer
	compactors  *z.Closer
	memtable    *z.Closer
	writes      *z.Closer
	valueGC     *z.Closer
	pub         *z.Closer
	cacheHealth *z.Closer
}

type lockedKeys struct {
	sync.RWMutex
	keys map[uint64]struct{}
}

func (lk *lockedKeys) add(key uint64) {
	lk.Lock()
	defer lk.Unlock()
	lk.keys[key] = struct{}{}
}

func (lk *lockedKeys) has(key uint64) bool {
	lk.RLock()
	defer lk.RUnlock()
	_, ok := lk.keys[key]
	return ok
}

func (lk *lockedKeys) all() []uint64 {
	lk.RLock()
	defer lk.RUnlock()
	keys := make([]uint64, 0, len(lk.keys))
	for key := range lk.keys {
		keys = append(keys, key)
	}
	return keys
}
const (
	// EmptyInstanceID represents an empty InstanceID.
	EmptyInstanceID = InstanceID("")
)

// InstanceID is the type used in instance identities.
type InstanceID string

// NewInstanceID generates a new identity for an instance.
func NewInstanceID() InstanceID {
	id := ulid.MustNew(ulid.Now(), rand.Reader)
	return InstanceID(strings.ToLower(id.String()))
}

func (e InstanceID) String() string {
	return string(e)
}

// Event is a local or remote event generated in collection and dispatcher
// by Dispatcher.
type Event interface {
	// Time (wall-clock) the event was created.
	Time() []byte
	// InstanceID is the associated instance's unique identifier.
	InstanceID() InstanceID
	// Collection is the associated instance's collection name.
	Collection() string
	// Marshal the event to JSON.
	Marshal() ([]byte, error)
}

// ActionType is the type used by actions done in a txn.
type ActionType int

const (
	// Create indicates the creation of an instance in a txn.
	Create ActionType = iota
	// Save indicates the mutation of an instance in a txn.
	Save
	// Delete indicates the deletion of an instance by ID in a txn.
	Delete
)

// Action is a operation done in the collection.
type Action struct {
	// Type of the action.
	Type ActionType
	// InstanceID of the instance in action.
	InstanceID InstanceID
	// CollectionName of the instance in action.
	CollectionName string
	// Previous is the instance before the action.
	Previous []byte
	// Current is the instance after the action was done.
	Current []byte
}

type ReduceAction struct {
	// Type of the reduced action.
	Type ActionType
	// Collection in which action was made.
	Collection string
	// InstanceID of the instance in reduced action.
	InstanceID InstanceID
}

// IndexFunc handles index updates.
type IndexFunc func(collection string, key ds.Key, oldData, newData []byte, txn ds.Txn) error

// EventCodec transforms actions generated in collections to
// events dispatched to thread logs, and viceversa.
type EventCodec interface {
	// Reduce applies generated events into state.
	Reduce(events []Event, store ds.TxnDatastore, baseKey ds.Key, indexFunc IndexFunc) ([]ReduceAction, error)
	// Create corresponding events to be dispatched.
	Create(ops []Action) ([]Event, format.Node, error)
	// EventsFromBytes deserializes a format.Node bytes payload into Events.
	EventsFromBytes(data []byte) ([]Event, error)
}
