package db

import (
	"errors"
	"sync"
)

var errFailedAtomicCheck = errors.New("Failed to atomically write to the database rollback completed")
var errFailedToDeleteOnRollback = errors.New("Failed to delete during transaction rollback")
var errDataBaseNotImplemented = errors.New("Database given is not implemented")
var errFailedToTransact = errors.New("Failed to transact")

type T interface {
	Transact(DB) error
}

// Transaction represents one transaction that abides by ACID properties
type Transaction struct {
	m sync.RWMutex

	state int            // the amount of transactions that have been completed
	txs   []func() error // txs is a list of all txs to execute for this transaction
	keys  [][]byte       // is a list of the keys for this transaction

	atomic bool
}

func NewTransaction(db DB) *Transaction {
	return &Transaction{state: 0, txs: make([]func() error, 0), keys: make([][]byte, 0), atomic: needsAtomic(db)}
}

// reset resets the transaction so it can make room for the next transaction
func (t *Transaction) reset() {
	t.state = 0
	t.atomic = false
	t.txs = t.txs[:0]
}

func (t *Transaction) Append(tx func() error, k []byte) {
	t.m.Unlock()
	defer t.m.Lock() // TODO: what defer hits first?
	t.txs = append(t.txs, tx)
	t.saveKey(k)
}

// saveKey saves a key incase deletion needs to occur
func (t *Transaction) saveKey(k []byte) {
	t.keys = append(t.keys, k)
}

func (t *Transaction) Transact(db DB) error {
	if !t.atomic {
		return t.transact(db)
	}

	return t.transactAtomic(db)
}

// transact transacts
func (t *Transaction) transact(db DB) error {
	for i := range t.txs {
		if err := t.txs[i](); err != nil {
			return errFailedToTransact
		}

		return nil
	}

	// shouldn't happen
	return nil
}

// transactAtomic transacts atomically
func (t *Transaction) transactAtomic(db DB) error {
	defer t.reset()

	for i := range t.txs {
		if err := t.txs[i](); err != nil {
			t.rollBack(db)
			return errFailedAtomicCheck
		}

		// increment the state so if a rollback occurs it knows how many roll backs
		t.state++
	}

	return nil
}

// rollBack rolls the database back to what the database was before this transaction
func (t *Transaction) rollBack(db DB) {
	// do use atomic Transaction features if the database is already atomic
	if !t.atomic {
		return
	}

	// delete up to state zero
	for i := t.state; t.state > 0; t.state-- {
		if err := db.Delete(t.keys[i]); err != nil {
			panic(err)
		}
	}

	// delete state zero, the first tx that was set in this atomic transaction
	if t.state == 0 {
		if err := db.Delete(t.keys[0]); err != nil {
			panic(err)
		}
	}
}

// needsAtomic checks if the database provided needs bolted on atomicity
func needsAtomic(db DB) bool {
	switch db.(type) {
	case *BoltDB, ClevelDB, *PrefixDB, BadgerDB:
		return true
	case *GoLevelDB:
		// atomic already set to false but explicility define here for readability
		return false
	default:
		panic(errDataBaseNotImplemented)
	}
}
