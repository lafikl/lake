package lake

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

var (
	insertRecord = `INSERT INTO lake_records (uid, namespace, metadata, blob) VALUES ($1, $2, $3, $4)`
	getRecords   = `SELECT id, uid, namespace, metadata, blob FROM lake_records WHERE namespace=$1`
	readRecord   = `SELECT id, uid, namespace, metadata, blob FROM lake_records WHERE uid=$1`
)

var (
	// ErrEmptyUID returns
	ErrEmptyUID = errors.New("given UID is empty")
)

// Lake is a system for storing and retrieving records for future analysis and computations
// think of it like a checkpointing system for data.
// so we don't have to refetch data every time we want to use the data
type Lake struct {
	db *sql.DB
}

type Record struct {
	ID        int
	UID       string
	Namespace string
	Metadata  map[string]string
	Blob      []byte
}

// New instatiate an instance of Lake which is responsible for appending new records to the Append only log
func New(url string) (*Lake, error) {
	connStr := fmt.Sprintf("%s", url)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	l := &Lake{db: db}

	return l, nil
}

// Append adds a record to the database
func (l *Lake) Append(r Record) error {
	buf, err := json.Marshal(r.Metadata)
	if err != nil {
		return err
	}

	_, err = l.db.Exec(insertRecord, r.UID, r.Namespace, buf, r.Blob)
	if err != nil {
		return err
	}
	return nil
}

// GetRecords fetches records from the lake
func (l *Lake) GetRecords(namespace string) ([]Record, error) {
	rows, err := l.db.Query(getRecords, namespace)
	if err != nil {
		return nil, err
	}

	records := []Record{}
	for rows.Next() {
		rec := Record{}
		buf := []byte{}
		if err := rows.Scan(
			&rec.ID,
			&rec.UID,
			&rec.Namespace,
			&buf,
			&rec.Blob); err != nil {
			return records, err
		}

		// parse metadata bytearray
		kv := map[string]string{}
		err = json.Unmarshal(buf, &kv)
		if err != nil {
			return records, err
		}
		rec.Metadata = kv

		records = append(records, rec)

	}
	return records, nil
}

func (l *Lake) GetRecord(uid []byte) (Record, error) {
	rec := Record{}

	if len(uid) <= 0 {
		return rec, ErrEmptyUID
	}

	rows, err := l.db.Query(getRecords, uid)
	if err != nil {
		return rec, err
	}

	buf := []byte{}

	if err := rows.Scan(
		&rec.ID,
		&rec.UID,
		&rec.Namespace,
		&buf,
		&rec.Blob); err != nil {
		return rec, err
	}

	// parse metadata bytearray
	kv := map[string]string{}
	err = json.Unmarshal(buf, &kv)
	if err != nil {
		return rec, err
	}
	rec.Metadata = kv

	return rec, nil
}
