package lake

import (
	"fmt"
	"testing"

	uuid "github.com/satori/go.uuid"
)

func TestAppend(t *testing.T) {
	l, err := New("postgres://klafi:@localhost/lake?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal(err)
	}

	ns := "test123"

	err = l.Append(Record{
		UID:       id.String(),
		Blob:      []byte("[1,2,3,4]"),
		Metadata:  map[string]string{"creator": "KL"},
		Namespace: ns,
	})

	if err != nil {
		t.Fatal(err)
	}

	records, err := l.GetRecords(ns)
	if err != nil {
		t.Fatal(err)
	}

	if len(records) < 0 {
		t.Fatal("number of records must more than 1")
	}

	fmt.Println(records)
}
