package gocb

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

type TestConfig struct {
	ConnStr       string `json:"ConnStr"`
	DefaultBucket string `json:"DefaultBucket"`
	SASLBucket    string `json:"SASLBucket"`
	SASLPasswd    string `json:"SASLPasswd"`
}

var bucket *Bucket

func TestMain(m *testing.M) {
	file, err := ioutil.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Unable to read test config file %v", err)
	}

	var config TestConfig
	err = json.Unmarshal(file, &config)
	if err != nil {
		log.Fatalf("Unable to unmarshal config %v", err)
	}

	c, err := Connect(config.ConnStr)
	if err != nil {
		log.Fatalf("Unable to connect %v", err)
	}

	bucket, err = c.OpenBucket(config.DefaultBucket, "")
	if err != nil {
		log.Fatalf("Unable to open bucket %v", err)
	}

	exitStatus := m.Run()
	if exitStatus != 0 {
		os.Exit(exitStatus)
	}

	bucket, err = c.OpenBucket(config.SASLBucket, config.SASLPasswd)
	os.Exit(m.Run())
}

func TestSimpleSet(t *testing.T) {
	cas, err := bucket.Upsert("testSimpleKey", "testSimpleVal", 0)
	if err != nil {
		t.Errorf("Failed to upsert %v", err)
	}
	if cas == 0 {
		t.Errorf("Incorrect cas")
	}
}

func TestRemove(t *testing.T) {
	key := "testSimpleKey"
	val := "testSimpleVal"

	cas, err := bucket.Upsert(key, val, 0)
	if err != nil {
		t.Errorf("Failed to upsert %v", err)
	}
	cas, err = bucket.Remove(key, cas)
	if err != nil {
		t.Errorf("Failed to remove key %v", err)
	}
}

func TestRemoveMiss(t *testing.T) {
	key := "testSimpleKey"
	val := "testSimpleVal"

	_, err := bucket.Upsert(key, val, 0)
	if err != nil {
		t.Errorf("Failed to upsert %v", err)
	}
	_, err = bucket.Remove(key, 1)
	if err == nil {
		t.Errorf("Removed key with incorrect cas %v", err)
	}
}

func TestSimpleAppend(t *testing.T) {
	key := "testSimpleKey"
	val := "testSimpleVal"

	cas, err := bucket.Upsert(key, val, 0)
	if err != nil {
		t.Errorf("Failed to upsert %v", err)
	}
	newcas, err := bucket.Append(key, "append")
	if err != nil {
		t.Errorf("Failed to append %v", err)
	}
	if newcas == cas {
		t.Errorf("Cas not changed after append")
	}
}

func TestSimplePrepend(t *testing.T) {
	key := "testSimpleKey"
	val := "testSimpleVal"

	cas, err := bucket.Upsert(key, val, 0)
	if err != nil {
		t.Errorf("Failed to upsert %v", err)
	}
	newcas, err := bucket.Prepend(key, "prepend")
	if err != nil {
		t.Errorf("Failed to append %v", err)
	}
	if newcas == cas {
		t.Errorf("Cas not changed after append")
	}
}

func TestSimpleReplace(t *testing.T) {
	key := "testSimpleKey"
	val := "testSimpleVal"

	cas, err := bucket.Upsert(key, val, 0)
	if err != nil {
		t.Errorf("Failed to upsert %v", err)
	}
	newcas, err := bucket.Replace(key, "newVal", cas, 0)
	if err != nil {
		t.Errorf("Failed to replace %v", err)
	}
	if newcas == cas {
		t.Errorf("Cas not changed after replace")
	}
}

func TestIncorrectCasReplace(t *testing.T) {
	key := "testSimpleKey"
	val := "testSimpleVal"

	_, err := bucket.Upsert(key, val, 0)
	if err != nil {
		t.Errorf("Failed to upsert %v", err)
	}
	_, err = bucket.Replace(key, "newVal", 1, 0)
	if err == nil {
		t.Errorf("Incorrectly replaced on wrong cas")
	}
}
