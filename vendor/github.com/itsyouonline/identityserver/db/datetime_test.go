package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestDateTime(t *testing.T) {
	now := time.Now()
	dt := DateTime(now)
	convertedNow := time.Time(dt)
	assert.Equal(t, now, convertedNow)
}

type testMarshalDateTimeType struct {
	MyDateTime DateTime
}

func TestDateTimeBson(t *testing.T) {
	now := bson.Now()

	sut := testMarshalDateTimeType{
		MyDateTime: DateTime(now),
	}
	serialized, err := bson.Marshal(&sut)
	assert.NoError(t, err)

	deserialized := testMarshalDateTimeType{}
	err = bson.Unmarshal(serialized, &deserialized)
	assert.NoError(t, err)

	assert.Equal(t, sut, deserialized)
}
