package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpsertRegistryEntry(t *testing.T) {
	m := &Manager{}
	err := m.UpsertRegistryEntry("", "", RegistryEntry{})
	assert.Equal(t, ErrUsernameOrGlobalIDRequired, err)
	err = m.UpsertRegistryEntry("username", "globalid", RegistryEntry{})
	assert.Equal(t, ErrUsernameAndGlobalIDAreMutuallyExclusive, err)
}

func TestDeleteRegistryEntry(t *testing.T) {
	m := &Manager{}
	err := m.DeleteRegistryEntry("", "", "RegistryEntryKey")
	assert.Equal(t, ErrUsernameOrGlobalIDRequired, err)
	err = m.DeleteRegistryEntry("username", "globalid", "RegistryEntryKey")
	assert.Equal(t, ErrUsernameAndGlobalIDAreMutuallyExclusive, err)
}
