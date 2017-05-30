package storagecluster

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/g8os/blockstor/log"
	"github.com/stretchr/testify/assert"
)

const configContent = `
storageClusters:
  xTremX:
    dataStorage:
    - address: 10.107.3.19:2000
    - address: 10.107.3.19:2001
    - address: 10.107.3.19:2002
    - address: 10.107.3.19:2003
    - address: 10.107.3.19:2004
    - address: 10.107.3.19:2005
    - address: 10.107.3.19:2006
    - address: 10.107.3.13:2007
    - address: 10.107.3.13:2008
    - address: 10.107.3.13:2009
    - address: 10.107.3.13:2010
    - address: 10.107.3.13:2011
    - address: 10.107.3.13:2012
    - address: 10.107.3.13:2013
    - address: 10.107.3.11:2014
    - address: 10.107.3.11:2015
    - address: 10.107.3.11:2016
    - address: 10.107.3.11:2017
    - address: 10.107.3.11:2018
    - address: 10.107.3.11:2019
    - address: 10.107.3.11:2020
    - address: 10.107.3.12:2021
    - address: 10.107.3.12:2022
    - address: 10.107.3.12:2023
    - address: 10.107.3.12:2024
    - address: 10.107.3.12:2025
    - address: 10.107.3.12:2026
    - address: 10.107.3.12:2027
    - address: 10.107.3.17:2028
    - address: 10.107.3.17:2029
    - address: 10.107.3.17:2030
    - address: 10.107.3.17:2031
    - address: 10.107.3.17:2032
    - address: 10.107.3.17:2033
    - address: 10.107.3.17:2034
    - address: 10.107.3.18:2035
    - address: 10.107.3.18:2036
    - address: 10.107.3.18:2037
    - address: 10.107.3.18:2038
    - address: 10.107.3.18:2039
    - address: 10.107.3.18:2040
    - address: 10.107.3.18:2041
    - address: 10.107.3.14:2042
    - address: 10.107.3.14:2043
    - address: 10.107.3.14:2044
    - address: 10.107.3.14:2045
    - address: 10.107.3.14:2046
    - address: 10.107.3.14:2047
    - address: 10.107.3.14:2048
    - address: 10.107.3.15:2049
    - address: 10.107.3.15:2050
    - address: 10.107.3.15:2051
    - address: 10.107.3.15:2052
    - address: 10.107.3.15:2053
    - address: 10.107.3.15:2054
    - address: 10.107.3.15:2055
    - address: 10.107.3.16:2056
    - address: 10.107.3.16:2057
    - address: 10.107.3.16:2058
    - address: 10.107.3.16:2059
    - address: 10.107.3.16:2060
    - address: 10.107.3.16:2061
    - address: 10.107.3.16:2062
    metadataStorage:
      address: 10.107.3.16:2063
vdisks:
  testvdisk_14961271116507015:
    blockSize: 4096
    id: testvdisk_14961271116507015
    readOnly: false
    size: 10
    storageCluster: xTremX
    type: db
`

func TestNewClusterClient(t *testing.T) {
	//Create a temporary configfile
	configFile, _ := ioutil.TempFile("", "nbdconfig")
	configPath := configFile.Name()
	defer os.Remove(configPath)

	configFile.Write([]byte(configContent))
	configFile.Close()

	//Create and validate a ClusterClient
	cfg := ClusterClientConfig{ConfigPath: configPath, VdiskID: "testvdisk_14961271116507015", StorageClusterName: ""}
	cc, err := NewClusterClient(cfg, log.New("storagecluster", log.DebugLevel))
	assert.NoError(t, err)
	assert.Equal(t, int64(63), cc.numberOfServers, "Wrong number of servers loaded from the configfile")
}
