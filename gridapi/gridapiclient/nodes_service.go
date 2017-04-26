package gridapiclient

import (
	"encoding/json"
	"net/http"
)

type NodesService service

// List Nodes
func (s *NodesService) ListNodes(headers, queryParams map[string]interface{}) ([]Node, *http.Response, error) {
	var u []Node

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get detailed information of a node
func (s *NodesService) GetNode(nodeid string, headers, queryParams map[string]interface{}) (Node, *http.Response, error) {
	var u Node

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// List bridges
func (s *NodesService) ListBridges(nodeid string, headers, queryParams map[string]interface{}) ([]Bridge, *http.Response, error) {
	var u []Bridge

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/bridges", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Creates a new bridge
func (s *NodesService) CreateBridge(nodeid string, body BridgeCreate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/bridges", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get bridge details
func (s *NodesService) GetBridge(bridgeid, nodeid string, headers, queryParams map[string]interface{}) (Bridge, *http.Response, error) {
	var u Bridge

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/bridges/"+bridgeid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Remove bridge
func (s *NodesService) DeleteBridge(bridgeid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/bridges/"+bridgeid, headers, queryParams)
}

// Create a new Container
func (s *NodesService) CreateContainer(nodeid string, body CreateContainer, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List running Containers
func (s *NodesService) ListContainers(nodeid string, headers, queryParams map[string]interface{}) ([]ContainerListItem, *http.Response, error) {
	var u []ContainerListItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete Container instance
func (s *NodesService) DeleteContainer(containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid, headers, queryParams)
}

// Get Container
func (s *NodesService) GetContainer(containerid, nodeid string, headers, queryParams map[string]interface{}) (Container, *http.Response, error) {
	var u Container

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Upload file to container
func (s *NodesService) FileUpload(containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/filesystem", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Download file from container
func (s *NodesService) FileDownload(containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/filesystem", headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Delete file from container
func (s *NodesService) FileDelete(containerid, nodeid string, body DeleteFile, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/filesystem", headers, queryParams)
}

// Get detailed information of the container os
func (s *NodesService) GetContainerOSInfo(containerid, nodeid string, headers, queryParams map[string]interface{}) (OSInfo, *http.Response, error) {
	var u OSInfo

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/info", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// List running jobs on the container
func (s *NodesService) ListContainerJobs(containerid, nodeid string, headers, queryParams map[string]interface{}) ([]JobListItem, *http.Response, error) {
	var u []JobListItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/jobs", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Kills all running jobs on the container
func (s *NodesService) KillAllContainerJobs(containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/jobs", headers, queryParams)
}

// Kills the job
func (s *NodesService) KillContainerJob(jobid, containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/jobs/"+jobid, headers, queryParams)
}

// Get details of a submitted job on the container
func (s *NodesService) GetContainerJob(jobid, containerid, nodeid string, headers, queryParams map[string]interface{}) (JobResult, *http.Response, error) {
	var u JobResult

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/jobs/"+jobid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Send signal to the job
func (s *NodesService) SendSignalToJob(jobid, containerid, nodeid string, body ProcessSignal, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/jobs/"+jobid, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Ping this container
func (s *NodesService) PingContainer(containerid, nodeid string, headers, queryParams map[string]interface{}) (bool, *http.Response, error) {
	var u bool

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/ping", nil, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get running processes in this container
func (s *NodesService) ListContainerProcesses(containerid, nodeid string, headers, queryParams map[string]interface{}) ([]Process, *http.Response, error) {
	var u []Process

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/processes", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Start a new process in this container
func (s *NodesService) StartContainerProcess(containerid, nodeid string, body CoreSystem, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/processes", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Send signal to the process
func (s *NodesService) SendSignalToProcess(processid, containerid, nodeid string, body ProcessSignal, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/processes/"+processid, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get process details
func (s *NodesService) GetContainerProcess(processid, containerid, nodeid string, headers, queryParams map[string]interface{}) (Process, *http.Response, error) {
	var u Process

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/processes/"+processid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Kill Process
func (s *NodesService) KillContainerProcess(processid, containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/processes/"+processid, headers, queryParams)
}

// Start Container instance
func (s *NodesService) StartContainer(containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/start", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// The aggregated consumption of container + all processes (cpu, memory, etc...)
func (s *NodesService) GetContainerState(containerid, nodeid string, headers, queryParams map[string]interface{}) (CoreStateResult, *http.Response, error) {
	var u CoreStateResult

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/state", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Stop Container instance
func (s *NodesService) StopContainer(containerid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/containers/"+containerid+"/stop", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get detailed information of all CPUs in the node
func (s *NodesService) GetCPUInfo(nodeid string, headers, queryParams map[string]interface{}) ([]CPUInfo, *http.Response, error) {
	var u []CPUInfo

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/cpus", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get detailed information of all the disks in the node
func (s *NodesService) GetDiskInfo(nodeid string, headers, queryParams map[string]interface{}) ([]DiskInfo, *http.Response, error) {
	var u []DiskInfo

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/disks", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get detailed information of the os of the node
func (s *NodesService) GetNodeOSInfo(nodeid string, headers, queryParams map[string]interface{}) (OSInfo, *http.Response, error) {
	var u OSInfo

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/info", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// List running jobs
func (s *NodesService) ListNodeJobs(nodeid string, headers, queryParams map[string]interface{}) ([]JobListItem, *http.Response, error) {
	var u []JobListItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/jobs", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Kills all running jobs
func (s *NodesService) KillAllNodeJobs(nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/jobs", headers, queryParams)
}

// Get the details of a submitted job
func (s *NodesService) GetNodeJob(jobid, nodeid string, headers, queryParams map[string]interface{}) (JobResult, *http.Response, error) {
	var u JobResult

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/jobs/"+jobid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Kills the job
func (s *NodesService) KillNodeJob(jobid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/jobs/"+jobid, headers, queryParams)
}

// Get detailed information about the memory in the node
func (s *NodesService) GetMemInfo(nodeid string, headers, queryParams map[string]interface{}) (MemInfo, *http.Response, error) {
	var u MemInfo

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/mem", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get detailed information about the network interfaces in the node
func (s *NodesService) GetNicInfo(nodeid string, headers, queryParams map[string]interface{}) ([]NicInfo, *http.Response, error) {
	var u []NicInfo

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/nics", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Ping this node
func (s *NodesService) PingNode(nodeid string, headers, queryParams map[string]interface{}) (bool, *http.Response, error) {
	var u bool

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/ping", nil, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get Processes
func (s *NodesService) ListNodeProcesses(nodeid string, headers, queryParams map[string]interface{}) ([]Process, *http.Response, error) {
	var u []Process

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/processes", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get process details
func (s *NodesService) GetNodeProcess(processid, nodeid string, headers, queryParams map[string]interface{}) (Process, *http.Response, error) {
	var u Process

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/processes/"+processid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Kill Process
func (s *NodesService) KillNodeProcess(processid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/processes/"+processid, headers, queryParams)
}

// Immediately reboot the machine.
func (s *NodesService) RebootNode(nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/reboot", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// The aggregated consumption of node + all processes (cpu, memory, etc...)
func (s *NodesService) GetNodeState(nodeid string, headers, queryParams map[string]interface{}) (CoreStateResult, *http.Response, error) {
	var u CoreStateResult

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/state", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new storage pool
func (s *NodesService) CreateStoragePool(nodeid string, body StoragePoolCreate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List storage pools present in the node
func (s *NodesService) ListStoragePools(nodeid string, headers, queryParams map[string]interface{}) ([]StoragePoolListItem, *http.Response, error) {
	var u []StoragePoolListItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get detailed information of this storage pool
func (s *NodesService) GetStoragePoolInfo(storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (StoragePool, *http.Response, error) {
	var u StoragePool

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete the storage pool
func (s *NodesService) DeleteStoragePool(storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname, headers, queryParams)
}

// Lists the devices in the storage pool
func (s *NodesService) ListStoragePoolDevices(storagepoolname, nodeid string, headers, queryParams map[string]interface{}) ([]StoragePoolDevice, *http.Response, error) {
	var u []StoragePoolDevice

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/devices", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Add extra devices to this storage pool
func (s *NodesService) CreateStoragePoolDevices(storagepoolname, nodeid string, body []string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/devices", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get information of the device
func (s *NodesService) GetStoragePoolDeviceInfo(deviceuuid, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (StoragePoolDevice, *http.Response, error) {
	var u StoragePoolDevice

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/devices/"+deviceuuid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes the device from the storagepool
func (s *NodesService) DeleteStoragePoolDevice(deviceuuid, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/devices/"+deviceuuid, headers, queryParams)
}

// Create a new filesystem
func (s *NodesService) CreateFilesystem(storagepoolname, nodeid string, body FilesystemCreate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List filesystems
func (s *NodesService) ListFilesystems(storagepoolname, nodeid string, headers, queryParams map[string]interface{}) ([]string, *http.Response, error) {
	var u []string

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete filesystem
func (s *NodesService) DeleteFilesystem(filesystemname, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems/"+filesystemname, headers, queryParams)
}

// Get detailed filesystem information
func (s *NodesService) GetFilesystemInfo(filesystemname, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (Filesystem, *http.Response, error) {
	var u Filesystem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems/"+filesystemname, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new readonly filesystem of the current state of the vdisk
func (s *NodesService) CreateSnapshot(filesystemname, storagepoolname, nodeid string, body string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems/"+filesystemname+"/snapshots", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List snapshots of this filesystem
func (s *NodesService) ListFilesystemSnapshots(filesystemname, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) ([]string, *http.Response, error) {
	var u []string

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems/"+filesystemname+"/snapshots", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get detailed information on the snapshot
func (s *NodesService) GetFilesystemSnapshotInfo(snapshotname, filesystemname, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (Snapshot, *http.Response, error) {
	var u Snapshot

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems/"+filesystemname+"/snapshots/"+snapshotname, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete snapshot
func (s *NodesService) DeleteFilesystemSnapshot(snapshotname, filesystemname, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems/"+filesystemname+"/snapshots/"+snapshotname, headers, queryParams)
}

// Rollback the filesystem to the state at the moment the snapshot was taken
func (s *NodesService) RollbackFilesystemSnapshot(snapshotname, filesystemname, storagepoolname, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/storagepools/"+storagepoolname+"/filesystems/"+filesystemname+"/snapshots/"+snapshotname+"/rollback", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Creates the VM
func (s *NodesService) CreateVM(nodeid string, body VMCreate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/vms", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List VMs
func (s *NodesService) ListVMs(nodeid string, headers, queryParams map[string]interface{}) ([]VMListItem, *http.Response, error) {
	var u []VMListItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/vms", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Deletes the VM
func (s *NodesService) DeleteVM(vmid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid, headers, queryParams)
}

// updates the VM
func (s *NodesService) UpdateVM(vmid, nodeid string, body VMUpdate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get detailed virtual machine object
func (s *NodesService) GetVM(vmid, nodeid string, headers, queryParams map[string]interface{}) (VM, *http.Response, error) {
	var u VM

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get statistical information about the virtual machine.
func (s *NodesService) GetVMInfo(vmid, nodeid string, headers, queryParams map[string]interface{}) (VMInfo, *http.Response, error) {
	var u VMInfo

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid+"/info", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Migrate the VM to another host
func (s *NodesService) MigrateVM(vmid, nodeid string, body VMMigrate, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid+"/migrate", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Pauses the VM
func (s *NodesService) PauseVM(vmid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid+"/pause", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Resumes the VM
func (s *NodesService) ResumeVM(vmid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid+"/resume", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Gracefully shutdown the VM
func (s *NodesService) ShutdownVM(vmid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid+"/shutdown", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Starts the VM
func (s *NodesService) StartVM(vmid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid+"/start", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Stops the VM
func (s *NodesService) StopVM(vmid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/vms/"+vmid+"/stop", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List running Zerotier networks
func (s *NodesService) ListZerotier(nodeid string, headers, queryParams map[string]interface{}) ([]ZerotierListItem, *http.Response, error) {
	var u []ZerotierListItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/zerotiers", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Join Zerotier network
func (s *NodesService) JoinZerotier(nodeid string, body ZerotierJoin, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/nodes/"+nodeid+"/zerotiers", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get Zerotier network details
func (s *NodesService) GetZerotier(zerotierid, nodeid string, headers, queryParams map[string]interface{}) (Zerotier, *http.Response, error) {
	var u Zerotier

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/nodes/"+nodeid+"/zerotiers/"+zerotierid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Exit the Zerotier network
func (s *NodesService) ExitZerotier(zerotierid, nodeid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/nodes/"+nodeid+"/zerotiers/"+zerotierid, headers, queryParams)
}
