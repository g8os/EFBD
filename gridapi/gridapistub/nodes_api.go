package gridapistub

import (
	"encoding/json"
	"net/http"
)

// NodesAPI is API implementation of /nodes root endpoint
type NodesAPI struct {
}

// ListNodes is the handler for GET /nodes
// List Nodes
func (api NodesAPI) ListNodes(w http.ResponseWriter, r *http.Request) {
	var respBody []Node
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetNode is the handler for GET /nodes/{nodeid}
// Get detailed information of a node
func (api NodesAPI) GetNode(w http.ResponseWriter, r *http.Request) {
	var respBody Node
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListNodeJobs is the handler for GET /nodes/{nodeid}/jobs
// List running jobs
func (api NodesAPI) ListNodeJobs(w http.ResponseWriter, r *http.Request) {
	var respBody []JobListItem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// KillAllNodeJobs is the handler for DELETE /nodes/{nodeid}/jobs
// Kills all running jobs
func (api NodesAPI) KillAllNodeJobs(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetNodeJob is the handler for GET /nodes/{nodeid}/jobs/{jobid}
// Get the details of a submitted job
func (api NodesAPI) GetNodeJob(w http.ResponseWriter, r *http.Request) {
	var respBody JobResult
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// KillNodeJob is the handler for DELETE /nodes/{nodeid}/jobs/{jobid}
// Kills the job
func (api NodesAPI) KillNodeJob(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetNodeOSInfo is the handler for GET /nodes/{nodeid}/info
// Get detailed information of the os of the node
func (api NodesAPI) GetNodeOSInfo(w http.ResponseWriter, r *http.Request) {
	var respBody OSInfo
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListStoragePools is the handler for GET /nodes/{nodeid}/storagepools
// List storage pools present in the node
func (api NodesAPI) ListStoragePools(w http.ResponseWriter, r *http.Request) {
	var respBody []StoragePoolListItem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateStoragePool is the handler for POST /nodes/{nodeid}/storagepools
// Create a new storage pool
func (api NodesAPI) CreateStoragePool(w http.ResponseWriter, r *http.Request) {
	var reqBody StoragePoolCreate

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetStoragePoolInfo is the handler for GET /nodes/{nodeid}/storagepools/{storagepoolname}
// Get detailed information of this storage pool
func (api NodesAPI) GetStoragePoolInfo(w http.ResponseWriter, r *http.Request) {
	var respBody StoragePool
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteStoragePool is the handler for DELETE /nodes/{nodeid}/storagepools/{storagepoolname}
// Delete the storage pool
func (api NodesAPI) DeleteStoragePool(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListStoragePoolDevices is the handler for GET /nodes/{nodeid}/storagepools/{storagepoolname}/devices
// Lists the devices in the storage pool
func (api NodesAPI) ListStoragePoolDevices(w http.ResponseWriter, r *http.Request) {
	var respBody []StoragePoolDevice
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateStoragePoolDevices is the handler for POST /nodes/{nodeid}/storagepools/{storagepoolname}/devices
// Add extra devices to this storage pool
func (api NodesAPI) CreateStoragePoolDevices(w http.ResponseWriter, r *http.Request) {
	var reqBody []string

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetStoragePoolDeviceInfo is the handler for GET /nodes/{nodeid}/storagepools/{storagepoolname}/devices/{deviceuuid}
// Get information of the device
func (api NodesAPI) GetStoragePoolDeviceInfo(w http.ResponseWriter, r *http.Request) {
	var respBody StoragePoolDevice
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteStoragePoolDevice is the handler for DELETE /nodes/{nodeid}/storagepools/{storagepoolname}/devices/{deviceuuid}
// Removes the device from the storagepool
func (api NodesAPI) DeleteStoragePoolDevice(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListFilesystems is the handler for GET /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems
// List filesystems
func (api NodesAPI) ListFilesystems(w http.ResponseWriter, r *http.Request) {
	var respBody []string
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateFilesystem is the handler for POST /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems
// Create a new filesystem
func (api NodesAPI) CreateFilesystem(w http.ResponseWriter, r *http.Request) {
	var reqBody FilesystemCreate

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetFilesystemInfo is the handler for GET /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems/{filesystemname}
// Get detailed filesystem information
func (api NodesAPI) GetFilesystemInfo(w http.ResponseWriter, r *http.Request) {
	var respBody Filesystem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteFilesystem is the handler for DELETE /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems/{filesystemname}
// Delete filesystem
func (api NodesAPI) DeleteFilesystem(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListFilesystemSnapshots is the handler for GET /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems/{filesystemname}/snapshots
// List snapshots of this filesystem
func (api NodesAPI) ListFilesystemSnapshots(w http.ResponseWriter, r *http.Request) {
	var respBody []string
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateSnapshot is the handler for POST /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems/{filesystemname}/snapshots
// Create a new readonly filesystem of the current state of the vdisk
func (api NodesAPI) CreateSnapshot(w http.ResponseWriter, r *http.Request) {
	var reqBody string

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetFilesystemSnapshotInfo is the handler for GET /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems/{filesystemname}/snapshots/{snapshotname}
// Get detailed information on the snapshot
func (api NodesAPI) GetFilesystemSnapshotInfo(w http.ResponseWriter, r *http.Request) {
	var respBody Snapshot
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteFilesystemSnapshot is the handler for DELETE /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems/{filesystemname}/snapshots/{snapshotname}
// Delete snapshot
func (api NodesAPI) DeleteFilesystemSnapshot(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// RollbackFilesystemSnapshot is the handler for POST /nodes/{nodeid}/storagepools/{storagepoolname}/filesystems/{filesystemname}/snapshots/{snapshotname}/rollback
// Rollback the filesystem to the state at the moment the snapshot was taken
func (api NodesAPI) RollbackFilesystemSnapshot(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListVMs is the handler for GET /nodes/{nodeid}/vms
// List VMs
func (api NodesAPI) ListVMs(w http.ResponseWriter, r *http.Request) {
	var respBody []VMListItem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateVM is the handler for POST /nodes/{nodeid}/vms
// Creates the VM
func (api NodesAPI) CreateVM(w http.ResponseWriter, r *http.Request) {
	var reqBody VMCreate

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetVM is the handler for GET /nodes/{nodeid}/vms/{vmid}
// Get detailed virtual machine object
func (api NodesAPI) GetVM(w http.ResponseWriter, r *http.Request) {
	var respBody VM
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// UpdateVM is the handler for PUT /nodes/{nodeid}/vms/{vmid}
// updates the VM
func (api NodesAPI) UpdateVM(w http.ResponseWriter, r *http.Request) {
	var reqBody VMUpdate

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteVM is the handler for DELETE /nodes/{nodeid}/vms/{vmid}
// Deletes the VM
func (api NodesAPI) DeleteVM(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetVMInfo is the handler for GET /nodes/{nodeid}/vms/{vmid}/info
// Get statistical information about the virtual machine.
func (api NodesAPI) GetVMInfo(w http.ResponseWriter, r *http.Request) {
	var respBody VMInfo
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// StartVM is the handler for POST /nodes/{nodeid}/vms/{vmid}/start
// Starts the VM
func (api NodesAPI) StartVM(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// StopVM is the handler for POST /nodes/{nodeid}/vms/{vmid}/stop
// Stops the VM
func (api NodesAPI) StopVM(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// PauseVM is the handler for POST /nodes/{nodeid}/vms/{vmid}/pause
// Pauses the VM
func (api NodesAPI) PauseVM(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ResumeVM is the handler for POST /nodes/{nodeid}/vms/{vmid}/resume
// Resumes the VM
func (api NodesAPI) ResumeVM(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ShutdownVM is the handler for POST /nodes/{nodeid}/vms/{vmid}/shutdown
// Gracefully shutdown the VM
func (api NodesAPI) ShutdownVM(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// MigrateVM is the handler for POST /nodes/{nodeid}/vms/{vmid}/migrate
// Migrate the VM to another host
func (api NodesAPI) MigrateVM(w http.ResponseWriter, r *http.Request) {
	var reqBody VMMigrate

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// RebootNode is the handler for POST /nodes/{nodeid}/reboot
// Immediately reboot the machine.
func (api NodesAPI) RebootNode(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetMemInfo is the handler for GET /nodes/{nodeid}/mem
// Get detailed information about the memory in the node
func (api NodesAPI) GetMemInfo(w http.ResponseWriter, r *http.Request) {
	var respBody MemInfo
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListNodeProcesses is the handler for GET /nodes/{nodeid}/processes
// Get Processes
func (api NodesAPI) ListNodeProcesses(w http.ResponseWriter, r *http.Request) {
	var respBody []Process
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetNodeProcess is the handler for GET /nodes/{nodeid}/processes/{processid}
// Get process details
func (api NodesAPI) GetNodeProcess(w http.ResponseWriter, r *http.Request) {
	var respBody Process
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// KillNodeProcess is the handler for DELETE /nodes/{nodeid}/processes/{processid}
// Kill Process
func (api NodesAPI) KillNodeProcess(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListBridges is the handler for GET /nodes/{nodeid}/bridges
// List bridges
func (api NodesAPI) ListBridges(w http.ResponseWriter, r *http.Request) {
	var respBody []Bridge
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateBridge is the handler for POST /nodes/{nodeid}/bridges
// Creates a new bridge
func (api NodesAPI) CreateBridge(w http.ResponseWriter, r *http.Request) {
	var reqBody BridgeCreate

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetBridge is the handler for GET /nodes/{nodeid}/bridges/{bridgeid}
// Get bridge details
func (api NodesAPI) GetBridge(w http.ResponseWriter, r *http.Request) {
	var respBody Bridge
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteBridge is the handler for DELETE /nodes/{nodeid}/bridges/{bridgeid}
// Remove bridge
func (api NodesAPI) DeleteBridge(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// PingNode is the handler for POST /nodes/{nodeid}/ping
// Ping this node
func (api NodesAPI) PingNode(w http.ResponseWriter, r *http.Request) {
	var respBody bool
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetCPUInfo is the handler for GET /nodes/{nodeid}/cpus
// Get detailed information of all CPUs in the node
func (api NodesAPI) GetCPUInfo(w http.ResponseWriter, r *http.Request) {
	var respBody []CPUInfo
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetNicInfo is the handler for GET /nodes/{nodeid}/nics
// Get detailed information about the network interfaces in the node
func (api NodesAPI) GetNicInfo(w http.ResponseWriter, r *http.Request) {
	var respBody []NicInfo
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListContainers is the handler for GET /nodes/{nodeid}/containers
// List running Containers
func (api NodesAPI) ListContainers(w http.ResponseWriter, r *http.Request) {
	var respBody []ContainerListItem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// CreateContainer is the handler for POST /nodes/{nodeid}/containers
// Create a new Container
func (api NodesAPI) CreateContainer(w http.ResponseWriter, r *http.Request) {
	var reqBody CreateContainer

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetContainer is the handler for GET /nodes/{nodeid}/containers/{containerid}
// Get Container
func (api NodesAPI) GetContainer(w http.ResponseWriter, r *http.Request) {
	var respBody Container
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// DeleteContainer is the handler for DELETE /nodes/{nodeid}/containers/{containerid}
// Delete Container instance
func (api NodesAPI) DeleteContainer(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// StopContainer is the handler for POST /nodes/{nodeid}/containers/{containerid}/stop
// Stop Container instance
func (api NodesAPI) StopContainer(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// FileDownload is the handler for GET /nodes/{nodeid}/containers/{containerid}/filesystem
// Download file from container
func (api NodesAPI) FileDownload(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// FileUpload is the handler for POST /nodes/{nodeid}/containers/{containerid}/filesystem
// Upload file to container
func (api NodesAPI) FileUpload(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// FileDelete is the handler for DELETE /nodes/{nodeid}/containers/{containerid}/filesystem
// Delete file from container
func (api NodesAPI) FileDelete(w http.ResponseWriter, r *http.Request) {
	var reqBody DeleteFile

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListContainerJobs is the handler for GET /nodes/{nodeid}/containers/{containerid}/jobs
// List running jobs on the container
func (api NodesAPI) ListContainerJobs(w http.ResponseWriter, r *http.Request) {
	var respBody []JobListItem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// KillAllContainerJobs is the handler for DELETE /nodes/{nodeid}/containers/{containerid}/jobs
// Kills all running jobs on the container
func (api NodesAPI) KillAllContainerJobs(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetContainerJob is the handler for GET /nodes/{nodeid}/containers/{containerid}/jobs/{jobid}
// Get details of a submitted job on the container
func (api NodesAPI) GetContainerJob(w http.ResponseWriter, r *http.Request) {
	var respBody JobResult
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// SendSignalToJob is the handler for POST /nodes/{nodeid}/containers/{containerid}/jobs/{jobid}
// Send signal to the job
func (api NodesAPI) SendSignalToJob(w http.ResponseWriter, r *http.Request) {
	var reqBody ProcessSignal

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// KillContainerJob is the handler for DELETE /nodes/{nodeid}/containers/{containerid}/jobs/{jobid}
// Kills the job
func (api NodesAPI) KillContainerJob(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// PingContainer is the handler for POST /nodes/{nodeid}/containers/{containerid}/ping
// Ping this container
func (api NodesAPI) PingContainer(w http.ResponseWriter, r *http.Request) {
	var respBody bool
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetContainerState is the handler for GET /nodes/{nodeid}/containers/{containerid}/state
// The aggregated consumption of container + all processes (cpu, memory, etc...)
func (api NodesAPI) GetContainerState(w http.ResponseWriter, r *http.Request) {
	var respBody CoreStateResult
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetContainerOSInfo is the handler for GET /nodes/{nodeid}/containers/{containerid}/info
// Get detailed information of the container os
func (api NodesAPI) GetContainerOSInfo(w http.ResponseWriter, r *http.Request) {
	var respBody OSInfo
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListContainerProcesses is the handler for GET /nodes/{nodeid}/containers/{containerid}/processes
// Get running processes in this container
func (api NodesAPI) ListContainerProcesses(w http.ResponseWriter, r *http.Request) {
	var respBody []Process
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// StartContainerProcess is the handler for POST /nodes/{nodeid}/containers/{containerid}/processes
// Start a new process in this container
func (api NodesAPI) StartContainerProcess(w http.ResponseWriter, r *http.Request) {
	var reqBody CoreSystem

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetContainerProcess is the handler for GET /nodes/{nodeid}/containers/{containerid}/processes/{processid}
// Get process details
func (api NodesAPI) GetContainerProcess(w http.ResponseWriter, r *http.Request) {
	var respBody Process
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// SendSignalToProcess is the handler for POST /nodes/{nodeid}/containers/{containerid}/processes/{processid}
// Send signal to the process
func (api NodesAPI) SendSignalToProcess(w http.ResponseWriter, r *http.Request) {
	var reqBody ProcessSignal

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// KillContainerProcess is the handler for DELETE /nodes/{nodeid}/containers/{containerid}/processes/{processid}
// Kill Process
func (api NodesAPI) KillContainerProcess(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// StartContainer is the handler for POST /nodes/{nodeid}/containers/{containerid}/start
// Start Container instance
func (api NodesAPI) StartContainer(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetNodeState is the handler for GET /nodes/{nodeid}/state
// The aggregated consumption of node + all processes (cpu, memory, etc...)
func (api NodesAPI) GetNodeState(w http.ResponseWriter, r *http.Request) {
	var respBody CoreStateResult
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetDiskInfo is the handler for GET /nodes/{nodeid}/disks
// Get detailed information of all the disks in the node
func (api NodesAPI) GetDiskInfo(w http.ResponseWriter, r *http.Request) {
	var respBody []DiskInfo
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ListZerotier is the handler for GET /nodes/{nodeid}/zerotiers
// List running Zerotier networks
func (api NodesAPI) ListZerotier(w http.ResponseWriter, r *http.Request) {
	var respBody []ZerotierListItem
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// JoinZerotier is the handler for POST /nodes/{nodeid}/zerotiers
// Join Zerotier network
func (api NodesAPI) JoinZerotier(w http.ResponseWriter, r *http.Request) {
	var reqBody ZerotierJoin

	// decode request
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.WriteHeader(400)
		return
	}

	// validate request
	if err := reqBody.Validate(); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// GetZerotier is the handler for GET /nodes/{nodeid}/zerotiers/{zerotierid}
// Get Zerotier network details
func (api NodesAPI) GetZerotier(w http.ResponseWriter, r *http.Request) {
	var respBody Zerotier
	json.NewEncoder(w).Encode(&respBody)
	// uncomment below line to add header
	// w.Header().Set("key","value")
}

// ExitZerotier is the handler for DELETE /nodes/{nodeid}/zerotiers/{zerotierid}
// Exit the Zerotier network
func (api NodesAPI) ExitZerotier(w http.ResponseWriter, r *http.Request) {
	// uncomment below line to add header
	// w.Header().Set("key","value")
}
