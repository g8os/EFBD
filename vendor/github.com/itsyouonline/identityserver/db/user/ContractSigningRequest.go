package user

type ContractSigningRequest struct {
	ContractId string `json:"contractId"`
	Party      string `json:"party"`
}
