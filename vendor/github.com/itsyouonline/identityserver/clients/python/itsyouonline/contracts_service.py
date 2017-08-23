class ContractsService:
    def __init__(self, client):
        self.client = client



    def GetContract(self, contractId, headers=None, query_params=None):
        """
        Get a contract
        It is method for GET /contracts/{contractId}
        """
        uri = self.client.base_url + "/contracts/"+contractId
        return self.client.session.get(uri, headers=headers, params=query_params)


    def SignContract(self, data, contractId, headers=None, query_params=None):
        """
        Sign a contract
        It is method for POST /contracts/{contractId}/signatures
        """
        uri = self.client.base_url + "/contracts/"+contractId+"/signatures"
        return self.client.post(uri, data, headers=headers, params=query_params)
