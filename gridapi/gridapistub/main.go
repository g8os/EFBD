package gridapistub

import (
	"log"
	"net/http"

	"github.com/g8os/blockstor/gridapi/gridapistub/goraml"

	"github.com/gorilla/mux"
	"gopkg.in/validator.v2"
)

func main() {
	// input validator
	validator.SetValidationFunc("multipleOf", goraml.MultipleOf)

	r := mux.NewRouter()

	// home page
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	NodesInterfaceRoutes(r, NodesAPI{})

	StorageclustersInterfaceRoutes(r, StorageclustersAPI{})

	VdisksInterfaceRoutes(r, VdisksAPI{})

	log.Println("starting server")
	http.ListenAndServe(":5000", r)
}
