package rest

import (
	"github.com/zero-os/0-stor/server/db"
	"net/http"
	"github.com/gorilla/mux"
	"github.com/zero-os/0-stor/server/manager"
)

type CreateNamespaceMiddleware struct {
	db db.DB
}

func NewCreateNamespaceMiddleware(db db.DB) *CreateNamespaceMiddleware{
	return &CreateNamespaceMiddleware{
		db: db,
	}
}

// Handler return HTTP handler representation of this middleware
func (cn *CreateNamespaceMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create namespace if not exists
		if nsid, OK := mux.Vars(r)["nsid"]; OK{
			if err := manager.NewNamespaceManager(cn.db).Create(nsid); err != nil{
				w.WriteHeader(500)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
