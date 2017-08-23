package routes

import (
	"net/http"
)

type Router struct {
	handler http.Handler
}

type Middleware func(h http.Handler) http.Handler

func NewRouter(h http.Handler) *Router {
	return &Router{
		handler: h,
	}
}

func (i *Router) Use(middlewares ...Middleware) {
	for _, middleware := range middlewares {
		i.handler = middleware(i.handler)
	}
}

func (i *Router) Handler() http.Handler {
	return i.handler
}
