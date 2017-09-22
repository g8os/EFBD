package server

import (
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/karlseguin/ccache"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/zero-os/0-stor/server/jwt"
)

type Method string

type jwtCacheVal struct {
	valid  bool
	scopes []string
}

const (
	// number of entries we want to keep in the LRU cache
	// rough estimation size of jwtCacheVal is 150 bytes
	// if our jwtCacheSize = 1024, it takes : 150 * 1024 bytes = 153 kilobytes
	jwtCacheSize = 2 << 11 // 4096
)

var (
	authEnabled = true
	jwtCache    *ccache.Cache
)

var (
	MethodWrite  Method = "write"
	MethodRead   Method = "read"
	MethodDelete Method = "delete"
	MethodAdmin  Method = "admin"
)

var (
	//ErrNoJWTToken is return when the grpc request doesnt provide a valid jwt token
	ErrNoJWTToken = errors.New("No jwt token in context")

	ErrWrongScopes = errors.New("JWT token doesn't contains required scopes")
)

func init() {
	conf := ccache.Configure()
	conf.MaxSize(jwtCacheSize)

	jwtCache = ccache.New(conf)
}

// disableAuth disable JWT authentification for the server
// call this only during the bootstrap of the server.
func disableAuth() {
	authEnabled = false
}

func validateJWT(ctx context.Context, method Method, label string) error {
	if !authEnabled {
		return nil
	}

	var scopes []string
	token, err := extractJWTToken(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	scopes, err = getScopes(token)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	expected, err := expectedScopes(method, label)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	if !jwt.CheckPermissions(expected, scopes) {
		log.Debugf("wrong scope: expected: %s received: %s", expected, scopes)
		return status.Error(codes.Unauthenticated, ErrWrongScopes.Error())
	}

	return nil
}

// getScopes tries to get token's scopes from cache.
// if not exist, it extract the scopes and insert it to cache
func getScopes(token string) ([]string, error) {
	scopes, inCache, err := getScopesFromCache(token)
	if err != nil {
		// there is error in token
		return nil, err
	}
	if inCache {
		// token is not in cache
		return scopes, nil
	}

	scopes, exp, err := jwt.CheckJWTGetScopes(token)

	if err != nil || time.Until(time.Unix(exp, 0)).Seconds() < 0 {
		// Insert invalid or expired token to cache
		// so we don't need to validate it again
		jwtCache.Set(token, jwtCacheVal{
			valid: false,
		}, time.Hour*24)

		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("expired token")
	}

	// insert valid token to cache
	cacheVal := jwtCacheVal{
		valid:  true,
		scopes: scopes,
	}
	jwtCache.Set(token, cacheVal, time.Until(time.Unix(exp, 0)))
	return scopes, nil
}

// get scopes from the cache
func getScopesFromCache(token string) (scopes []string, exists bool, err error) {
	item := jwtCache.Get(token)
	if item == nil {
		return
	}
	exists = true

	// check validity
	cacheVal := item.Value().(jwtCacheVal)
	if !cacheVal.valid {
		err = fmt.Errorf("invalid token")
		return
	}

	// check cache expiration
	if item.Expired() {
		jwtCache.Delete(token)
		err = fmt.Errorf("expired token")
		return
	}

	scopes = cacheVal.scopes
	return
}

// extractJWTToken extract a token from the incoming grpc request
func extractJWTToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrNoJWTToken
	}

	token, ok := md["authorization"]
	if !ok || len(token) < 1 {
		return "", ErrNoJWTToken
	}

	return token[0], nil
}

// expectedScopes deduct the required scope based on the request method
func expectedScopes(method Method, label string) ([]string, error) {
	if err := jwt.ValidateNamespaceLabel(label); err != nil {
		return nil, err
	}

	adminScope := strings.Replace(label, "_0stor_", ".0stor.", 1)

	return []string{
		// example :: first.0stor.gig.read
		fmt.Sprintf("%s.%s", adminScope, string(method)),
		//admin ::first.0stor.gig
		adminScope,
	}, nil
}
