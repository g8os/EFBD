package grpc

import (
	"errors"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"google.golang.org/grpc/codes"

	"google.golang.org/grpc/status"

	"golang.org/x/net/context"

	"github.com/zero-os/0-stor/server/jwt"

	"google.golang.org/grpc/metadata"
)

type Method string

var (
	MethodWrite  Method = "write"
	MethodRead   Method = "read"
	MethodDelete Method = "delete"
	MethodAdmin  Method = "admin"
)

//ErrNoJWTToken is return when the grpc request doesnt provide a valid jwt token
var ErrNoJWTToken = errors.New("No jwt token in context")
var ErrWrongScopes = errors.New("JWT token doesn't contains required scopes")

func validateJWT(ctx context.Context, method Method, label string) error {
	isTesting, ok := os.LookupEnv("STOR_TESTING")
	if ok && isTesting == "true" {
		return nil
	}

	token, err := extractJWTToken(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	scopes, err := jwt.CheckJWTGetScopes(token)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	// codes.Unauthenticated
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
