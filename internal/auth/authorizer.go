package auth

import (
	"fmt"
	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// We will wrap Casbin with our own library, so in the future if we switch
// to another authorization tool, we don't have to change a bunch of code in 
// places, just the code in this library

// Casbin is a library that supports enforcing authorization based on various
// control models, including ACLs. We will use ACL to authorize services in
// this project

// The model and policy arguments are path to the config file where we define the
// model (which in this case will be ACL), and path to a CSV file containing ACL
// tables
func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		if !a.enforcer.Enforce(subject, object, action) {
			msg := fmt.Sprintf(
				"%s not permitted to %s to %s",
				subject,
				action,
				object,
			)
			st := status.New(codes.PermissionDenied, msg)
			return st.Err()
		}
	}
	return nil;
}