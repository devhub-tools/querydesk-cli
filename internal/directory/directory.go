// Package directory finds the QueryDesk credential a local connection asks for by its
// database name and user, from the databases the member can reach.
package directory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

type Directory struct {
	Client *querydesk.Client

	mu        sync.Mutex
	databases []querydesk.Database
	listedAt  time.Time
}

// Target is the credential a connection runs as.
type Target struct {
	Database   querydesk.Database
	Credential querydesk.Credential
}

// a PR's database comes and goes, so the list is read again unless it's fresh
const freshFor = 30 * time.Second

func (d *Directory) List(ctx context.Context) ([]querydesk.Database, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.databases != nil && time.Since(d.listedAt) < freshFor {
		return d.databases, nil
	}
	databases, err := d.Client.Databases(ctx)
	if err != nil {
		return nil, err
	}
	d.databases, d.listedAt = databases, time.Now()
	return databases, nil
}

// Find picks the credential for database (a QueryDesk database's name or id, or a
// credential's id) and user (the credential's username; empty or unknown takes the default
// one), among databases whose adapter is in adapters.
func (d *Directory) Find(ctx context.Context, database, user string, adapters []string) (*Target, error) {
	databases, err := d.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, db := range databases {
		if !contains(adapters, db.Adapter) {
			continue
		}
		for _, credential := range db.Credentials {
			if credential.ID == database {
				return &Target{db, credential}, nil
			}
		}
		if db.Name != database && db.ID != database {
			continue
		}
		if credential, ok := pick(db.Credentials, user); ok {
			return &Target{db, credential}, nil
		}
		return nil, fmt.Errorf("%s has no credential you can use", db.Name)
	}

	var names []string
	for _, db := range databases {
		if contains(adapters, db.Adapter) {
			names = append(names, db.Name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("QueryDesk has no %s database you can reach", strings.Join(adapters, " or "))
	}
	return nil, fmt.Errorf("QueryDesk has no database %q you can reach; yours are: %s", database, strings.Join(names, ", "))
}

func pick(credentials []querydesk.Credential, user string) (querydesk.Credential, bool) {
	for _, credential := range credentials {
		if credential.Username == user {
			return credential, true
		}
	}
	for _, credential := range credentials {
		if credential.Default {
			return credential, true
		}
	}
	if len(credentials) > 0 {
		return credentials[0], true
	}
	return querydesk.Credential{}, false
}

func contains(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}
