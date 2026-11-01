// Command querydesk lets a database client on this machine (psql, Postico, TablePlus,
// Sequel Ace) reach the databases you can in QueryDesk. It speaks each database's wire
// protocol on localhost and runs every statement through QueryDesk's HTTPS API, as you,
// with QueryDesk's reviews, data protection and audit log applied.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/devhub-tools/querydesk-cli/internal/auth"
	"github.com/devhub-tools/querydesk-cli/internal/config"
	"github.com/devhub-tools/querydesk-cli/internal/directory"
	"github.com/devhub-tools/querydesk-cli/internal/mysqlwire"
	"github.com/devhub-tools/querydesk-cli/internal/pgwire"
	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

var version = "dev"

const usage = `QueryDesk CLI: reach your QueryDesk databases from any database client.

Usage:
  querydesk login [--server URL]   sign in to QueryDesk in the browser
  querydesk start [--verbose]      listen for database clients on localhost, in the background
  querydesk stop                   stop listening
  querydesk status                 whether it's listening, and where
  querydesk databases              list your databases and how to connect to each
  querydesk logout                 sign out
  querydesk version
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "login":
		err = login(ctx, os.Args[2:])
	case "logout":
		err = logout()
	case "start":
		err = start(ctx, os.Args[2:])
	case "stop":
		err = stopBackground()
	case "status":
		err = status()
	case "databases", "ls":
		err = databases(ctx)
	case "version", "--version":
		fmt.Println(version)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "querydesk:", err)
		os.Exit(1)
	}
}

func login(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("login", flag.ExitOnError)
	server := flags.String("server", "", "QueryDesk's address, such as https://querydesk.example.com")
	_ = flags.Parse(args)

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if *server != "" {
		cfg.Server = strings.TrimRight(*server, "/")
		if err := cfg.Save(); err != nil {
			return err
		}
	}
	a, err := authFor(cfg)
	if err != nil {
		return err
	}
	err = a.SignIn(ctx, func(target string) error {
		fmt.Printf("Opening your browser to sign in to %s.\nIf it doesn't open, visit:\n\n  %s\n\n", cfg.Server, target)
		_ = openBrowser(target)
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Println("Signed in. Run `querydesk start`: it listens in the background and lists your databases and how to connect to each.")
	return nil
}

func logout() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	a, err := authFor(cfg)
	if err != nil {
		return err
	}
	if err := a.SignOut(); err != nil {
		return err
	}
	fmt.Println("Signed out of", cfg.Server)
	return nil
}

func start(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("start", flag.ExitOnError)
	verbose := flags.Bool("verbose", false, "log each statement and how long QueryDesk took to answer it")
	foreground := flags.Bool("foreground", false, "stay in this terminal instead of running in the background")
	_ = flags.Parse(args)
	if !*foreground {
		return startBackground(ctx, *verbose)
	}

	cfg, client, err := setup()
	if err != nil {
		return err
	}
	if _, err := client.Tokens.Token(ctx); err != nil {
		return err
	}
	logger := log.New(os.Stderr, "", log.LstdFlags)
	if *verbose {
		client.Trace = func(sql string, status int, firstByte time.Duration) {
			logger.Printf("%4dms %d %s", firstByte.Milliseconds(), status, oneLine(sql, 120))
		}
	}
	dir := &directory.Directory{Client: client}

	// localhost only: the connection never leaves this machine
	pg, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.PostgresPort))
	if err != nil {
		return fmt.Errorf("couldn't listen for Postgres clients: %w", err)
	}
	my, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.MySQLPort))
	if err != nil {
		pg.Close()
		return fmt.Errorf("couldn't listen for MySQL clients: %w", err)
	}
	go func() {
		<-ctx.Done()
		pg.Close()
		my.Close()
	}()
	// listening: `querydesk start` waits for this, and `querydesk stop` reads it
	removePid, err := writePid()
	if err != nil {
		return err
	}
	defer removePid()
	logger.Printf("signed in to %s", cfg.Server)
	logger.Printf("Postgres clients: 127.0.0.1:%d, MySQL clients: 127.0.0.1:%d", cfg.PostgresPort, cfg.MySQLPort)
	if err := printDatabases(ctx, cfg, client, os.Stderr); err != nil {
		logger.Printf("couldn't list your databases: %v", err)
	}

	errs := make(chan error, 2)
	go func() {
		errs <- (&pgwire.Server{Client: client, Directory: dir, Password: cfg.LocalPassword, Log: logger}).Serve(pg)
	}()
	go func() {
		errs <- (&mysqlwire.Server{Client: client, Directory: dir, Password: cfg.LocalPassword, Log: logger}).Serve(my)
	}()
	if err := <-errs; err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func databases(ctx context.Context) error {
	cfg, client, err := setup()
	if err != nil {
		return err
	}
	return printDatabases(ctx, cfg, client, os.Stdout)
}

func printDatabases(ctx context.Context, cfg *config.Config, client *querydesk.Client, w io.Writer) error {
	list, err := client.Databases(ctx)
	if err != nil {
		return err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	out := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(out, "DATABASE\tENGINE\tCONNECT WITH")
	for _, db := range list {
		for _, credential := range db.Credentials {
			fmt.Fprintf(out, "%s\t%s\t%s\n", db.Name, db.Adapter, connectURL(cfg, db, credential))
		}
	}
	return out.Flush()
}

func connectURL(cfg *config.Config, db querydesk.Database, credential querydesk.Credential) string {
	scheme, port, query := "", 0, ""
	switch {
	case slices.Contains(pgwire.Adapters, db.Adapter):
		scheme, port, query = "postgres", cfg.PostgresPort, "sslmode=disable"
	case slices.Contains(mysqlwire.Adapters, db.Adapter):
		scheme, port = "mysql", cfg.MySQLPort
	default:
		return "(not supported by the CLI yet)"
	}
	u := url.URL{Scheme: scheme, User: url.UserPassword(credential.Username, cfg.LocalPassword),
		Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/" + db.Name, RawQuery: query}
	return u.String()
}

func setup() (*config.Config, *querydesk.Client, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, err
	}
	// a token from the environment, for a script or CI, is used as it is
	if token := os.Getenv("QUERYDESK_TOKEN"); token != "" {
		return cfg, &querydesk.Client{Server: cfg.Server, Tokens: staticToken(token)}, nil
	}
	a, err := authFor(cfg)
	if err != nil {
		return nil, nil, err
	}
	return cfg, &querydesk.Client{Server: cfg.Server, Tokens: a}, nil
}

type staticToken string

func (t staticToken) Token(context.Context) (string, error) { return string(t), nil }

func (t staticToken) Renew(context.Context, string) (string, error) {
	return "", errors.New("QueryDesk rejected QUERYDESK_TOKEN")
}

func authFor(cfg *config.Config) (*auth.Auth, error) {
	dir, err := config.Dir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &auth.Auth{Server: cfg.Server, Store: auth.Keychain{}, LockPath: filepath.Join(dir, "refresh.lock")}, nil
}

func oneLine(sql string, max int) string {
	sql = strings.Join(strings.Fields(sql), " ")
	if len(sql) > max {
		return sql[:max] + "…"
	}
	return sql
}

func openBrowser(target string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", target).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", target).Start()
	default:
		return exec.Command("xdg-open", target).Start()
	}
}
