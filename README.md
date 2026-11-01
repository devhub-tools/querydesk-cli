# QueryDesk CLI

Connect Postico, psql, TablePlus, Sequel Ace or any other Postgres or MySQL client to the databases you can reach in [QueryDesk](https://querydesk.com).

The CLI listens on localhost. It speaks each database's wire protocol and sends every statement to QueryDesk over HTTPS, signed in as you. QueryDesk runs the statement with its usual reviews, data protection and audit log. Your client never gets a database password, and your machine never needs a network route to the database.

## Install

With Homebrew, on macOS or Linux:

```bash
brew install devhub-tools/tap/querydesk
```

Or download the archive for your platform from [Releases](https://github.com/devhub-tools/querydesk-cli/releases) and put `querydesk` on your `PATH`. To build it from source:

```bash
go install github.com/devhub-tools/querydesk-cli@latest
```

## Use

Sign in. This opens your browser and keeps the sign-in in your OS keychain:

```bash
querydesk login
```

Pass `--server https://querydesk.example.com` if you run your own QueryDesk.

Start the listeners and leave the process running:

```bash
querydesk start
```

List your databases and the URL to connect to each one:

```bash
querydesk databases
```

```
DATABASE       ENGINE    CONNECT WITH
orders         mysql     mysql://app:…@127.0.0.1:13306/orders
payments-prod  postgres  postgres://reader:…@127.0.0.1:15432/payments-prod?sslmode=disable
```

Paste a URL into your client, or fill in the fields:

| Field    | Value                                                               |
| -------- | ------------------------------------------------------------------- |
| Host     | `127.0.0.1`                                                         |
| Port     | `15432` for Postgres and CockroachDB, `13306` for MySQL and TiDB    |
| Database | the database's name in QueryDesk                                    |
| User     | the QueryDesk credential's username, or anything for the default one |
| Password | the local password that `querydesk databases` shows                 |

The local password stops other users and processes on your machine from running queries as you. It lives in the CLI's config file (`querydesk/config.json` under your OS config directory), and you can change it there.

A MySQL client can also connect without a database. `SHOW DATABASES` lists the ones you can reach, and `USE` picks one.

## How it behaves

- QueryDesk runs each statement on its own, so transactions aren't available. `BEGIN` fails with a message that says so. Leave your client in autocommit.
- `SET` changes stay in the CLI and `SHOW` reads them back. They don't reach the database.
- A query that needs review fails with the id of the query waiting in QueryDesk. Run it again once it's approved.
- Cancelling a query in your client (Postgres cancel, or MySQL `KILL QUERY`) closes its request to QueryDesk.
- Rows stream as QueryDesk reads them, so a large result starts arriving right away.
- `COPY` to or from the client, `LISTEN`/`NOTIFY` and Postgres binary result formats aren't supported.

Set `QUERYDESK_TOKEN` to use a QueryDesk access token instead of the keychain sign-in, for a script or CI.

## Release

Push a `v0.1.0` tag. The Release workflow builds every platform with GoReleaser and publishes a GitHub release. A tag with a suffix (`v0.1.0-beta`) is a prerelease. [devhub-tools/homebrew-tap](https://github.com/devhub-tools/homebrew-tap) checks for a new release every hour and points the cask at it. To update Homebrew right away:

```bash
gh workflow run querydesk.yml --repo devhub-tools/homebrew-tap
```

## License

MIT
