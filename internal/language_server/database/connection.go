package database

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/denisenkom/go-mssqldb"
)

type Connection struct {
	config *Config
	db     *sql.DB
}

func NewConnection() (*Connection, error) {
	config, err := GetConfig()
	if err != nil {
		return nil, err
	}
	return &Connection{
		config: config,
	}, nil
}

func (c *Connection) Connect() error {
	query := url.Values{}
	if c.config.TrustCert {
		query.Add("TrustServerCertificate", "true")
	} else {
		query.Add("TrustServerCertificate", "false")
	}
    query.Add("app name", "SequelGo")
	u := &url.URL{
		Scheme:     "sqlserver",
		User:       url.UserPassword(c.config.User, c.config.Pass),
		Host:       fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
		OmitHost:   false,
		ForceQuery: false,
		RawQuery:   query.Encode(),
	}
	db, err := sql.Open("sqlserver", u.String())
	if err != nil {
		return fmt.Errorf("SequelGo: Error connecting to Db: %s", err.Error())
	}
    err = db.Ping()
	if err != nil {
		return fmt.Errorf("SequelGo: Error connecting to Db: %s", err.Error())
	}
	c.db = db

	return nil
}
