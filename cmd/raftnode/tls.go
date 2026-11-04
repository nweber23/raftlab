package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// tlsFiles names the PEM files for mutual TLS; all empty means plain HTTP.
type tlsFiles struct{ ca, cert, key string }

func (f tlsFiles) enabled() bool { return f.ca != "" || f.cert != "" || f.key != "" }

// configs builds the listener's and the dialer's tls.Config from the files.
// The listener asks every client for a certificate and verifies one if given,
// so the key-value API stays usable without client certs while POST /raft
// (which requires a verified one, see run) is closed to everyone but peers.
func (f tlsFiles) configs() (server, client *tls.Config, err error) {
	if f.ca == "" || f.cert == "" || f.key == "" {
		return nil, nil, errors.New("-tls-ca, -tls-cert and -tls-key must be given together")
	}
	pemCA, err := os.ReadFile(f.ca)
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemCA) {
		return nil, nil, fmt.Errorf("%s holds no certificate", f.ca)
	}
	pair, err := tls.LoadX509KeyPair(f.cert, f.key)
	if err != nil {
		return nil, nil, err
	}
	server = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair},
		ClientCAs: pool, ClientAuth: tls.VerifyClientCertIfGiven,
	}
	client = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, RootCAs: pool}
	return server, client, nil
}
