// Package pki makes the small certificate hierarchy raftlab needs for mutual
// TLS between nodes: one self-signed CA and one leaf certificate per node.
// It is meant for demos, tests and private clusters, not as a certificate
// manager: no revocation, no rotation, fixed lifetimes.
package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

// NodeName is the certificate Common Name of node id. The transport requires
// the sender of every peer message to present exactly this name.
func NodeName(id raft.NodeID) string { return fmt.Sprintf("raftlab-node-%d", id) }

// CA signs node certificates.
type CA struct {
	Cert *x509.Certificate
	key  *ecdsa.PrivateKey
	PEM  []byte // the CA certificate
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

// NewCA creates a fresh CA valid for ten years.
func NewCA() (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "raftlab CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0),
		IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// Pool returns a certificate pool containing only this CA.
func (ca *CA) Pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.Cert)
	return p
}

// Issue creates a certificate for node id, valid for both server and client
// authentication and for the given host names or IP addresses (so the same
// certificate serves the node's listener and authenticates its outgoing calls).
func (ca *CA) Issue(id raft.NodeID, hosts ...string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: NodeName(id)},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(1, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

// IssuePair is Issue followed by tls.X509KeyPair.
func (ca *CA) IssuePair(id raft.NodeID, hosts ...string) (tls.Certificate, error) {
	c, k, err := ca.Issue(id, hosts...)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(c, k)
}

// WriteCluster creates a CA and a certificate per node in dir: ca.pem,
// node-<id>.pem and node-<id>-key.pem (the key files are mode 0600). The CA's
// private key is not written: it is only needed to issue, and this tool issues
// everything in one go. Existing files are never overwritten.
func WriteCluster(dir string, ids []raft.NodeID, hosts []string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	ca, err := NewCA()
	if err != nil {
		return err
	}
	write := func(name string, data []byte, mode os.FileMode) error {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	}
	if err := write("ca.pem", ca.PEM, 0o644); err != nil {
		return err
	}
	for _, id := range ids {
		c, k, err := ca.Issue(id, hosts...)
		if err != nil {
			return err
		}
		if err := errors.Join(write(fmt.Sprintf("node-%d.pem", id), c, 0o644), write(fmt.Sprintf("node-%d-key.pem", id), k, 0o600)); err != nil {
			return err
		}
	}
	return nil
}
