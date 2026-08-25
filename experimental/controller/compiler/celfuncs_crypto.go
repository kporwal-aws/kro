package compiler

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"github.com/kubernetes-sigs/kro/pkg/cel/conversion"
)

func celCryptoFunctions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("rsa.generateKey",
			cel.Overload("rsa_generateKey_int",
				[]*cel.Type{cel.IntType},
				cel.StringType,
				cel.UnaryBinding(rsaGenerateKey),
			),
		),
		cel.Function("ecdsa.generateKey",
			cel.Overload("ecdsa_generateKey_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(ecdsaGenerateKey),
			),
		),
		cel.Function("ed25519.generateKey",
			cel.Overload("ed25519_generateKey",
				[]*cel.Type{},
				cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					return ed25519GenerateKey()
				}),
			),
		),
		cel.Function("x509.createCertificateRequest",
			cel.Overload("x509_createCertificateRequest_string_map",
				[]*cel.Type{cel.StringType, cel.DynType},
				cel.StringType,
				cel.BinaryBinding(x509CreateCertificateRequest),
			),
		),
		cel.Function("x509.createCertificate",
			cel.Overload("x509_createCertificate_string_map",
				[]*cel.Type{cel.StringType, cel.DynType},
				cel.StringType,
				cel.BinaryBinding(x509CreateCertificate),
			),
		),
		cel.Function("crypto.publicKey",
			cel.Overload("crypto_publicKey_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(cryptoPublicKey),
			),
		),
	}
}

func rsaGenerateKey(arg ref.Val) ref.Val {
	bits := int(arg.Value().(int64))
	if bits != 2048 && bits != 3072 && bits != 4096 {
		return types.NewErr("rsa.generateKey: invalid bits %d; allowed: 2048, 3072, 4096", bits)
	}
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return types.NewErr("rsa.generateKey: %v", err)
	}
	return marshalPKCS8PEM(key)
}

func ecdsaGenerateKey(arg ref.Val) ref.Val {
	curveName := arg.Value().(string)
	var curve elliptic.Curve
	switch curveName {
	case "P256":
		curve = elliptic.P256()
	case "P384":
		curve = elliptic.P384()
	case "P521":
		curve = elliptic.P521()
	default:
		return types.NewErr("ecdsa.generateKey: invalid curve %q; allowed: P256, P384, P521", curveName)
	}
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		return types.NewErr("ecdsa.generateKey: %v", err)
	}
	return marshalPKCS8PEM(key)
}

func ed25519GenerateKey() ref.Val {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return types.NewErr("ed25519.generateKey: %v", err)
	}
	return marshalPKCS8PEM(priv)
}

func x509CreateCertificateRequest(keyArg, templateArg ref.Val) ref.Val {
	keyPEM := keyArg.Value().(string)
	priv, err := parsePKCS8PEM(keyPEM)
	if err != nil {
		return types.NewErr("x509.createCertificateRequest: %v", err)
	}

	raw, err := conversion.GoNativeType(templateArg)
	if err != nil {
		return types.NewErr("x509.createCertificateRequest: converting template: %v", err)
	}
	tmplMap, ok := raw.(map[string]any)
	if !ok {
		return types.NewErr("x509.createCertificateRequest: template must be a map, got %T", raw)
	}

	// Validate unknown keys
	known := map[string]bool{"subject": true, "dnsNames": true, "ipAddresses": true}
	for k := range tmplMap {
		if !known[k] {
			return types.NewErr("x509.createCertificateRequest: unknown key %q", k)
		}
	}

	// Parse subject (required)
	subjectRaw, exists := tmplMap["subject"]
	if !exists {
		return types.NewErr("x509.createCertificateRequest: missing required field \"subject\"")
	}
	subjectMap, ok := subjectRaw.(map[string]any)
	if !ok {
		return types.NewErr("x509.createCertificateRequest: subject must be a map, got %T", subjectRaw)
	}

	// Validate unknown subject keys
	knownSubject := map[string]bool{"commonName": true, "organization": true}
	for k := range subjectMap {
		if !knownSubject[k] {
			return types.NewErr("x509.createCertificateRequest: unknown subject key %q", k)
		}
	}

	var subject pkix.Name
	var cn string
	if v, ok := subjectMap["commonName"]; ok {
		cn, ok = v.(string)
		if !ok {
			return types.NewErr("x509.createCertificateRequest: subject.commonName must be a string")
		}
		subject.CommonName = cn
	}
	if v, ok := subjectMap["organization"]; ok {
		orgs, err := toStringSlice(v, "subject.organization")
		if err != nil {
			return types.NewErr("x509.createCertificateRequest: %v", err)
		}
		subject.Organization = orgs
	}

	// Parse dnsNames
	var dnsNames []string
	if v, exists := tmplMap["dnsNames"]; exists {
		dnsNames, err = toStringSlice(v, "dnsNames")
		if err != nil {
			return types.NewErr("x509.createCertificateRequest: %v", err)
		}
	}

	// Auto-add CN to dnsNames (deduplicated)
	if cn != "" {
		found := false
		for _, d := range dnsNames {
			if d == cn {
				found = true
				break
			}
		}
		if !found {
			dnsNames = append([]string{cn}, dnsNames...)
		}
	}

	// Parse ipAddresses
	var ips []net.IP
	if v, exists := tmplMap["ipAddresses"]; exists {
		ipStrs, err := toStringSlice(v, "ipAddresses")
		if err != nil {
			return types.NewErr("x509.createCertificateRequest: %v", err)
		}
		for _, s := range ipStrs {
			ip := net.ParseIP(s)
			if ip == nil {
				return types.NewErr("x509.createCertificateRequest: invalid IP address %q", s)
			}
			ips = append(ips, ip)
		}
	}

	// Validate at least one identity
	if cn == "" && len(dnsNames) == 0 && len(ips) == 0 {
		return types.NewErr("x509.createCertificateRequest: at least one identity required (commonName, dnsNames, or ipAddresses)")
	}

	tmpl := &x509.CertificateRequest{
		Subject:     subject,
		DNSNames:    dnsNames,
		IPAddresses: ips,
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, tmpl, priv)
	if err != nil {
		return types.NewErr("x509.createCertificateRequest: %v", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	return types.String(csrPEM)
}

// x509CreateCertificate creates an X.509 certificate signed by the provided private key.
// For self-signed certificates, the same key signs the certificate. The template map supports:
//   - subject: map with commonName (string), organization ([]string)
//   - dnsNames: []string
//   - ipAddresses: []string
//   - validityDays: int (default: 365)
//   - isCA: bool (default: false)
//   - keyUsage: []string (default depends on isCA)
//
// Returns a map with: certPEM, notBefore, notAfter.
func x509CreateCertificate(keyArg, templateArg ref.Val) ref.Val {
	keyPEM := keyArg.Value().(string)
	priv, err := parsePKCS8PEM(keyPEM)
	if err != nil {
		return types.NewErr("x509.createCertificate: %v", err)
	}

	signer, ok := priv.(crypto.Signer)
	if !ok {
		return types.NewErr("x509.createCertificate: key does not implement crypto.Signer")
	}

	raw, err := conversion.GoNativeType(templateArg)
	if err != nil {
		return types.NewErr("x509.createCertificate: converting template: %v", err)
	}
	tmplMap, ok := raw.(map[string]any)
	if !ok {
		return types.NewErr("x509.createCertificate: template must be a map, got %T", raw)
	}

	// Validate unknown keys
	known := map[string]bool{
		"subject": true, "dnsNames": true, "ipAddresses": true,
		"validityDays": true, "isCA": true, "keyUsage": true,
	}
	for k := range tmplMap {
		if !known[k] {
			return types.NewErr("x509.createCertificate: unknown key %q", k)
		}
	}

	// Parse subject (required)
	subjectRaw, exists := tmplMap["subject"]
	if !exists {
		return types.NewErr("x509.createCertificate: missing required field \"subject\"")
	}
	subjectMap, ok := subjectRaw.(map[string]any)
	if !ok {
		return types.NewErr("x509.createCertificate: subject must be a map, got %T", subjectRaw)
	}

	knownSubject := map[string]bool{"commonName": true, "organization": true}
	for k := range subjectMap {
		if !knownSubject[k] {
			return types.NewErr("x509.createCertificate: unknown subject key %q", k)
		}
	}

	var subject pkix.Name
	var cn string
	if v, ok := subjectMap["commonName"]; ok {
		cn, ok = v.(string)
		if !ok {
			return types.NewErr("x509.createCertificate: subject.commonName must be a string")
		}
		subject.CommonName = cn
	}
	if v, ok := subjectMap["organization"]; ok {
		orgs, err := toStringSlice(v, "subject.organization")
		if err != nil {
			return types.NewErr("x509.createCertificate: %v", err)
		}
		subject.Organization = orgs
	}

	// Parse dnsNames
	var dnsNames []string
	if v, exists := tmplMap["dnsNames"]; exists {
		dnsNames, err = toStringSlice(v, "dnsNames")
		if err != nil {
			return types.NewErr("x509.createCertificate: %v", err)
		}
	}

	// Auto-add CN to dnsNames (deduplicated)
	if cn != "" {
		found := false
		for _, d := range dnsNames {
			if d == cn {
				found = true
				break
			}
		}
		if !found {
			dnsNames = append([]string{cn}, dnsNames...)
		}
	}

	// Parse ipAddresses
	var ips []net.IP
	if v, exists := tmplMap["ipAddresses"]; exists {
		ipStrs, err := toStringSlice(v, "ipAddresses")
		if err != nil {
			return types.NewErr("x509.createCertificate: %v", err)
		}
		for _, s := range ipStrs {
			ip := net.ParseIP(s)
			if ip == nil {
				return types.NewErr("x509.createCertificate: invalid IP address %q", s)
			}
			ips = append(ips, ip)
		}
	}

	// Validate at least one identity
	if cn == "" && len(dnsNames) == 0 && len(ips) == 0 {
		return types.NewErr("x509.createCertificate: at least one identity required (commonName, dnsNames, or ipAddresses)")
	}

	// Parse validityDays (default: 365)
	validityDays := int64(365)
	if v, exists := tmplMap["validityDays"]; exists {
		switch vt := v.(type) {
		case int64:
			validityDays = vt
		case float64:
			validityDays = int64(vt)
		default:
			return types.NewErr("x509.createCertificate: validityDays must be an integer, got %T", v)
		}
		if validityDays <= 0 {
			return types.NewErr("x509.createCertificate: validityDays must be positive")
		}
	}

	// Parse isCA (default: false)
	isCA := false
	if v, exists := tmplMap["isCA"]; exists {
		isCA, ok = v.(bool)
		if !ok {
			return types.NewErr("x509.createCertificate: isCA must be a boolean, got %T", v)
		}
	}

	// Parse keyUsage (defaults depend on isCA)
	var keyUsage x509.KeyUsage
	if v, exists := tmplMap["keyUsage"]; exists {
		usageStrs, err := toStringSlice(v, "keyUsage")
		if err != nil {
			return types.NewErr("x509.createCertificate: %v", err)
		}
		for _, u := range usageStrs {
			ku, err := parseKeyUsage(u)
			if err != nil {
				return types.NewErr("x509.createCertificate: %v", err)
			}
			keyUsage |= ku
		}
	} else if isCA {
		keyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign
	} else {
		keyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment
	}

	// Generate serial number
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return types.NewErr("x509.createCertificate: generating serial number: %v", err)
	}

	notBefore := time.Now().UTC()
	notAfter := notBefore.Add(time.Duration(validityDays) * 24 * time.Hour)

	template := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  isCA,
		BasicConstraintsValid: true,
		KeyUsage:              keyUsage,
		SignatureAlgorithm:    selectSignatureAlgorithm(signer),
	}

	// Self-signed: parent == template
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, signer.Public(), priv)
	if err != nil {
		return types.NewErr("x509.createCertificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	return types.String(certPEM)
}

// parseKeyUsage converts a string key usage name to x509.KeyUsage.
func parseKeyUsage(s string) (x509.KeyUsage, error) {
	switch s {
	case "digitalSignature":
		return x509.KeyUsageDigitalSignature, nil
	case "contentCommitment":
		return x509.KeyUsageContentCommitment, nil
	case "keyEncipherment":
		return x509.KeyUsageKeyEncipherment, nil
	case "dataEncipherment":
		return x509.KeyUsageDataEncipherment, nil
	case "keyAgreement":
		return x509.KeyUsageKeyAgreement, nil
	case "certSign":
		return x509.KeyUsageCertSign, nil
	case "crlSign":
		return x509.KeyUsageCRLSign, nil
	default:
		return 0, fmt.Errorf("unknown keyUsage %q; allowed: digitalSignature, contentCommitment, keyEncipherment, dataEncipherment, keyAgreement, certSign, crlSign", s)
	}
}

// selectSignatureAlgorithm picks a signature algorithm appropriate for the key type.
func selectSignatureAlgorithm(signer crypto.Signer) x509.SignatureAlgorithm {
	switch k := signer.Public().(type) {
	case *rsa.PublicKey:
		return x509.SHA256WithRSA
	case *ecdsa.PublicKey:
		switch k.Curve {
		case elliptic.P384():
			return x509.ECDSAWithSHA384
		case elliptic.P521():
			return x509.ECDSAWithSHA512
		default:
			return x509.ECDSAWithSHA256
		}
	case ed25519.PublicKey:
		return x509.PureEd25519
	default:
		return x509.SHA256WithRSA
	}
}

func cryptoPublicKey(arg ref.Val) ref.Val {
	keyPEM := arg.Value().(string)
	priv, err := parsePKCS8PEM(keyPEM)
	if err != nil {
		return types.NewErr("crypto.publicKey: %v", err)
	}
	signer, ok := priv.(crypto.Signer)
	if !ok {
		return types.NewErr("crypto.publicKey: key does not implement crypto.Signer")
	}
	pubDER, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return types.NewErr("crypto.publicKey: %v", err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	return types.String(pubPEM)
}

// helpers

func marshalPKCS8PEM(key any) ref.Val {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return types.NewErr("failed to marshal private key: %v", err)
	}
	p := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	return types.String(p)
}

func parsePKCS8PEM(pemStr string) (any, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM")
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("expected PRIVATE KEY PEM block, got %q", block.Type)
	}
	return x509.ParsePKCS8PrivateKey(block.Bytes)
}

func toStringSlice(v any, field string) ([]string, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a list, got %T", field, v)
	}
	out := make([]string, len(list))
	for i, item := range list {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s[%d] must be a string, got %T", field, i, item)
		}
		out[i] = s
	}
	return out, nil
}
