package etcdconf

import (
	"errors"
	"io/fs"
	"reflect"
	"strings"
	"testing"
)

func files(m map[string]string) func(string) ([]byte, error) {
	return func(path string) ([]byte, error) {
		if s, ok := m[path]; ok {
			return []byte(s), nil
		}
		return nil, fs.ErrNotExist
	}
}

const configFile = `
name: a
listen-client-urls: https://0.0.0.0:2379
advertise-client-urls: https://10.0.0.1:2379
client-transport-security:
  cert-file: /file/server.crt
  key-file: /file/server.key
  trusted-ca-file: /file/ca.crt
  client-cert-auth: true
`

func TestResolve(t *testing.T) {
	fromFile := Settings{
		CertFile:            "/file/server.crt",
		KeyFile:             "/file/server.key",
		TrustedCAFile:       "/file/ca.crt",
		ListenClientURLs:    []string{"https://0.0.0.0:2379"},
		AdvertiseClientURLs: []string{"https://10.0.0.1:2379"},
	}
	tests := []struct {
		name    string
		args    []string
		environ []string
		want    Settings
	}{
		{
			name: "etcd defaults",
			want: Settings{
				ListenClientURLs:    []string{"http://localhost:2379"},
				AdvertiseClientURLs: []string{"http://localhost:2379"},
			},
		},
		{
			name: "flags",
			args: []string{
				"--name=a",
				"--cert-file=/s.crt",
				"--key-file", "/s.key",
				"-trusted-ca-file=/ca.crt",
				"--client-cert-auth",
				"--listen-client-urls=https://0.0.0.0:2379, https://127.0.0.1:2379",
				"--advertise-client-urls", "https://10.0.0.1:2379",
			},
			want: Settings{
				CertFile:            "/s.crt",
				KeyFile:             "/s.key",
				TrustedCAFile:       "/ca.crt",
				ListenClientURLs:    []string{"https://0.0.0.0:2379", "https://127.0.0.1:2379"},
				AdvertiseClientURLs: []string{"https://10.0.0.1:2379"},
			},
		},
		{
			name:    "environment fallback, flag wins",
			args:    []string{"--cert-file=/flag.crt"},
			environ: []string{"ETCD_CERT_FILE=/env.crt", "ETCD_KEY_FILE=/env.key", "ETCD_LISTEN_CLIENT_URLS=https://0.0.0.0:2379"},
			want: Settings{
				CertFile:            "/flag.crt",
				KeyFile:             "/env.key",
				ListenClientURLs:    []string{"https://0.0.0.0:2379"},
				AdvertiseClientURLs: []string{"http://localhost:2379"},
			},
		},
		{
			name:    "config file from flag ignores other flags and environment",
			args:    []string{"--cert-file=/flag.crt", "--config-file", "/etc/etcd.yaml"},
			environ: []string{"ETCD_KEY_FILE=/env.key"},
			want:    fromFile,
		},
		{
			name:    "config file from environment",
			environ: []string{"ETCD_CONFIG_FILE=/etc/etcd.yaml"},
			want:    fromFile,
		},
		{
			name: "config file flag wins over environment",
			args: []string{"--config-file=/etc/etcd.yaml"},
			environ: []string{
				"ETCD_CONFIG_FILE=/missing.yaml",
			},
			want: fromFile,
		},
	}
	read := files(map[string]string{"/etc/etcd.yaml": configFile})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Resolve(tt.args, tt.environ, read)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Resolve()\n got: %+v\nwant: %+v", got, tt.want)
			}
		})
	}
}

func TestResolveConfigFileDefaults(t *testing.T) {
	read := files(map[string]string{"/etc/etcd.yaml": "name: a\n"})
	got, err := Resolve([]string{"--config-file=/etc/etcd.yaml"}, nil, read)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	want := []string{"http://localhost:2379"}
	if !reflect.DeepEqual(got.ListenClientURLs, want) || !reflect.DeepEqual(got.AdvertiseClientURLs, want) {
		t.Errorf("Resolve() = %+v, want etcd default client URLs", got)
	}
}

func TestResolveErrors(t *testing.T) {
	read := files(map[string]string{"/bad.yaml": "listen-client-urls: [1, 2"})
	tests := []struct {
		name string
		args []string
		want error
		text string
	}{
		{name: "missing config file", args: []string{"--config-file=/missing.yaml"}, want: fs.ErrNotExist},
		{name: "invalid config file", args: []string{"--config-file=/bad.yaml"}, text: "/bad.yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Resolve(tt.args, nil, read)
			if err == nil {
				t.Fatal("Resolve() error = nil")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("Resolve() error = %v, want %v", err, tt.want)
			}
			if !strings.Contains(err.Error(), tt.text) {
				t.Errorf("Resolve() error = %q, want it to contain %q", err, tt.text)
			}
		})
	}
}

func TestLocalClientURL(t *testing.T) {
	tests := []struct {
		name   string
		listen []string
		want   string
	}{
		{name: "any IPv4 address", listen: []string{"https://0.0.0.0:2379"}, want: "https://127.0.0.1:2379"},
		{name: "any IPv6 address", listen: []string{"https://[::]:2379"}, want: "https://[::1]:2379"},
		{name: "loopback preferred over specific address", listen: []string{"https://10.0.0.1:2379", "https://127.0.0.1:2379"}, want: "https://127.0.0.1:2379"},
		{name: "localhost kept", listen: []string{"http://localhost:2379"}, want: "http://localhost:2379"},
		{name: "specific address only", listen: []string{"https://10.0.0.1:2379"}, want: "https://10.0.0.1:2379"},
		{name: "unix socket skipped", listen: []string{"unixs://etcd.sock:2379", "https://10.0.0.1:2379"}, want: "https://10.0.0.1:2379"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Settings{ListenClientURLs: tt.listen}.LocalClientURL()
			if err != nil {
				t.Fatalf("LocalClientURL() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("LocalClientURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLocalClientURLErrors(t *testing.T) {
	for _, listen := range [][]string{nil, {"unix://etcd.sock:2379"}, {"https://bad host:2379"}} {
		if got, err := (Settings{ListenClientURLs: listen}).LocalClientURL(); err == nil {
			t.Errorf("LocalClientURL(%q) = %q, want an error", listen, got)
		}
	}
}
