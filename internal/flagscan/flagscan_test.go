package flagscan

import (
	"reflect"
	"strings"
	"testing"
)

func takesValue(names ...string) func(string) bool {
	return func(name string) bool {
		for _, n := range names {
			if n == name {
				return true
			}
		}
		return false
	}
}

func TestScan(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		value func(string) bool
		want  []Flag
	}{
		{
			name: "double dash with equals",
			args: []string{"--cert-file=/a.crt"},
			want: []Flag{{Name: "cert-file", Value: "/a.crt", HasValue: true, Start: 0, End: 1}},
		},
		{
			name: "single dash with equals",
			args: []string{"-cert-file=/a.crt"},
			want: []Flag{{Name: "cert-file", Value: "/a.crt", HasValue: true, Start: 0, End: 1}},
		},
		{
			name:  "separate value token",
			args:  []string{"--cert-file", "/a.crt", "--name=x"},
			value: takesValue("cert-file"),
			want: []Flag{
				{Name: "cert-file", Value: "/a.crt", HasValue: true, Start: 0, End: 2},
				{Name: "name", Value: "x", HasValue: true, Start: 2, End: 3},
			},
		},
		{
			name: "unknown flag value token is skipped",
			args: []string{"--name", "infra0", "--client-cert-auth"},
			want: []Flag{
				{Name: "name", Start: 0, End: 1},
				{Name: "client-cert-auth", Start: 2, End: 3},
			},
		},
		{
			name: "empty value after equals",
			args: []string{"--cert-file="},
			want: []Flag{{Name: "cert-file", HasValue: true, Start: 0, End: 1}},
		},
		{
			name:  "missing value at the end",
			args:  []string{"--cert-file"},
			value: takesValue("cert-file"),
			want:  []Flag{{Name: "cert-file", Start: 0, End: 1}},
		},
		{
			name:  "value token may start with a dash",
			args:  []string{"--initial-cluster-token", "-x-", "--name=a"},
			value: takesValue("initial-cluster-token"),
			want: []Flag{
				{Name: "initial-cluster-token", Value: "-x-", HasValue: true, Start: 0, End: 2},
				{Name: "name", Value: "a", HasValue: true, Start: 2, End: 3},
			},
		},
		{
			name: "terminator stops scanning",
			args: []string{"--name=a", "--", "--cert-file=/a.crt"},
			want: []Flag{{Name: "name", Value: "a", HasValue: true, Start: 0, End: 1}},
		},
		{
			name: "bad syntax is not a flag",
			args: []string{"-", "---x", "-=y", "--name=a"},
			want: []Flag{{Name: "name", Value: "a", HasValue: true, Start: 3, End: 4}},
		},
		{
			name: "no flags",
			args: []string{"gateway", "start"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := tt.value
			if value == nil {
				value = takesValue()
			}
			got := Scan(tt.args, value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Scan(%q)\n got: %+v\nwant: %+v", strings.Join(tt.args, " "), got, tt.want)
			}
		})
	}
}
