// Package flagscan reads command lines written for Go's flag package
// without knowing every flag they contain.
package flagscan

import "strings"

// Flag is one flag found on a command line.
type Flag struct {
	Name  string
	Value string
	// HasValue is false for a bare flag such as "--client-cert-auth".
	HasValue bool
	// Start and End delimit the tokens of the flag: args[Start:End].
	Start, End int
}

// Scan returns the flags in args, following the syntax of Go's flag package.
//
// A flag for which takesValue reports true and that is written without "="
// consumes the next token as its value. For any other flag the next token is
// left alone; tokens that are not flags are skipped, because on a valid etcd
// command line they can only be values of flags unknown to the caller.
// Scanning stops at "--".
func Scan(args []string, takesValue func(name string) bool) []Flag {
	var flags []Flag
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		name, ok := flagName(arg)
		if !ok {
			continue
		}
		f := Flag{Name: name, Start: i, End: i + 1}
		if n, v, found := strings.Cut(name, "="); found {
			f.Name, f.Value, f.HasValue = n, v, true
		} else if takesValue(name) && i+1 < len(args) {
			i++
			f.Value, f.HasValue, f.End = args[i], true, i+1
		}
		flags = append(flags, f)
	}
	return flags
}

func flagName(arg string) (string, bool) {
	if len(arg) < 2 || arg[0] != '-' {
		return "", false
	}
	name := strings.TrimPrefix(arg[1:], "-")
	if name == "" || name[0] == '-' || name[0] == '=' {
		return "", false
	}
	return name, true
}
