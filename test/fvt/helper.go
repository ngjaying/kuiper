package fvt

import (
	"net/url"
	"path/filepath"
	"runtime"
)

func FilePathToURL(path string) (string, error) {
	abs := filepath.ToSlash(path)
	u := url.URL{
		Scheme: "file",
		Path:   abs,
	}
	if runtime.GOOS == "windows" {
		if len(abs) >= 2 && abs[0] == '/' && abs[2] == ':' {
			u.Path = abs[1:]
		}
	}
	return u.String(), nil
}
