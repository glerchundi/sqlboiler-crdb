package override

import (
	"embed"
	"path/filepath"
)

// content holds our template overrides
//go:embed templates/* templates_test/*
var templates embed.FS

func Template(filename string) ([]byte, error) {
	return templates.ReadFile(filename)
}

func TemplateFilenames() ([]string, error) {
	return filenames(&templates, ".")
}

func filenames(fs *embed.FS, path string) ([]string, error) {
	entries, err := fs.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, entry := range entries {
		fp := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			res, err := filenames(fs, fp)
			if err != nil {
				return nil, err
			}
			out = append(out, res...)
			continue
		}
		out = append(out, fp)
	}
	return out, nil
}
