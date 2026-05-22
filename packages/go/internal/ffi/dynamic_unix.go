//go:build !windows

package ffi

import "github.com/ebitengine/purego"

type dynamicLibrary struct {
	handle uintptr
}

func openDynamicLibrary(path string) (*dynamicLibrary, error) {
	handle, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, err
	}
	return &dynamicLibrary{handle: handle}, nil
}

func (l *dynamicLibrary) lookup(name string) (uintptr, error) {
	return purego.Dlsym(l.handle, name)
}

func (l *dynamicLibrary) close() error {
	if l == nil || l.handle == 0 {
		return nil
	}
	err := purego.Dlclose(l.handle)
	l.handle = 0
	return err
}
