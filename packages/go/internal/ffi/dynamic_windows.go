//go:build windows

package ffi

import "syscall"

type dynamicLibrary struct {
	dll *syscall.DLL
}

func openDynamicLibrary(path string) (*dynamicLibrary, error) {
	dll, err := syscall.LoadDLL(path)
	if err != nil {
		return nil, err
	}
	return &dynamicLibrary{dll: dll}, nil
}

func (l *dynamicLibrary) lookup(name string) (uintptr, error) {
	proc, err := l.dll.FindProc(name)
	if err != nil {
		return 0, err
	}
	return proc.Addr(), nil
}

func (l *dynamicLibrary) close() error {
	if l == nil || l.dll == nil {
		return nil
	}
	err := l.dll.Release()
	l.dll = nil
	return err
}
