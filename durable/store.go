// Package durable supplies atomic, cross-process journals on an operator-owned
// durable POSIX filesystem. Replicas must share this root (including locks).
package durable

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type Store struct{ Root string }
type Entry struct{ path string }

func (s Store) With(ctx context.Context, key string, fn func(*Entry) error) error {
	if !filepath.IsAbs(s.Root) || filepath.Clean(s.Root) != s.Root || key == "" {
		return fmt.Errorf("durable store requires canonical absolute root and key")
	}
	if err := os.MkdirAll(s.Root, 0700); err != nil {
		return err
	}
	name := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
	lock, err := os.OpenFile(filepath.Join(s.Root, name+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return fn(&Entry{filepath.Join(s.Root, name+".json")})
}
func (e *Entry) Read(v any) error {
	b, err := os.ReadFile(e.path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
func (e *Entry) Write(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(e.path), ".journal-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), e.path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(e.path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (s Store) Read(ctx context.Context, key string, v any) error {
	return s.With(ctx, key, func(e *Entry) error { return e.Read(v) })
}
func (s Store) Write(ctx context.Context, key string, v any) error {
	return s.With(ctx, key, func(e *Entry) error { return e.Write(v) })
}
