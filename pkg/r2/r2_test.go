package r2

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCredentials_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.json")
	data := `{
		"account_id": "abc123",
		"access_key_id": "AKID",
		"secret_access_key": "SECRET",
		"bucket": "my-backups"
	}`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	creds, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds.AccountID != "abc123" {
		t.Errorf("AccountID = %q, want %q", creds.AccountID, "abc123")
	}
	if creds.AccessKeyID != "AKID" {
		t.Errorf("AccessKeyID = %q, want %q", creds.AccessKeyID, "AKID")
	}
	if creds.SecretAccessKey != "SECRET" {
		t.Errorf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, "SECRET")
	}
	if creds.Bucket != "my-backups" {
		t.Errorf("Bucket = %q, want %q", creds.Bucket, "my-backups")
	}
}

func TestLoadCredentials_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadCredentials(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestLoadCredentials_FileNotFound(t *testing.T) {
	_, err := LoadCredentials("/nonexistent/creds.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadCredentials_MissingAccountID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.json")
	data := `{"access_key_id": "AKID", "secret_access_key": "SECRET", "bucket": "b"}`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadCredentials(path)
	if err == nil {
		t.Error("expected error for missing account_id")
	}
}

func TestLoadCredentials_MissingAccessKeyID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.json")
	data := `{"account_id": "abc", "secret_access_key": "SECRET", "bucket": "b"}`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadCredentials(path)
	if err == nil {
		t.Error("expected error for missing access_key_id")
	}
}

func TestLoadCredentials_MissingSecretAccessKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.json")
	data := `{"account_id": "abc", "access_key_id": "AKID", "bucket": "b"}`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadCredentials(path)
	if err == nil {
		t.Error("expected error for missing secret_access_key")
	}
}

func TestLoadCredentials_MissingBucket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.json")
	data := `{"account_id": "abc", "access_key_id": "AKID", "secret_access_key": "SECRET"}`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadCredentials(path)
	if err == nil {
		t.Error("expected error for missing bucket")
	}
}
