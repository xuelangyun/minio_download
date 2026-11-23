package main

import (
	"path/filepath"
	"testing"
)

func TestSafeComponent(t *testing.T) {
	for _, in := range []string{"", ".", "..", "a/b", "a:b", "name. "} {
		got := safe(in)
		if got == "" || got == "." || got == ".." || filepath.IsAbs(got) {
			t.Fatalf("unsafe component for %q: %q", in, got)
		}
	}
}

func TestSafeJoinRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"../escape", "/absolute", "..", "a/../../b"} {
		if _, err := safeJoin(root, rel); err == nil {
			t.Errorf("expected path traversal to be rejected: %q", rel)
		}
	}
}

func TestSafeJoinAllowsNestedObject(t *testing.T) {
	root := t.TempDir()
	got, err := safeJoin(root, "camera/left/video.mp4")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "camera", "left", "video.mp4")
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMD5ETagRecognition(t *testing.T) {
	if !isMD5ETag("d41d8cd98f00b204e9800998ecf8427e") {
		t.Fatal("standard MD5 ETag should be recognized")
	}
	if isMD5ETag("d41d8cd98f00b204e9800998ecf8427e-2") {
		t.Fatal("multipart ETag must not be treated as MD5")
	}
	if !sameETag("\"abcd\"", "abcd") {
		t.Fatal("quoted and unquoted ETags should match")
	}
}

func TestWindowsObjectNameRules(t *testing.T) {
	if !windowsReserved("CON.txt") || !windowsReserved("lpt1") {
		t.Fatal("Windows reserved device names were not recognized")
	}
	if invalidWindowsObjectPart("bad:name") == false {
		t.Fatal("Windows-invalid object name should be rejected")
	}
	if invalidWindowsObjectPart("camera_left.mp4") {
		t.Fatal("normal object filename should be accepted")
	}
}
