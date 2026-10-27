package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// Retain the original implementation as a differential oracle.
func referenceStrictJSON(b []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := referenceInspect(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(dst)
}
func referenceInspect(d *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("depth")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if t == nil {
		return errors.New("null")
	}
	if delim, ok := t.(json.Delim); ok {
		if delim != '{' && delim != '[' {
			return errors.New("delimiter")
		}
		keys := map[string]bool{}
		for d.More() {
			if delim == '{' {
				k, err := d.Token()
				if err != nil {
					return err
				}
				key, ok := k.(string)
				if !ok || keys[key] {
					return errors.New("duplicate")
				}
				keys[key] = true
			}
			if err := referenceInspect(d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
	}
	return err
}
func BenchmarkStrictJSON(b *testing.B)          { benchmarkJSON(b, StrictJSON) }
func BenchmarkStrictJSONReference(b *testing.B) { benchmarkJSON(b, referenceStrictJSON) }
func benchmarkJSON(b *testing.B, decode func([]byte, any) error) {
	for _, size := range []int{128, 32768} {
		b.Run(strings.Repeat("k", size/32768)+"bytes_"+map[int]string{128: "128", 32768: "32768"}[size], func(b *testing.B) {
			payload := []byte(`{"model":"chat","messages":[{"role":"user","content":"` + strings.Repeat("x", size) + `"}],"max_tokens":16}`)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				var r Request
				if err := decode(payload, &r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func FuzzStrictJSONEquivalent(f *testing.F) {
	for _, s := range []string{`{}`, `{"x":1,"\u0078":2}`, `{"x":[1,true,"null",{"y":"a\\\"b"}]}`, `{"x":null}`, `[[]]`, `"hi"`, `1e999`, "{\"\xff\":1,\"\xfe\":2}", strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33), `{"messages":[{"role":"user","content":"hi"}]}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			t.Skip()
		}
		var old, new any
		left, right := referenceStrictJSON(b, &old), StrictJSON(b, &new)
		if (left == nil) != (right == nil) {
			t.Fatalf("acceptance mismatch %q: old=%v new=%v", b, left, right)
		}
		if left == nil {
			a, _ := json.Marshal(old)
			c, _ := json.Marshal(new)
			if !bytes.Equal(a, c) {
				t.Fatalf("decode mismatch")
			}
		}
	})
}
