package graphcontroller

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func TestNormalizeSecretStringData(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]any
		want map[string]any
	}{
		{
			name: "stringData folds into data",
			in: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"stringData": map[string]any{"tls.key": "key-material", "empty": ""},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": map[string]any{"tls.key": b64("key-material"), "empty": ""},
			},
		},
		{
			name: "stringData wins on collision, other data keys kept",
			in: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data":       map[string]any{"both": b64("from-data"), "raw": b64("raw")},
				"stringData": map[string]any{"both": "from-stringData"},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": map[string]any{"both": b64("from-stringData"), "raw": b64("raw")},
			},
		},
		{
			name: "empty stringData removed, data untouched",
			in: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data":       map[string]any{"raw": b64("raw")},
				"stringData": map[string]any{},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": map[string]any{"raw": b64("raw")},
			},
		},
		{
			name: "null stringData removed",
			in:   map[string]any{"apiVersion": "v1", "kind": "Secret", "stringData": nil},
			want: map[string]any{"apiVersion": "v1", "kind": "Secret"},
		},
		{
			name: "null data with stringData",
			in: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": nil, "stringData": map[string]any{"k": "v"},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": map[string]any{"k": b64("v")},
			},
		},
		{
			name: "Secret without stringData untouched",
			in: map[string]any{
				"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
				"data": map[string]any{"raw": b64("raw")},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
				"data": map[string]any{"raw": b64("raw")},
			},
		},
		{
			name: "non-string value left for the API server to reject",
			in: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"stringData": map[string]any{"k": "v", "n": int64(1)},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"stringData": map[string]any{"k": "v", "n": int64(1)},
			},
		},
		{
			name: "non-map stringData left for the API server to reject",
			in:   map[string]any{"apiVersion": "v1", "kind": "Secret", "stringData": "oops"},
			want: map[string]any{"apiVersion": "v1", "kind": "Secret", "stringData": "oops"},
		},
		{
			name: "non-map data left for the API server to reject",
			in: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": "oops", "stringData": map[string]any{"k": "v"},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": "oops", "stringData": map[string]any{"k": "v"},
			},
		},
		{
			name: "ConfigMap untouched",
			in: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"stringData": map[string]any{"k": "v"},
			},
			want: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"stringData": map[string]any{"k": "v"},
			},
		},
		{
			name: "Secret kind in another group untouched",
			in: map[string]any{
				"apiVersion": "example.com/v1", "kind": "Secret",
				"stringData": map[string]any{"k": "v"},
			},
			want: map[string]any{
				"apiVersion": "example.com/v1", "kind": "Secret",
				"stringData": map[string]any{"k": "v"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			normalizeSecretStringData(tc.in)
			assert.Equal(t, tc.want, tc.in)
		})
	}
}

func TestNormalizeSecretStringData_DoesNotMutateInputData(t *testing.T) {
	data := map[string]any{"raw": b64("raw")}
	obj := map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"data":       data,
		"stringData": map[string]any{"k": "v"},
	}
	normalizeSecretStringData(obj)
	assert.Equal(t, map[string]any{"raw": b64("raw")}, data, "the rendered data map may be shared and must not be mutated")
	assert.Equal(t, map[string]any{"raw": b64("raw"), "k": b64("v")}, obj["data"])
}
