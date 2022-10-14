package kvwriter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/jeremywohl/flatten"
)

var (
	kvBufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 100))
		},
	}
)

// Formatter transforms the input into a formatted string.
type Formatter func(interface{}) string

// KeyValueWriter parses the JSON input and writes it in a human-friendly format to Out.
type KeyValueWriter struct {
	// Out is the output destination.
	Out io.Writer

	// PairsDelimiter defines a character to delimit individual pairs. (default: ' ')
	PairsDelimiter rune

	// KeyValueDelimiter defines a character to delimit key and value. (default: '=')
	KeyValueDelimiter rune

	// QuoteValues defines if you want to quote values. If enabled it will quote all values
	// for consistency. If PairsDelimiter doesn't occur in the keys nor values
	// then you don't need to quote values. (default: true)
	QuoteValues bool

	// KeysExclude defines keys to not display in output. JSON structure is flattened so
	// json '{"event": {"name": "x"}}' would produce 'event.name' key with 'x' as a value.
	KeysExclude []string

	FormatKey   Formatter
	FormatValue Formatter

	FormatExtra func(map[string]interface{}, *bytes.Buffer) error
}

// NewKeyValueWriter creates and initializes a new KeyValueWriter.
func NewKeyValueWriter(options ...func(w *KeyValueWriter)) KeyValueWriter {
	w := KeyValueWriter{
		Out:               os.Stdout,
		PairsDelimiter:    ' ',
		KeyValueDelimiter: '=',
		QuoteValues:       true,
	}

	for _, opt := range options {
		opt(&w)
	}

	return w
}

// Write transforms the JSON input with formatters and appends to w.Out.
func (w KeyValueWriter) Write(p []byte) (n int, err error) {
	var buf = kvBufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		kvBufPool.Put(buf)
	}()

	var evt map[string]interface{}
	d := json.NewDecoder(bytes.NewReader(p))
	d.UseNumber()
	err = d.Decode(&evt)
	if err != nil {
		return n, fmt.Errorf("cannot decode event: %s", err)
	}

	evt, err = flatten.Flatten(evt, "", flatten.DotStyle)
	if err != nil {
		return n, fmt.Errorf("cannot flatten event: %s", err)
	}

	w.writePairs(evt, buf)

	if w.FormatExtra != nil {
		err = w.FormatExtra(evt, buf)
		if err != nil {
			return n, err
		}
	}

	err = buf.WriteByte('\n')
	if err != nil {
		return n, err
	}

	_, err = buf.WriteTo(w.Out)
	return len(p), err
}

// writePairs appends formatted key-value pairs to buf.
func (w KeyValueWriter) writePairs(evt map[string]interface{}, buf *bytes.Buffer) {
	var keys = make([]string, 0, len(evt))
	for key := range evt {
		var isExcluded bool
		for _, excluded := range w.KeysExclude {
			if key == excluded {
				isExcluded = true
				break
			}
		}
		if isExcluded {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	fk := defaultFormatKey
	fv := defaultFormatValue

	if w.FormatKey != nil {
		fk = w.FormatKey
	}
	if w.FormatValue != nil {
		fv = w.FormatValue
	}

	for i, key := range keys {
		buf.WriteString(fk(key))
		buf.WriteRune(w.KeyValueDelimiter)

		switch value := evt[key].(type) {
		case string:
			buf.WriteString(quoteValue(fv(value), w.QuoteValues))
		case json.Number:
			buf.WriteString(quoteValue(fv(value), w.QuoteValues))
		default:
			b, err := json.Marshal(value)
			if err != nil {
				buf.WriteString(quoteValue(fmt.Sprintf("[error: %v]", err), w.QuoteValues))
			} else {
				buf.WriteString(quoteValue(fv(b), w.QuoteValues))
			}
		}

		if i < len(keys)-1 { // Skip PairsDelimiter for last field
			buf.WriteRune(w.PairsDelimiter)
		}
	}
}

func quoteValue(v string, q bool) string {
	if q {
		return strconv.Quote(v)
	}
	return v
}

func defaultFormatKey(i interface{}) string {
	return fmt.Sprintf("%s", i)
}

func defaultFormatValue(i interface{}) string {
	return fmt.Sprintf("%s", i)
}
