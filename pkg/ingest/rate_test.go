package ingest

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFormatQuantity(t *testing.T) {

	assert.Equal(t, "1", formatQuantity(1))
	assert.Equal(t, "2 Thousand", formatQuantity(2*1000))
	assert.Equal(t, "2 Million", formatQuantity(2*1000*1000))
	assert.Equal(t, "2.1 Million", formatQuantity(2*1050*1000))
	assert.Equal(t, "2.15 Million", formatQuantity(2150000))
	assert.Equal(t, "2.155 Million", formatQuantity(2155000))
	assert.Equal(t, "2.1556 Million", formatQuantity(2155600))
	assert.Equal(t, "21.556 Million", formatQuantity(21556000))
	assert.Equal(t, "215.566 Million", formatQuantity(215566000))
}
