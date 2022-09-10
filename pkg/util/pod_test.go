package util

import (
	"github.com/nebuly-ai/nebulnetes/pkg/constant"
	"github.com/nebuly-ai/nebulnetes/pkg/factory"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestIsPodOverQuota(t *testing.T) {
	tests := []struct {
		name     string
		pod      v1.Pod
		expected bool
	}{
		{
			name:     "Pod with label with value overquota",
			expected: true,
		},
		{
			name: "Pod with label with value inquota",
			pod: factory.BuildPod("ns-1", "pd-1").
				WithLabel(constant.LabelCapacityInfo, string(constant.CapacityInfoInQuota)).
				Get(),
			expected: false,
		},
		{
			name:     "Pod without labels",
			pod:      factory.BuildPod("ns-1", "pd-1").Get(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsPodOverQuota(tt.pod))
		})
	}
}
