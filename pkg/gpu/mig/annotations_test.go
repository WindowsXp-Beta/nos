package mig

import (
	"fmt"
	"github.com/nebuly-ai/nebulnetes/pkg/api/n8s.nebuly.ai/v1alpha1"
	"github.com/nebuly-ai/nebulnetes/pkg/gpu/mig/types"
	"github.com/nebuly-ai/nebulnetes/pkg/util/resource"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestSpecMatchesStatusAnnotations(t *testing.T) {
	testCases := []struct {
		name     string
		status   map[string]string
		spec     map[string]string
		expected bool
	}{
		{
			name:     "Empty maps",
			status:   make(map[string]string),
			spec:     make(map[string]string),
			expected: true,
		},
		{
			name: "Matches",
			status: map[string]string{
				fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 0, "1g.10gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 0, "1g.10gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 0, "2g.40gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 0, "2g.40gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 1, "1g.20gb"): "2",
				fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 1, "1g.20gb"): "2",
			},
			spec: map[string]string{
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "1g.10gb"): "2",
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "2g.40gb"): "2",
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 1, "1g.20gb"): "4",
			},
			expected: true,
		},
		{
			name: "Do not matches",
			status: map[string]string{
				fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 0, "1g.10gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 0, "1g.10gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 0, "2g.40gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 0, "2g.40gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 1, "1g.20gb"): "2",
				fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 1, "1g.20gb"): "2",
			},
			spec: map[string]string{
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "1g.10gb"): "2",
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "2g.40gb"): "2",
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 1, "1g.20gb"): "4",
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 1, "4g.40gb"): "1",
			},
			expected: false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			specAnnotations := make([]types.GPUSpecAnnotation, 0)
			for k, v := range tt.spec {
				a, _ := types.NewGPUSpecAnnotation(k, v)
				specAnnotations = append(specAnnotations, a)
			}

			statusAnnotations := make([]types.GPUStatusAnnotation, 0)
			for k, v := range tt.status {
				a, _ := types.NewGPUStatusAnnotation(k, v)
				statusAnnotations = append(statusAnnotations, a)
			}

			matches := SpecMatchesStatus(specAnnotations, statusAnnotations)
			assert.Equal(t, tt.expected, matches)
		})
	}
}

func TestSpecMatchesResources(t *testing.T) {
	testCases := []struct {
		name      string
		resources []types.MigDeviceResource
		spec      map[string]string
		expected  bool
	}{
		{
			name:      "Empty",
			spec:      make(map[string]string),
			resources: make([]types.MigDeviceResource, 0),
			expected:  true,
		},
		{
			name: "Matches",
			resources: []types.MigDeviceResource{
				{
					Device: resource.Device{
						ResourceName: v1.ResourceName("nvidia.com/mig-1g.10gb"),
					},
					GpuIndex: 0,
				},
				{
					Device: resource.Device{
						ResourceName: v1.ResourceName("nvidia.com/mig-1g.10gb"),
					},
					GpuIndex: 0,
				},
				{
					Device: resource.Device{
						ResourceName: v1.ResourceName("nvidia.com/mig-2g.40gb"),
					},
					GpuIndex: 0,
				},
				{
					Device: resource.Device{
						ResourceName: v1.ResourceName("nvidia.com/mig-1g.20gb"),
					},
					GpuIndex: 1,
				},
				{
					Device: resource.Device{
						ResourceName: v1.ResourceName("nvidia.com/mig-1g.20gb"),
					},
					GpuIndex: 1,
				},
			},
			spec: map[string]string{
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "1g.10gb"): "2",
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "2g.40gb"): "1",
				fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 1, "1g.20gb"): "2",
			},
			expected: true,
		},
		//{
		//	name: "Do not matches",
		//	status: map[string]string{
		//		fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 0, "1g.10gb"): "1",
		//		fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 0, "1g.10gb"): "1",
		//		fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 0, "2g.40gb"): "1",
		//		fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 0, "2g.40gb"): "1",
		//		fmt.Sprintf(v1alpha1.AnnotationFreeMigStatusFormat, 1, "1g.20gb"): "2",
		//		fmt.Sprintf(v1alpha1.AnnotationUsedMigStatusFormat, 1, "1g.20gb"): "2",
		//	},
		//	spec: map[string]string{
		//		fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "1g.10gb"): "2",
		//		fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 0, "2g.40gb"): "2",
		//		fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 1, "1g.20gb"): "4",
		//		fmt.Sprintf(v1alpha1.AnnotationGPUMigSpecFormat, 1, "4g.40gb"): "1",
		//	},
		//	expected: false,
		//},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			specAnnotations := make([]types.GPUSpecAnnotation, 0)
			for k, v := range tt.spec {
				a, _ := types.NewGPUSpecAnnotation(k, v)
				specAnnotations = append(specAnnotations, a)
			}

			matches := SpecMatchesResources(specAnnotations, tt.resources)
			assert.Equal(t, tt.expected, matches)
		})
	}
}
