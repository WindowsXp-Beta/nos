/*
 * Copyright 2023 nebuly.com.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mig

import (
	"context"
	"fmt"

	"github.com/nebuly-ai/nos/pkg/constant"
	"github.com/nebuly-ai/nos/pkg/gpu"
	"github.com/nebuly-ai/nos/pkg/gpu/nvml"
	"github.com/nebuly-ai/nos/pkg/resource"
	"github.com/nebuly-ai/nos/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type Client interface {
	GetMigDevices(ctx context.Context) (gpu.DeviceList, gpu.Error)
	GetUsedMigDevices(ctx context.Context) (gpu.DeviceList, gpu.Error)
	GetAllocatableMigDevices(ctx context.Context) (gpu.DeviceList, gpu.Error)
	CreateMigDevices(ctx context.Context, profileList ProfileList, client *client.Client, nodeName string) (ProfileList, error)
	DeleteMigDevice(ctx context.Context, device gpu.Device) gpu.Error
	DeleteAllExcept(ctx context.Context, resources gpu.DeviceList) error
}

type clientImpl struct {
	resourceClient resource.Client
	nvmlClient     nvml.Client
}

func NewClient(resourceClient resource.Client, nvmlClient nvml.Client) Client {
	return &clientImpl{
		resourceClient: resourceClient,
		nvmlClient:     nvmlClient,
	}
}

// CreateMigDevices creates the MIG resources provided as argument, which can span multiple GPUs, and returns
// the resources that were actually created.
//
// If any error happens, and it is not possible to create the required resources on a certain GPUs,
// CreateMigResources still tries to create the resources on the other GPUs and returns the ones that
// it possible to create. This means that if any error happens, the returned ProfileList will be a subset
// of the input list, otherwise the two lists will have the same length and items.
func (c clientImpl) CreateMigDevices(ctx context.Context, profileList ProfileList, cli *client.Client, nodeName string) (ProfileList, error) {
	logger := log.FromContext(ctx)
	var errors = make(gpu.ErrorList, 0)
	var createdProfiles = make(ProfileList, 0)
	for gpuIndex, profiles := range profileList.GroupByGPU() {
		profileNames := make([]string, 0)
		for _, p := range profiles {
			profileNames = append(profileNames, p.Name.String())
		}
		var err gpu.Error
		for i := 0; i < 3; i++ {
			err = c.nvmlClient.CreateMigDevices(profileNames, gpuIndex)
			if err.IsInsuffcient() {
				for j := 0; j < 3; j++ {
					logger.V(1).Info("We may encounter the mig constraint", "tried creation times", i, "tried cleanning times", j)
					// 1. get running pods
					var podList v1.PodList
					// TODO(wxp): if we need to specify namespace
					if getPodErr := (*cli).List(ctx, &podList, client.MatchingFields{constant.PodNodeNameKey: nodeName}); getPodErr != nil {
						logger.Error(getPodErr, "Error when retrieving pods in mig creation retrying")
						continue
					}
					// 2. get used devices
					used, err := c.GetUsedMigDevices(ctx)
					if err != nil {
						logger.Error(err, "Error when retrieving used mig devices in mig creation retrying")
						continue
					}
					// 3. delete running pods
					if deletePodErr := (*cli).DeleteAllOf(ctx, &v1.Pod{}, client.MatchingFields{constant.PodNodeNameKey: nodeName}); deletePodErr != nil {
						logger.Error(deletePodErr, fmt.Sprintf("Error when deleting pods running on %v", nodeName))
					}
					// 4. delete used devices
					isErrorInDeletion := false
					for _, migDevice := range used {
						if err := c.DeleteMigDevice(ctx, migDevice); err != nil {
							logger.Error(err, "Error when deleting mig instance in mig creation retrying", "mig instance", migDevice.DeviceId)
							isErrorInDeletion = true
						}
					}
					if isErrorInDeletion {
						continue
					}
					// 5. restart deleted pods
					isErrorInRestarting := false
					for _, pod := range podList.Items {
						pod.Labels[constant.PodRestartLabel] = "true"
						if err := (*cli).Create(ctx, &pod); err != nil {
							logger.Error(err, fmt.Sprintf("Error when restarting pods on %v", nodeName), "pod", pod)
							isErrorInRestarting = true
						}
					}
					if !isErrorInRestarting {
						logger.V(1).Info(fmt.Sprintf("finish cleaning mig instances on %v", nodeName))
						break
					}
				}
			} else {
				errors = append(errors, err)
				continue
			}
		}
		createdProfiles = append(createdProfiles, profiles...)
	}
	if len(errors) > 0 {
		return createdProfiles, errors
	}
	return createdProfiles, nil
}

func (c clientImpl) DeleteMigDevice(_ context.Context, resource gpu.Device) gpu.Error {
	return c.nvmlClient.DeleteMigDevice(resource.DeviceId)
}

func (c clientImpl) GetMigDevices(ctx context.Context) (gpu.DeviceList, gpu.Error) {
	// Get used
	used, err := c.GetUsedMigDevices(ctx)
	if err != nil {
		return nil, err
	}
	// Get allocatable
	allocatable, err := c.GetAllocatableMigDevices(ctx)
	if err != nil {
		return nil, err
	}
	// Get free
	free := gpu.ComputeFreeDevicesAndUpdateStatus(used, allocatable)

	return append(used, free...), nil
}

func (c clientImpl) GetUsedMigDevices(ctx context.Context) (gpu.DeviceList, gpu.Error) {
	// Fetch used devices
	usedResources, err := c.resourceClient.GetUsedDevices(ctx)
	if err != nil {
		return nil, gpu.NewGenericError(err)
	}

	// Consider only NVIDIA GPUs
	var isNvidiaResource = func(d resource.Device) bool {
		return d.IsNvidiaResource()
	}
	usedGpus := util.Filter(usedResources, isNvidiaResource)

	// Extract MIG devices
	return c.extractMigDevices(ctx, usedGpus)
}

func (c clientImpl) GetAllocatableMigDevices(ctx context.Context) (gpu.DeviceList, gpu.Error) {
	// Fetch allocatable devices
	allocatableResources, err := c.resourceClient.GetAllocatableDevices(ctx)
	if err != nil {
		return nil, gpu.NewGenericError(err)
	}

	// Consider only NVIDIA GPUs
	var isNvidiaResource = func(d resource.Device) bool {
		return d.IsNvidiaResource()
	}
	allocatableGPUs := util.Filter(allocatableResources, isNvidiaResource)

	// Extract MIG devices
	return c.extractMigDevices(ctx, allocatableGPUs)
}

// DeleteAllExcept deletes all the devices that are not in the list of devices to keep.
func (c clientImpl) DeleteAllExcept(_ context.Context, resourcesToKeep gpu.DeviceList) error {
	nResources := len(resourcesToKeep)
	idsToKeep := make([]string, nResources)
	for i, r := range resourcesToKeep {
		idsToKeep[i] = r.DeviceId
	}
	return c.nvmlClient.DeleteAllMigDevicesExcept(idsToKeep)
}

func (c clientImpl) extractMigDevices(ctx context.Context, devices []resource.Device) ([]gpu.Device, gpu.Error) {
	logger := klog.FromContext(ctx)

	// Retrieve MIG device ID and GPU index
	migDevices := make([]gpu.Device, 0)
	for _, r := range devices {
		if !IsNvidiaMigDevice(r.ResourceName) {
			continue
		}
		gpuIndex, err := c.nvmlClient.GetMigDeviceGpuIndex(r.DeviceId)
		if gpu.IgnoreNotFound(err) != nil {
			logger.Error(
				err,
				"unable to fetch GPU index",
				"resourceName",
				r.DeviceId,
				"MIG device ID",
				r.DeviceId,
			)
			return nil, err
		}
		if gpu.IsNotFound(err) {
			logger.V(1).Info("could not find GPU index of MIG device", "MIG device ID", r.DeviceId)
			continue
		}
		migDevice := gpu.Device{
			Device:   r,
			GpuIndex: gpuIndex,
		}
		migDevices = append(migDevices, migDevice)
	}

	return migDevices, nil
}
