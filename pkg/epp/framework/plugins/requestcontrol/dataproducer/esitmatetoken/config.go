/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
you may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package esitmatetoken

const (
	ModeFixed   Mode = "fixed"
	ModeDynamic Mode = "dynamic"
)

// Mode defines the Mode for image processing.
type Mode string

// Resolution defines the Width and height of an image.
type Resolution struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

type fixedTokenEstimatorConfig struct {
	FixedToken int `json:"fixedToken"`
}

type dynamicTokenEstimatorConfig struct {
	Factor int `json:"factor"`
}

// ImageTokenEstimatorConfig defines the configuration for image modality.
type ImageTokenEstimatorConfig struct {
	Mode              Mode                         `json:"mode"`
	DefaultResolution Resolution                   `json:"defaultResolution"`
	DynamicCfg        *dynamicTokenEstimatorConfig `json:"dynamic,omitempty"`
	FixedCfg          *fixedTokenEstimatorConfig   `json:"fixed,omitempty"`
}

// MultiModalTokenEstimatorConfig defines the configuration for multimodal inputs.
type MultiModalTokenEstimatorConfig struct {
	Image *ImageTokenEstimatorConfig `json:"image,omitempty"`
}

// DefaultMultimodalConfig provides default configuration for multimodal inputs.
var DefaultMultimodalConfig = MultiModalTokenEstimatorConfig{
	Image: &ImageTokenEstimatorConfig{
		Mode: ModeDynamic,
		//  Default is 360p image
		DefaultResolution: Resolution{
			Width:  640,
			Height: 360,
		},
		DynamicCfg: &dynamicTokenEstimatorConfig{
			Factor: 1024,
		},
	},
}
