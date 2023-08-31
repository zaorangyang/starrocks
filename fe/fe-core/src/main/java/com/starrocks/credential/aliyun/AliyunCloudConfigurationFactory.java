// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.credential.aliyun;

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;

import java.util.Map;

public class AliyunCloudConfigurationFactory extends CloudConfigurationFactory {
    private final Map<String, String> properties;

    public AliyunCloudConfigurationFactory(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    protected CloudConfiguration buildForStorage() {
        // TODO
        return null;
    }
}