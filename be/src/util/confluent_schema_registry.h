// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif
#include "libserdes/serdes.h"
#ifdef __cplusplus
}
#endif

uint32_t get_schema_id(serdes_t *sd, const uint8_t **payloadp, size_t *sizep, char *errstr, int errstr_size);

void *pb_schema_load_cb (serdes_schema_t *ss,
                        const char *definition,
                        size_t definition_len,
                        char *errstr, size_t errstr_size,
                        void *opaque);

void pb_schema_unload_cb (serdes_schema_t *ss, void *schema_obj, void *opaque);
