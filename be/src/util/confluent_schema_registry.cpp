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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/core_local.cpp

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

#include "util/confluent_schema_registry.h"

#define SERDES_FRAMING_CP1 5
#define SERDES_FRAMING_NONE 0

namespace starrocks {

uint32_t get_schema_id(serdes_t *sd, const uint8_t **payloadp, size_t *sizep, char *errstr, int errstr_size) {
    const uint8_t *payload = *payloadp;
    size_t size = *sizep;
    
    size_t header_len = serdes_deserializer_framing_size(sd);
    uint32_t schema_id = -1;
    if (header_len == SERDES_FRAMING_NONE) {
        snprintf(errstr, errstr_size,
                "\"deserializer.framing\" not configured");
        return -1;
    } else if (header_len == SERDES_FRAMING_CP1) {
        if (size < 5) {
            snprintf(errstr, errstr_size,
                        "Payload is smaller (%zd) than framing (%d)",
                         size, 5);
            return -1;
        }
        /* Magic byte */
        if (payload[0] != 0) {
                snprintf(errstr, errstr_size,
                         "Invalid CP1 magic byte %d, expected %d",
                         payload[0], 0);
                return -1;
        }
        uint32_t* schema_idp = &schema_id;
        /* Schema ID */
        memcpy(schema_idp, payload+1, 4);
        *schema_idp = ntohl(*schema_idp);
        /* Seek past framing */
        payload += 5;
        size -= 5;
        *payloadp = (const uint8_t *)payload;
        *sizep    = size;
    } else {
        snprintf(errstr, errstr_size, "Unsupported framing type\n");
    }
    return schema_id;
}

void *pb_schema_load_cb (serdes_schema_t *ss,
                         const char *definition,
                                  size_t definition_len,
                                  char *errstr, size_t errstr_size,
                                  void *opaque) {
        void* pb_schema = malloc(sizeof(char) * definition_len);
        memcpy(pb_schema, definition, definition_len);
        return pb_schema;
}

void pb_schema_unload_cb (serdes_schema_t *ss, void *schema_obj,
                                   void *opaque) {
        free(schema_obj);
}

} // namespace starrocks