//Copyright 2012 Ariel Weisberg
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package com.afewmoreamps;

import java.nio.ByteBuffer;

class CaskEntry {
    final MiniCask miniCask;
    final long timestamp;
    final byte flags;
    final ByteBuffer key;
    final int valuePosition;
    final int valueLength;
    final ByteBuffer value;

    public CaskEntry(
            MiniCask miniCask,
            long timestamp,
            byte flags,
            ByteBuffer key,
            int valuePosition,
            int valueLength,
            ByteBuffer value) {
        this.miniCask = miniCask;
        this.timestamp = timestamp;
        this.flags = flags;
        this.key = key;
        this.valuePosition = valuePosition;
        this.valueLength = valueLength;
        this.value = value;
    }
}