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
    final byte keyHash[];
    final int valuePosition;
    final ByteBuffer key;
    final ByteBuffer value;

    public CaskEntry(
            MiniCask miniCask,
            byte keyHash[],
            int valuePosition,
            ByteBuffer key,
            ByteBuffer value) {
        this.miniCask = miniCask;
        this.keyHash = keyHash;
        this.valuePosition = valuePosition;
        this.key = key;
        this.value = value;
    }
}