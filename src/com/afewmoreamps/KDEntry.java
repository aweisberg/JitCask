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
import java.nio.ByteOrder;

class KDEntry {
    /**
     * The length of the fixed (not variable size) portion of the key dir header
     * If you change this you need to change the value in KeyDirUnsignedBytes so the comparator still works
     */
    public static final int SIZE = 28;
    public final int fileId;
    public final int valuePos;

    /*
     * Null KDEntry for return by compareAndSet when the expected value ends up being null
     */
    KDEntry() {
        fileId = -1;
        valuePos = -1;
    }

    KDEntry(final byte contents[]) {
        final ByteBuffer buf = ByteBuffer.wrap(contents).order(ByteOrder.nativeOrder());
        buf.position(20);
        fileId = buf.getInt();
        valuePos = buf.getInt();
    }

    public static void toBytes(ByteBuffer out, int fileId, int valuePos) {
        out.putInt(fileId);
        out.putInt(valuePos);
        assert(out.position() == out.capacity());
    }

    /*
     * Assumes the key is already decorated, the storage allocated for the key is reused e.g. update in place
     */
    public static void toBytes(byte key[], int fileId, int valuePos) {
        ByteBuffer out = ByteBuffer.wrap(key).order(ByteOrder.nativeOrder());
        out.position(20);
        out.putInt(fileId);
        out.putInt(valuePos);
        assert(out.position() == out.capacity());
    }
}