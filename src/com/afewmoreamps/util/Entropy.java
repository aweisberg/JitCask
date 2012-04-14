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
package com.afewmoreamps.util;

import java.nio.ByteBuffer;

public class Entropy {

    private final ByteBuffer m_entropy = ByteBuffer.allocate(1024 * 1024 * 4);


    public Entropy(int entropy, int seed) {
        if (entropy < 1 || entropy > 127) {
            throw new IllegalArgumentException();
        }

        java.util.Random r = new java.util.Random(seed);

        while (m_entropy.hasRemaining()) {
            m_entropy.put((byte)r.nextInt(entropy));
        }
    }

    public byte[] get(int amount) {
        if (amount > 1024 * 1024 * 4) {
            throw new IllegalArgumentException();
        }
        byte out[] = new byte[amount];
        if (m_entropy.remaining() < amount) {
            m_entropy.clear();
        }
        m_entropy.get(out);
        return out;
    }
}
