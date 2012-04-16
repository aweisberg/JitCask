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

import static org.junit.Assert.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import sun.security.util.ByteArrayLexOrder;

import com.afewmoreamps.util.Entropy;
import com.google.common.util.concurrent.ListenableFuture;

public class JitCaskImplTest {

    private final File tempPath = new File("/tmp", System.getProperty("user.name"));

    @Before
    public void setUp() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(tempPath);
        tempPath.mkdirs();
    }

    @Test
    public void testPut() throws Exception {
        JitCaskImpl jc = new JitCaskImpl(tempPath, true);

        jc.open();

        final int keyCount = 10000;
        final int seed = 42;
        final int entropyQuality = 5;
        final int keySize = 64;
        final int valueSize = 2048;

        Random r = new Random(seed);

        Set<byte[]> keySet = new TreeSet<byte[]>(new ByteArrayLexOrder());
        List<byte[]> keys = new ArrayList<byte[]>(keyCount);
        List<byte[]> values = new ArrayList<byte[]>(keyCount);

        Entropy entropy = new Entropy(entropyQuality, seed);
        ArrayList<ListenableFuture<?>> results = new ArrayList<ListenableFuture<?>>(keyCount);
        for (int ii = 0; ii < keyCount; ii++) {
            byte key[] = new byte[keySize];
            do {
                r.nextBytes(key);
            }
            while (!keySet.add(key));
            keys.add(key);
            byte value[] = entropy.get(valueSize);
            values.add(value);
            results.add(jc.put(key, value));
        }

        for (Future<?> f : results) {
            f.get();
        }

        int zz = 0;
        for (CaskEntry entry : jc) {
            byte expectedKey[] = keys.get(zz);
            byte expectedValue[] = values.get(zz++);
            assertTrue(Arrays.equals(expectedKey, entry.key));
            assertTrue(Arrays.equals(expectedValue, entry.getValueUncompressed()));
        }

        zz = 0;
        for (int ii = 0; ii < keyCount; ii++) {
            byte expectedKey[] = keys.get(zz);
            byte expectedValue[] = values.get(zz++);
            assertTrue(Arrays.equals(expectedValue, jc.get(expectedKey).get()));
        }

        results.clear();
        zz = 0;
        for (int ii = 0; ii < keyCount; ii++) {
            byte expectedKey[] = keys.get(zz++);
            if (ii % 2 == 0) {
                continue;
            }
            results.add(jc.remove(expectedKey));
        }

        for (Future<?> f : results) {
            f.get();
        }

        zz = 0;
        for (int ii = 0; ii < keyCount; ii++) {
            byte expectedKey[] = keys.get(zz);
            byte expectedValue[] = values.get(zz++);
            if (ii % 2 == 0) {
                assertTrue(Arrays.equals(expectedValue, jc.get(expectedKey).get()));
            } else {
                assertNull(jc.get(expectedKey).get());
            }
        }

        jc.close();
        jc = new JitCaskImpl(tempPath, true);
        jc.open();

        zz = 0;
        for (int ii = 0; ii < keyCount; ii++) {
            byte expectedKey[] = keys.get(zz);
            byte expectedValue[] = values.get(zz++);
            if (ii % 2 == 0) {
                try {
                    byte value[] = jc.get(expectedKey).get();
                    boolean equals = Arrays.equals(expectedValue, value);
                    if (!equals) {
                        System.out.println(ii);
                    }
                } catch (ExecutionException e) {
                    System.out.println(ii);
                    throw e;
                }
                assertTrue(Arrays.equals(expectedValue, jc.get(expectedKey).get()));
            } else {
                assertNull(jc.get(expectedKey).get());
            }
        }

        long start = System.currentTimeMillis();
        int keyIndex = 0;
        for (; keyIndex < 20000000; keyIndex++) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
            keyBuffer.putInt(keyIndex);
            byte key[] = keyBuffer.array();
            byte value[] = entropy.get(valueSize);
            jc.put(key, value);
        }
        System.out.println("Loaded 20 million keys in " + ((System.currentTimeMillis() - start) / 1000));

        int keysRetrieved = 0;
        start = System.currentTimeMillis();
        while (true) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
            keyBuffer.putInt(r.nextInt(20000000));
            byte key[] = keyBuffer.array();
            assertNotNull(jc.get(key));
            keysRetrieved++;
            if (keysRetrieved % 100000 == 0) {
                System.out.println("Retrieved " + keysRetrieved + " in " + ((System.currentTimeMillis() - start) / 1000));
            }
        }

//        long start = System.currentTimeMillis();
//        int keyIndex = 0;
//        while (true) {
//            ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
//            keyBuffer.putInt(keyIndex++);
//            byte key[] = keyBuffer.array();
//            jc.put(key, new byte[0]);
//            if (keyIndex % 1000000 == 0) {
//                System.out.println("Inserted " + (keyIndex / 1000000) + " million 64 byte keys in " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
//            }
//        }
    }

}
