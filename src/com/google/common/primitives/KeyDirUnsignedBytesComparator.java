/*
 * Copyright (C) 2009 The Guava Authors
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

package com.google.common.primitives;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;

import sun.misc.Unsafe;
import sun.security.util.ByteArrayLexOrder;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Comparator;


/**
 * Static utility methods pertaining to {@code byte} primitives that interpret
 * values as <i>unsigned</i> (that is, any negative value {@code b} is treated
 * as the positive value {@code 256 + b}). The corresponding methods that treat
 * the values as signed are found in {@link SignedBytes}, and the methods for
 * which signedness is not an issue are in {@link Bytes}.
 *
 * <p>See the Guava User Guide article on <a href=
 * "http://code.google.com/p/guava-libraries/wiki/PrimitivesExplained">
 * primitive utilities</a>.
 *
 * @author Kevin Bourrillion
 * @author Martin Buchholz
 * @author Hiroshi Yamauchi
 * @author Louis Wasserman
 * @since 1.0
 */
public final class KeyDirUnsignedBytesComparator implements Comparator<byte[]> {
    private static final int UNSIGNED_MASK = 0xFF;

      static final boolean littleEndian =
          ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

      /*
       * The following static final fields exist for performance reasons.
       *
       * In UnsignedBytesBenchmark, accessing the following objects via static
       * final fields is the fastest (more than twice as fast as the Java
       * implementation, vs ~1.5x with non-final static fields, on x86_32)
       * under the Hotspot server compiler. The reason is obviously that the
       * non-final fields need to be reloaded inside the loop.
       *
       * And, no, defining (final or not) local variables out of the loop still
       * isn't as good because the null check on the theUnsafe object remains
       * inside the loop and BYTE_ARRAY_BASE_OFFSET doesn't get
       * constant-folded.
       *
       * The compiler can treat static final fields as compile-time constants
       * and can constant-fold them while (final or not) local variables are
       * run time values.
       */

      static final Unsafe theUnsafe;

      /** The offset to the first element in a byte array. */
      static final int BYTE_ARRAY_BASE_OFFSET;

      static {
        theUnsafe = (Unsafe) AccessController.doPrivileged(
            new PrivilegedAction<Object>() {
              @Override
              public Object run() {
                try {
                  Field f = Unsafe.class.getDeclaredField("theUnsafe");
                  f.setAccessible(true);
                  return f.get(null);
                } catch (NoSuchFieldException e) {
                  // It doesn't matter what we throw;
                  // it's swallowed in getBestComparator().
                  throw new Error();
                } catch (IllegalAccessException e) {
                  throw new Error();
                }
              }
            });

        BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

        // sanity check - this should never fail
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
          throw new AssertionError();
        }
      }

      @Override public int compare(byte[] left, byte[] right) {
        long lw1 = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET);
        long rw1 = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET);

        assert(left.length == right.length && left.length == 37);
        long diff1 = lw1 ^ rw1;

        if (diff1 != 0) {
            if (!littleEndian) {
              return UnsignedLongs.compare(lw1, rw1);
            }

            // Use binary search
            int n = 0;
            int y;
            int x = (int) diff1;
            if (x == 0) {
              x = (int) (diff1 >>> 32);
              n = 32;
            }

            y = x << 16;
            if (y == 0) {
              n += 16;
            } else {
              x = y;
            }

            y = x << 8;
            if (y == 0) {
              n += 8;
            }
            return (int) (((lw1 >>> n) & UNSIGNED_MASK) - ((rw1 >>> n) & UNSIGNED_MASK));
          }

        long lw2 = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET + 8);
        long rw2 = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET + 8);

        long diff2 = lw2 ^ rw2;

        if (diff2 != 0) {
            if (!littleEndian) {
              return UnsignedLongs.compare(lw2, rw2);
            }

            // Use binary search
            int n = 0;
            int y;
            int x = (int) diff2;
            if (x == 0) {
              x = (int) (diff2 >>> 32);
              n = 32;
            }

            y = x << 16;
            if (y == 0) {
              n += 16;
            } else {
              x = y;
            }

            y = x << 8;
            if (y == 0) {
              n += 8;
            }
            return (int) (((lw2 >>> n) & UNSIGNED_MASK) - ((rw2 >>> n) & UNSIGNED_MASK));
          }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = 16; i < 20; i++) {
          int result = UnsignedBytes.compare(left[i], right[i]);
          if (result != 0) {
            return result;
          }
        }
        return 0;
      }

    /*
     * Verify that changes to the last 17 bytes don't effect comparison
     */
    public static void main(String args[]) throws Exception {
        Comparator<byte[]> c = new KeyDirUnsignedBytesComparator();
        Comparator<byte[]> d = new ByteArrayLexOrder();
        java.util.Random r = new java.util.Random();

        long count = 0;
        while (true) {
            byte a[] = new byte[20];
            byte b[] = new byte[20];
            r.nextBytes(a);
            b = Arrays.copyOf(a, a.length);
            if (r.nextInt() % 1 == 0) {
                a[r.nextInt(a.length)] = (byte)r.nextInt(128);
            } else {
                b[r.nextInt(a.length)] = (byte)r.nextInt(128);
            }
            if (c.compare(a, b) != d.compare(a, b)) {
                StringBuilder sb = new StringBuilder();
                for (byte bi : a) {
                    sb.append(bi).append(',');
                }
                sb.append('\n');
                for (byte bi : b) {
                    sb.append(bi).append(',');
                }
                System.out.println("Comparison failed \n" + sb);
                return;
            }
            if (c.compare(a, a) != 0) {
                StringBuilder sb = new StringBuilder();
                for (byte bi : a) {
                    sb.append(bi).append(',');
                }
                System.out.println("Equality failed " + sb);
            }
            if (c.compare(b,  b) != 0) {
                StringBuilder sb = new StringBuilder();
                for (byte bi : b) {
                    sb.append(bi).append(',');
                }
                System.out.println("Equality failed " + sb);
            }
            count++;
            if (count % 500000000 == 0) {
                System.out.println("Did " + count);
            }
        }
    }
}
