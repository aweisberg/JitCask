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

import java.io.IOException;
import com.google.common.util.concurrent.*;

public interface JitCask {

    public void open() throws IOException;

    public ListenableFuture<byte[]> get(byte key[]);

    public ListenableFuture<?> put(byte key[], byte value[]);
    public ListenableFuture<?> put(byte key[], byte value[], boolean compressValue);

    public ListenableFuture<?> remove(byte key[]);

    public void close() throws IOException;

    /*
     * Make all writes submitted prior to invocation of sync durable
     */
    public void sync() throws IOException;
}
