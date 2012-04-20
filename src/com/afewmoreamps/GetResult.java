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
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFutureTask;

public class GetResult extends OpResult {
    private final ListenableFutureTask<byte[]> value;
    public final byte compressedValue[];
    public final byte key[];

    public GetResult(byte key[], byte compressedValue[], ListenableFutureTask<byte[]> value, int latency) {
        super(latency);
        this.key = key;
        this.compressedValue = compressedValue;
        this.value = value;
    }

    public byte[] value() throws IOException {
        value.run();
        try {
            return value.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        } catch (Exception e) {
            //Dont expect to do this.
            throw new IOException(e);
        }
    }
}
