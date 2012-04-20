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

import com.google.common.util.concurrent.AbstractFuture;

public class SettableFuture<V> extends AbstractFuture<V> {
    public static <V> SettableFuture<V> create() {
        return new SettableFuture<V>();
    }

    private SettableFuture() {}

    @Override
    public boolean set(V value) {
        return super.set(value);
    }

    @Override
    public boolean setException(Throwable t) {
        return super.setException(t);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

}
