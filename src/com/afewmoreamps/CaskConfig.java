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

import java.io.File;

public class CaskConfig {
    final File caskPath;
    public int syncInterval = 100;
    public boolean compressByDefault = true;
    public boolean syncByDefault = true;
    public int maxValidValueSize = 1024 * 1024 * 100;

    public CaskConfig(File caskPath) {
        this.caskPath = caskPath;
    }
}
