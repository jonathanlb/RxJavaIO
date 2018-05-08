# RxJavaIO
Utilities to bridge RxJava to non-reactive or pull-based systems, for example `java.io.InputStream` for publishing JSON to JAX-RS REST frameworks.

## Dependencies
<dl>
  <dt>Java 6</dt>
  <dd>Keep the dependencies light and accessible to projects unable/unwilling to update.</dd>

  <dt>RxJava-2</dt>
  <dd>Build upon reactive-streams.</dd>
</dl>

## Build
```sh
$ git clone https://github.com/jonathanlb/RxJavaIO
$ cd RxJavaIO
$ mvn install
```

## Todo
- Support Java NIO.

## License
    Copyright (c) 2018-present.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

