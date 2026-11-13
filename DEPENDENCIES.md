# Locked local dependencies

The unchanged public Python entry points import the exact h2 4.3.0, hpack 4.1.0 and hyperframe 6.1.0 source packages supplied beside the project. They need no network installation. requirements.txt retains the upstream pins. The packages were extracted unchanged from the following universal pure-Python wheels; these dependencies provide protocol machinery, not a task diagnosis. The original business source, contract, assertions and archived incident are unchanged.

[
  {
    "archive": "h2-4.3.0-py3-none-any.whl",
    "sha256": "c438f029a25f7945c69e0ccf0fb951dc3f73a5f6412981daee861431b70e2bdd",
    "package_files": {
      "h2/__init__.py": "b3892c18f75413875495df6372e3f129b293a230392c3f1b42001fb92b973f5f",
      "h2/config.py": "67ec2be6b162cdc076f964d5582b86b5ef32fbc71e6f9076fab95c5742f9b6e5",
      "h2/connection.py": "001ac10ddb176742575dbc6b1c77471113d885f0ce0860b65e56269adc0f9419",
      "h2/errors.py": "4be4dd61a25f3ff8709458f6b90189190e12e3fc96894ffd138e1a3f174009b1",
      "h2/events.py": "c30ec1788172d40cd35677cadf3e2ba315488a3175f79c5edc2459fb80d3ce0e",
      "h2/exceptions.py": "dc49b798b310047c58a4e2a868f74a0861469fc03dadef4006d51d2a56376d93",
      "h2/frame_buffer.py": "feff91238b08dbcea2ee81cbacb64b8d417809bfc9bfb3c48e552480be9710dd",
      "h2/py.typed": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      "h2/settings.py": "8d10648219e222250642fd7ad90de6cd64139a2d05e395269b32078dbe0cbb55",
      "h2/stream.py": "ba8608152270ee91bb8467149ff727b3fc8bec1ad6bead3b418206a49eb82e5f",
      "h2/utilities.py": "00d12d74bf69d2b304aa415842c84a40a2fcc906f70bf27314a6ba305e52046e",
      "h2/windows.py": "26958d843bfec782798c85d25a9d0834792af482203cfdbbc6bf455d8bdacc51"
    },
    "pure_python_any_platform": true
  },
  {
    "archive": "hpack-4.1.0-py3-none-any.whl",
    "sha256": "157ac792668d995c657d93111f46b4535ed114f0c9c8d672271bbec7eae1b496",
    "package_files": {
      "hpack/__init__.py": "5589bcbe1b3b87642b58c497e9efa4103b73258cc0de316076dfd54303f66208",
      "hpack/exceptions.py": "5eacbcbe2aaf28a1c1c90ec8a3c169f873e50cfdd3ed133d6a994f4b176bf632",
      "hpack/hpack.py": "06abd248e677400807e63e59457e4ca77712c71e6a7bfa3883f78ee311cc03c2",
      "hpack/huffman.py": "c6e199140a8b727c6b484b9c8ea3503a1257c5d189e4f1b58438089868bfa8e3",
      "hpack/huffman_constants.py": "e0f84e31e97998dd1ab24151ae7e2756927f09339484ed6a6c544eeaefc87f63",
      "hpack/huffman_table.py": "ddd26e52678fe84a1c7c70be50b873926053a179f594fd871ba8303ff78199b6",
      "hpack/py.typed": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      "hpack/struct.py": "3574b63bce2edb31dda248eeb5ffbd21267b875ab264b4cf13e4caa5c5a5d53e",
      "hpack/table.py": "500937f5f52433fafc76704a3832ffa86afdff044e9ca4150761033f60032642"
    },
    "pure_python_any_platform": true
  },
  {
    "archive": "hyperframe-6.1.0-py3-none-any.whl",
    "sha256": "b03380493a519fce58ea5af42e4a42317bf9bd425596f7a0835ffce80f1a42e5",
    "package_files": {
      "hyperframe/__init__.py": "43f3017642040964cbbe4b0e5e0e67607517f2303c861a10b2e8fa394436e252",
      "hyperframe/exceptions.py": "0d92eb670e72ecc2c13119310b1aea7f9121cfc0681369fb32955aa780f9bdc2",
      "hyperframe/flags.py": "f65be969e297adf61ebb18fc5deab6d440f8d0e0d38a9a376577835225f95992",
      "hyperframe/frame.py": "6ac62192190eaf5bd10777ee6581351c88532050f424d3965b067cbde7735b15",
      "hyperframe/py.typed": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    },
    "pure_python_any_platform": true
  }
]

# Upstream licenses

===== h2-4.3.0-py3-none-any.whl / h2-4.3.0.dist-info/licenses/LICENSE =====
The MIT License (MIT)

Copyright (c) 2015-2020 Cory Benfield and contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

===== hpack-4.1.0-py3-none-any.whl / hpack-4.1.0.dist-info/LICENSE =====
The MIT License (MIT)

Copyright (c) 2014 Cory Benfield

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

===== hyperframe-6.1.0-py3-none-any.whl / hyperframe-6.1.0.dist-info/LICENSE =====
The MIT License (MIT)

Copyright (c) 2014 Cory Benfield

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
