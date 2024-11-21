let wasm = undefined;
function loadWasmModuleSynchronously() {
  const xhr = new XMLHttpRequest();
  xhr.open('GET', "/v1/pub/lz4.wasm", false); // false makes it synchronous
  xhr.overrideMimeType('text/plain; charset=x-user-defined');
  xhr.send();

  if (xhr.status >= 200 && xhr.status < 300) {
    return xhr.response;
  } else {
    throw new Error(`Failed to fetch WASM module: ${xhr.statusText}`);
  }
}

function binaryStringToArrayBuffer(binaryString) {
  const length = binaryString.length;
  const buffer = new ArrayBuffer(length);
  const view = new Uint8Array(buffer);

  for (let i = 0; i < length; i++) {
    view[i] = binaryString.charCodeAt(i) & 0xFF;
  }

  return buffer;
}


var response =  binaryStringToArrayBuffer(loadWasmModuleSynchronously())


export async function compress(bytes, pack = true) {

  const lTextDecoder = typeof TextDecoder === 'undefined' ? (0, module.require)('util').TextDecoder : TextDecoder;
  let cachedTextDecoder = new lTextDecoder('utf-8', { ignoreBOM: true, fatal: true });
  cachedTextDecoder.decode();
  let cachegetUint8Memory0 = null;
  function getUint8Memory0() {
    if (cachegetUint8Memory0 === null || cachegetUint8Memory0.buffer !== wasm.instance.exports.memory.buffer) {
      cachegetUint8Memory0 = new Uint8Array(wasm.instance.exports.memory.buffer);
    }
    return cachegetUint8Memory0;
  }

  function getStringFromWasm0(ptr, len) {
    return cachedTextDecoder.decode(getUint8Memory0().subarray(ptr, ptr + len));
  }

  const heap = new Array(32).fill(undefined);

  heap.push(undefined, null, true, false);

  let heap_next = heap.length;

  function addHeapObject(obj) {
    if (heap_next === heap.length) heap.push(heap.length + 1);
    const idx = heap_next;
    heap_next = heap[idx];

    heap[idx] = obj;
    return idx;
  }

  let WASM_VECTOR_LEN = 0;

  function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1);
    getUint8Memory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
  }

  let cachegetInt32Memory0 = null;
  function getInt32Memory0() {
    if (cachegetInt32Memory0 === null || cachegetInt32Memory0.buffer !== wasm.instance.exports.memory.buffer) {
      cachegetInt32Memory0 = new Int32Array(wasm.instance.exports.memory.buffer);
    }
    return cachegetInt32Memory0;
  }

  function getArrayU8FromWasm0(ptr, len) {
    return getUint8Memory0().subarray(ptr / 1, ptr / 1 + len);
  }
  /**
  * @param {Uint8Array} input
  * @returns {Uint8Array}
  */
  function compress(input) {
    try {
      const retptr = wasm.instance.exports.__wbindgen_add_to_stack_pointer(-16);
      const ptr0 = passArray8ToWasm0(input, wasm.instance.exports.__wbindgen_malloc);
      const len0 = WASM_VECTOR_LEN;
      wasm.instance.exports.compress(retptr, ptr0, len0);
      var r0 = getInt32Memory0()[retptr / 4 + 0];
      var r1 = getInt32Memory0()[retptr / 4 + 1];
      var v1 = getArrayU8FromWasm0(r0, r1).slice();
      wasm.instance.exports.__wbindgen_free(r0, r1 * 1);
      return v1;
    } finally {
      wasm.instance.exports.__wbindgen_add_to_stack_pointer(16);
    }
  }

  function getObject(idx) { return heap[idx]; }

  function dropObject(idx) {
    if (idx < 36) return;
    heap[idx] = heap_next;
    heap_next = idx;
  }

  function takeObject(idx) {
    const ret = getObject(idx);
    dropObject(idx);
    return ret;
  }
  /**
  * @param {Uint8Array} input
  * @returns {Uint8Array}
  */
  function decompress(input) {
    try {
      const retptr = wasm.instance.exports.__wbindgen_add_to_stack_pointer(-16);
      const ptr0 = passArray8ToWasm0(input, wasm.instance.exports.__wbindgen_malloc);
      const len0 = WASM_VECTOR_LEN;
      wasm.instance.exports.decompress(retptr, ptr0, len0);
      var r0 = getInt32Memory0()[retptr / 4 + 0];
      var r1 = getInt32Memory0()[retptr / 4 + 1];
      var r2 = getInt32Memory0()[retptr / 4 + 2];
      var r3 = getInt32Memory0()[retptr / 4 + 3];
      if (r3) {
        throw takeObject(r2);
      }
      var v1 = getArrayU8FromWasm0(r0, r1).slice();
      wasm.instance.exports.__wbindgen_free(r0, r1 * 1);
      return v1;
    } finally {
      wasm.instance.exports.__wbindgen_add_to_stack_pointer(16);
    }
  }

  function __wbindgen_string_new(arg0, arg1) {
    const ret = getStringFromWasm0(arg0, arg1);
    return addHeapObject(ret);
  };

  if (!wasm) {
    wasm = await WebAssembly.instantiate(response, {
      './lz4_wasm_bg.js': {
        __wbindgen_string_new,
        compress,
        decompress
      },
    })
  }

  if (!pack) return decompress(bytes)
  return compress(bytes)
  //Example:
  //(new TextDecoder("utf-8")).decode(decompress(compress((new TextEncoder()).encode("compress this text, compress this text pls. thx. thx. thx. thx. thx"))))

}