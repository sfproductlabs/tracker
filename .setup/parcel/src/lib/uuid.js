import { v1 as uuidv1, validate } from "uuid";

function uuidToBase64(uuid) {
  // Remove dashes and convert UUID to binary
  const hex = uuid.replace(/-/g, '');
  let binary = '';
  for (let i = 0; i < hex.length; i += 2) {
      binary += String.fromCharCode(parseInt(hex.substr(i, 2), 16));
  }

  // Convert binary data to Base64
  return btoa(binary)
      .replace(/\+/g, '-')  // URL-safe character replacements
      .replace(/\//g, '_')
      .replace(/=+$/, '');  // Remove padding
}

function base64ToUuid(base64) {
  // Replace URL-safe characters back to Base64 equivalents
  base64 = base64.replace(/-/g, '+').replace(/_/g, '/');

  // Add Base64 padding if necessary
  base64 = base64.padEnd(base64.length + (4 - base64.length % 4) % 4, '=');
  
  // Decode Base64 to binary
  const binary = atob(base64);
  let hex = '';

  // Convert binary to hexadecimal
  for (let i = 0; i < binary.length; i++) {
      let hexPart = binary.charCodeAt(i).toString(16);
      hex += hexPart.padStart(2, '0');
  }

  // Format as UUID
  return hex.substr(0, 8) + '-' + hex.substr(8, 4) + '-' + hex.substr(12, 4) + '-' + hex.substr(16, 4) + '-' + hex.substr(20);
}

function uuidToBinaryArray(uuid) {
  // Remove hyphens and convert UUID to an array of byte values
  const hex = uuid.replace(/-/g, '');
  const byteArray = new Uint8Array(hex.length / 2);

  for (let i = 0, j = 0; i < hex.length; i += 2, j++) {
      byteArray[j] = parseInt(hex.substr(i, 2), 16);
  }

  return byteArray;
}

function binaryArrayToUuid(binaryArray) {
  let hex = '';
  for (let i = 0; i < binaryArray.length; i++) {
      hex += binaryArray[i].toString(16).padStart(2, '0');
  }
  return hex.substr(0, 8) + '-' + hex.substr(8, 4) + '-' + hex.substr(12, 4) + '-' + hex.substr(16, 4) + '-' + hex.substr(20);
}

//Test:
//uuid = '123e4567-e89b-12d3-a456-426614174000';
// buffer = uuidToBinaryArray(uuid);
// uuid = binaryArrayToUuid(buffer);
// console.debug(uuid)
//uuidToBase64(uuid)

/**
 * Adds the dashes into a UUID hex string.
 * Example:
 * "6ec0bd7f11c043da975e2a8ad9ebae0b" -> "6ec0bd7f-11c0-43da-975e-2a8ad9ebae0b"
 *    
 * @param {string} uuidHexStr UUID hex string (without dashes)
 * @returns {string} UUID string with dashes
 */
export function uuidHexStrToUUIDStr(uuidHexStr) {
  const a = Array.from(uuidHexStr);
  const b = a.map((x, idx) => {
    return (idx === 8 || idx === 12 || idx === 16 || idx === 20) ? "-".concat(x) : x;
  });
  return b.join('');
}

export function uuidToTime (uuid_str) {
  var uuid_arr = uuid_str.split( '-' ),
      time_str = [
          uuid_arr[ 2 ].substring( 1 ),
          uuid_arr[ 1 ],
          uuid_arr[ 0 ]
      ].join( '' );
  return parseInt( time_str, 16 );
};

export function uuidToDate (uuid_str) {
  var int_time = uuidToTime( uuid_str ) - 122192928000000000,
      int_millisec = Math.floor( int_time / 10000 );
  return new Date( int_millisec );
};

export function uuidHexToDate(uuidHexStr) {
  return uuidToDate(uuidHexStrToUUIDStr(uuidHexStr));
}

export const uuidOrHexToDate = (uuid_str) => {
  return validate(uuid_str) ? uuidToDate(uuid_str) : uuidHexToDate(uuid_str);
}

export function uuid() {
  return uuidv1().replace(/-/g, '');
}


export class UUIDHEX {
  constructor(uuid=uuidv1()) {
      this.hex = uuid.replace(/-/g, '');      
      this.uuid = this.original_str(this.hex);
  }

  toString() {
      return this.hex;
  }

  original_str() {
      let hex = this.hex;
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;

  }

  equals(other) {
      if (other instanceof UUIDHEX) {
          return this.hex === other.hex;
      }
      return false;
  }

  hashCode() {
      let hash = 0;
      for (let i = 0; i < this.hex.length; i++) {
          let char = this.hex.charCodeAt(i);
          hash = (hash << 5) - hash + char;
          hash |= 0; // Convert to 32bit integer
      }
      return hash;
  }

}