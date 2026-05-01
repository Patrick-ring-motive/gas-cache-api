const API_KEYS = {
  'prod_key_abc123': {
    name: 'Production',
    partition: 'prod',
    rateLimit: 1000
  },
  'dev_key_xyz789': {
    name: 'Development',
    partition: 'dev',
    rateLimit: 100
  },
  'test_key_def456': {
    name: 'Testing',
    partition: 'test',
    rateLimit: 50
  }
};

function doPost(e) {
  const startTime = Date.now();
  let logData = {
    timestamp: new Date().toISOString()
  };

  if (!e.postData || !e.postData.contents) {
    return jsonResponse({
      ok: false,
      error: 'No POST data'
    });
  }

  let parsed;
  try {
    parsed = JSON.parse(e.postData.contents);
  } catch (err) {
    return jsonResponse({
      ok: false,
      error: 'Invalid JSON'
    });
  }

  // Auth check
  const authResult = verifyApiKey(parsed.apiKey, e);
  if (!authResult.ok) {
    logData.auth = false;
    logData.error = authResult.error;
    logData.duration = Date.now() - startTime;
    Logger.log(JSON.stringify(logData));
    return jsonResponse(authResult);
  }

  const keyInfo = authResult.keyInfo;
  logData.keyName = keyInfo.name;
  logData.auth = true;

  // Rate limit check
  const rateLimitResult = checkRateLimit(parsed.apiKey, keyInfo.rateLimit);
  if (!rateLimitResult.ok) {
    logData.rateLimited = true;
    logData.error = rateLimitResult.error;
    logData.duration = Date.now() - startTime;
    Logger.log(JSON.stringify(logData));
    return jsonResponse(rateLimitResult);
  }

  const {
    action,
    key,
    value,
    ttl,
    partition
  } = parsed;

  // Override partition from key config if not specified
  const finalPartition = partition || keyInfo.partition || 'default';
  logData.action = action;
  logData.partition = finalPartition;

  const cache = CacheService.getScriptCache();
  const partKey = (k) => `${finalPartition}:${k}`;

  let result;

  switch (action) {
    case 'get':
      if (!key) {
        result = {
          ok: false,
          error: 'key required'
        };
        break;
      }
      const val = cache.get(partKey(key));
      result = {
        ok: true,
        data: val,
        hit: !!val
      };
      logData.keyCount = 1;
      logData.hit = !!val;
      break;

    case 'set':
      if (!key || value === undefined) {
        result = {
          ok: false,
          error: 'key and value required'
        };
        break;
      }
      const finalTtl = Math.min(ttl || 21600, 21600);

      const chunks = chunkValue(value);
      if (chunks.ok === false) {
        result = chunks;
        break;
      }

      const writeResult = writeChunked(cache, partKey(key), chunks.data, finalTtl);
      if (!writeResult.ok) {
        result = writeResult;
        break;
      }

      result = {
        ok: true,
        chunked: chunks.data.length > 1,
        chunks: chunks.data.length
      };
      logData.bytes = JSON.stringify(value).length;
      logData.chunks = chunks.data.length;
      break;

    case 'delete':
      if (!key) {
        result = {
          ok: false,
          error: 'key required'
        };
        break;
      }

      const delResult = deleteChunked(cache, partKey(key));
      result = delResult;
      logData.keyCount = 1;
      logData.chunksDeleted = delResult.chunksDeleted || 0;
      break;

    case 'getAll':
      if (!Array.isArray(key)) {
        result = {
          ok: false,
          error: 'key must be array'
        };
        break;
      }

      const keys = key.map(k => partKey(k));
      const values = cache.getAll(keys);

      const reconstructed = {};
      let reconstructErrors = [];

      for (const [pk, val] of Object.entries(values)) {
        const origKey = pk.replace(`${finalPartition}:`, '');
        const reconResult = reconstructValue(val, origKey, cache, finalPartition);

        if (reconResult.ok) {
          reconstructed[origKey] = reconResult.data;
        } else {
          reconstructErrors.push({
            key: origKey,
            error: reconResult.error
          });
        }
      }

      result = {
        ok: true,
        data: reconstructed,
        errors: reconstructErrors.length > 0 ? reconstructErrors : undefined
      };
      logData.keyCount = key.length;
      logData.hits = Object.keys(reconstructed).length;
      break;

    case 'putAll':
      if (typeof value !== 'object') {
        result = {
          ok: false,
          error: 'value must be object'
        };
        break;
      }

      const finalTtl2 = Math.min(ttl || 21600, 21600);
      const pairs = {};
      let totalBytes = 0;
      let chunkErrors = [];

      for (const [k, v] of Object.entries(value)) {
        const chunks = chunkValue(v);
        if (!chunks.ok) {
          chunkErrors.push({
            key: k,
            error: chunks.error
          });
          continue;
        }

        totalBytes += JSON.stringify(v).length;

        if (chunks.data.length === 1) {
          pairs[partKey(k)] = chunks.data[0];
        } else {
          pairs[partKey(k)] = JSON.stringify({
            chunks: chunks.data.length
          });
          chunks.data.forEach((chunk, i) => {
            pairs[partKey(`${k}:chunk:${i}`)] = chunk;
          });
        }
      }

      try {
        cache.putAll(pairs, finalTtl2);
        result = {
          ok: true,
          keys: Object.keys(value).length - chunkErrors.length,
          errors: chunkErrors.length > 0 ? chunkErrors : undefined
        };
        logData.keyCount = Object.keys(value).length;
        logData.bytes = totalBytes;
      } catch (err) {
        result = {
          ok: false,
          error: err.message
        };
      }
      break;

    case 'removeAll':
      if (!Array.isArray(key)) {
        result = {
          ok: false,
          error: 'key must be array'
        };
        break;
      }

      const keysToRemove = [];
      for (const k of key) {
        const metaVal = cache.get(partKey(k));
        try {
          const meta = JSON.parse(metaVal);
          if (meta.chunks) {
            keysToRemove.push(partKey(k));
            for (let i = 0; i < meta.chunks; i++) {
              keysToRemove.push(partKey(`${k}:chunk:${i}`));
            }
          } else {
            keysToRemove.push(partKey(k));
          }
        } catch {
          keysToRemove.push(partKey(k));
        }
      }

      try {
        cache.removeAll(keysToRemove);
        result = {
          ok: true
        };
        logData.keyCount = key.length;
      } catch (err) {
        result = {
          ok: false,
          error: err.message
        };
      }
      break;

    case 'clear':
      const allKeys = listPartitionKeys(cache, finalPartition);
      if (allKeys.length > 0) {
        try {
          cache.removeAll(allKeys);
          result = {
            ok: true,
            keysCleared: allKeys.length
          };
          logData.keysCleared = allKeys.length;
        } catch (err) {
          result = {
            ok: false,
            error: err.message
          };
        }
      } else {
        result = {
          ok: true,
          keysCleared: 0
        };
      }
      break;

    default:
      result = {
        ok: false,
        error: `Unknown action: ${action}`
      };
  }

  logData.success = result.ok;
  logData.duration = Date.now() - startTime;
  if (!result.ok) logData.error = result.error;

  Logger.log(JSON.stringify(logData));

  return jsonResponse(result);
}

function verifyApiKey(apiKey, e) {
  if (!apiKey) {
    return {
      ok: false,
      error: 'API key required'
    };
  }

  const keyInfo = API_KEYS[apiKey];
  if (!keyInfo) {
    return {
      ok: false,
      error: 'Invalid API key'
    };
  }

  // Optional: IP whitelist check
  // const clientIp = e.parameter?.userip || 'unknown';
  // if (keyInfo.allowedIps && !keyInfo.allowedIps.includes(clientIp)) {
  //   return { ok: false, error: 'IP not allowed' };
  // }

  return {
    ok: true,
    keyInfo
  };
}

function checkRateLimit(apiKey, limit) {
  const cache = CacheService.getScriptCache();
  const rateLimitKey = `__ratelimit:${apiKey}:${getHourKey()}`;

  const current = cache.get(rateLimitKey);
  const count = current ? parseInt(current) : 0;

  if (count >= limit) {
    return {
      ok: false,
      error: 'Rate limit exceeded',
      limit,
      current: count
    };
  }

  cache.put(rateLimitKey, (count + 1).toString(), 3600);

  return {
    ok: true,
    remaining: limit - count - 1
  };
}

function getHourKey() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${now.getUTCMonth()}-${now.getUTCDate()}-${now.getUTCHours()}`;
}

function jsonResponse(data) {
  return ContentService.createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}

function chunkValue(value) {
  try {
    const str = typeof value === 'string' ? value : JSON.stringify(value);
    const CHUNK_SIZE = 90000;

    if (str.length <= CHUNK_SIZE) {
      return {
        ok: true,
        data: [str]
      };
    }

    const chunks = [];
    for (let i = 0; i < str.length; i += CHUNK_SIZE) {
      chunks.push(str.slice(i, i + CHUNK_SIZE));
    }
    return {
      ok: true,
      data: chunks
    };
  } catch (err) {
    return {
      ok: false,
      error: `Chunk failed: ${err.message}`
    };
  }
}

function writeChunked(cache, key, chunks, ttl) {
  try {
    if (chunks.length === 1) {
      cache.put(key, chunks[0], ttl);
    } else {
      const meta = {
        chunks: chunks.length
      };
      cache.put(key, JSON.stringify(meta), ttl);
      chunks.forEach((chunk, i) => {
        cache.put(`${key}:chunk:${i}`, chunk, ttl);
      });
    }
    return {
      ok: true
    };
  } catch (err) {
    return {
      ok: false,
      error: `Write failed: ${err.message}`
    };
  }
}

function deleteChunked(cache, key) {
  try {
    const metaVal = cache.get(key);
    if (!metaVal) {
      cache.remove(key);
      return {
        ok: true,
        chunksDeleted: 0
      };
    }

    try {
      const meta = JSON.parse(metaVal);
      if (meta.chunks) {
        const toRemove = [key];
        for (let i = 0; i < meta.chunks; i++) {
          toRemove.push(`${key}:chunk:${i}`);
        }
        cache.removeAll(toRemove);
        return {
          ok: true,
          chunksDeleted: meta.chunks
        };
      }
    } catch {
      // Not chunked metadata
    }

    cache.remove(key);
    return {
      ok: true,
      chunksDeleted: 0
    };
  } catch (err) {
    return {
      ok: false,
      error: `Delete failed: ${err.message}`
    };
  }
}

function reconstructValue(metaVal, key, cache, partition) {
  if (!metaVal) return {
    ok: true,
    data: null
  };

  try {
    const meta = JSON.parse(metaVal);
    if (!meta.chunks) return {
      ok: true,
      data: metaVal
    };

    const chunkKeys = Array.from({
        length: meta.chunks
      }, (_, i) =>
      `${partition}:${key}:chunk:${i}`
    );
    const chunkVals = cache.getAll(chunkKeys);

    let reconstructed = '';
    for (let i = 0; i < meta.chunks; i++) {
      const chunk = chunkVals[`${partition}:${key}:chunk:${i}`];
      if (!chunk) {
        return {
          ok: false,
          error: `Missing chunk ${i}`
        };
      }
      reconstructed += chunk;
    }

    return {
      ok: true,
      data: reconstructed
    };
  } catch (err) {
    return {
      ok: true,
      data: metaVal
    };
  }
}

function listPartitionKeys(cache, partition) {
  const tracker = cache.get(`__partition:${partition}:keys`);
  if (!tracker) return [];

  try {
    return JSON.parse(tracker);
  } catch {
    return [];
  }
}
