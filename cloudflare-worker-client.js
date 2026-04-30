export default {
  async fetch(request, env) {
    const cache = new GASCache(env.GAS_CACHE_URL, { 
      apiKey: env.GAS_API_KEY,
      partition: 'prod',
      maxRetries: 3,
      timeout: 8000
    });
    
    const setResult = await cache.set('test', 'x'.repeat(150000));
    if (!setResult.ok) {
      return new Response(JSON.stringify({ error: setResult.error }), { status: 500 });
    }
    
    const getResult = await cache.get('test');
    if (!getResult.ok) {
      return new Response(JSON.stringify({ error: getResult.error }), { status: 500 });
    }
    
    return new Response(JSON.stringify({ 
      val: getResult.data?.slice(0, 100),
      hit: getResult.hit,
      stats: cache.stats 
    }));
  }
};

class GASCache {
  constructor(url, opts = {}) {
    this.url = url;
    this.apiKey = opts.apiKey;
    this.partition = opts.partition || 'default';
    this.maxRetries = opts.maxRetries || 2;
    this.timeout = opts.timeout || 5000;
    this.stats = { calls: 0, errors: 0, retries: 0, authErrors: 0 };
    
    if (!this.apiKey) {
      throw new Error('API key required');
    }
  }
  
  async #call(action, params = {}, retryCount = 0) {
    this.stats.calls++;
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);
    
    let res;
    try {
      res = await fetch(this.url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          apiKey: this.apiKey,
          action, 
          partition: this.partition,
          ...params 
        }),
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
    } catch (err) {
      clearTimeout(timeoutId);
      this.stats.errors++;
      
      if (retryCount < this.maxRetries && this.#isRetryable(err)) {
        this.stats.retries++;
        const delay = Math.min(1000 * Math.pow(2, retryCount), 5000);
        await new Promise(r => setTimeout(r, delay));
        return this.#call(action, params, retryCount + 1);
      }
      
      return { ok: false, error: `Network error: ${err.message}` };
    }
    
    if (!res.ok) {
      this.stats.errors++;
      
      if (retryCount < this.maxRetries && res.status >= 500) {
        this.stats.retries++;
        const delay = Math.min(1000 * Math.pow(2, retryCount), 5000);
        await new Promise(r => setTimeout(r, delay));
        return this.#call(action, params, retryCount + 1);
      }
      
      return { ok: false, error: `HTTP ${res.status}` };
    }
    
    let data;
    try {
      data = await res.json();
    } catch (err) {
      this.stats.errors++;
      return { ok: false, error: 'Invalid JSON response' };
    }
    
    // Track auth errors separately
    if (!data.ok && (data.error.includes('API key') || data.error.includes('Rate limit'))) {
      this.stats.authErrors++;
    }
    
    return data;
  }
  
  #isRetryable(err) {
    return err.name === 'AbortError' || 
           err.message.includes('network');
  }
  
  async get(key) {
    return this.#call('get', { key });
  }
  
  async set(key, value, ttl) {
    return this.#call('set', { key, value, ttl });
  }
  
  async delete(key) {
    return this.#call('delete', { key });
  }
  
  async getAll(keys) {
    return this.#call('getAll', { key: keys });
  }
  
  async putAll(keyValuePairs, ttl) {
    return this.#call('putAll', { value: keyValuePairs, ttl });
  }
  
  async removeAll(keys) {
    return this.#call('removeAll', { key: keys });
  }
  
  async clear() {
    return this.#call('clear');
  }
  
  async batchGet(keys, batchSize = 50) {
    const results = {};
    const errors = [];
    
    for (let i = 0; i < keys.length; i += batchSize) {
      const batch = keys.slice(i, i + batchSize);
      const result = await this.getAll(batch);
      
      if (!result.ok) {
        errors.push({ batch: i / batchSize, error: result.error });
        continue;
      }
      
      Object.assign(results, result.data);
      
      if (result.errors) {
        errors.push(...result.errors);
      }
    }
    
    return { 
      ok: true, 
      data: results,
      errors: errors.length > 0 ? errors : undefined
    };
  }
  
  async batchSet(keyValuePairs, ttl, batchSize = 50) {
    const entries = Object.entries(keyValuePairs);
    const errors = [];
    let successCount = 0;
    
    for (let i = 0; i < entries.length; i += batchSize) {
      const batch = Object.fromEntries(entries.slice(i, i + batchSize));
      const result = await this.putAll(batch, ttl);
      
      if (!result.ok) {
        errors.push({ batch: i / batchSize, error: result.error });
        continue;
      }
      
      successCount += result.keys || 0;
      
      if (result.errors) {
        errors.push(...result.errors);
      }
    }
    
    return { 
      ok: true, 
      keys: successCount,
      errors: errors.length > 0 ? errors : undefined
    };
  }
}
