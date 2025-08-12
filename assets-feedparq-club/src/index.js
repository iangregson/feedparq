export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // handle feeds
    if (
      url.pathname.startsWith('/feeds/')
      && url.pathname.endsWith('.parquet')
    ) {
      // handle CORS
      if (request.method === "OPTIONS") {
        // Handle CORS preflight requests
        return handleOptions(request);
      } else if (
        request.method === "GET" ||
        request.method === "HEAD" ||
        request.method === "POST"
      ) {
        // Handle requests to the API server
        return handleFeedRequest(request, env);
      } else {
        return new Response(null, {
          status: 405,
          statusText: "Method Not Allowed",
        });
      }
    }

    // else 404
    return new Response('Not found', { status: 404 });
  },
}

async function handleFeedRequest(request, env) {
  const cache = caches.default;
  const origin = request.headers.get('Origin') ?? null;
  const url = new URL(request.url);

  const cacheHit = await cache.match(request);
  if (cacheHit) {
    return cacheHit;
  }
  
  const [key] = url.pathname.split('/').slice(-1);
  const head = await env.BUCKET_FEEDPARQ.head(key);
  if (head === null) {
    const response = new Response('Not found', { status: 404 });
    origin && response.headers.set("Access-Control-Allow-Origin", origin);
    return response;
  }
  
  const obj = await env.BUCKET_FEEDPARQ.get(key, {
    range: request.headers,
    onlyIf: request.headers,
  });

  const headers = new Headers();
  obj.writeHttpMetadata(headers);
  headers.set('etag', obj.httpEtag);
  origin && headers.set("Access-Control-Allow-Origin", origin);
  headers.append('content-type', 'application/vnd.apache.parquet');
  
 
  if (obj.range) {
    headers.set(
      "content-range",
      `bytes ${obj.range.offset}-${obj.range.end ?? obj.size - 1}/${obj.size}`
    );
  } else {
    event.waitUntil(
      cache.put(request, new Response(obj.body, {
        headers,
      }))
    );
  }

  headers.append("Vary", "Origin");

  const status = obj.body
    ? (request.headers.get("range") !== null
      ? 206 : 200)
    : 304;
  
  const response = new Response(obj.body, {
    headers,
    status,
  });
  
  return response;
}

async function handleOptions(request) {
  if (
    request.headers.get("Origin") !== null &&
    request.headers.get("Access-Control-Request-Method") !== null &&
    request.headers.get("Access-Control-Request-Headers") !== null
  ) {
    // Handle CORS preflight requests.
    return new Response(null, {
      headers: {
        ...corsHeaders,
        "Access-Control-Allow-Headers": request.headers.get(
          "Access-Control-Request-Headers"
        ),
      },
    });
  } else {
    // Handle standard OPTIONS request.
    return new Response(null, {
      headers: {
        Allow: "GET, HEAD, POST, OPTIONS",
      },
    });
  }
}